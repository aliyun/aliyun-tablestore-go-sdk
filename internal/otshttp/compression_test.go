package otshttp

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestHTTPCompressionEncodeRequest(t *testing.T) {
	for _, flags := range []struct {
		name              string
		request, response bool
	}{
		{name: "default"},
		{name: "request", request: true},
		{name: "response", response: true},
		{name: "both", request: true, response: true},
	} {
		for _, input := range []struct {
			name string
			body []byte
		}{
			{name: "nil"},
			{name: "empty", body: []byte{}},
			{name: "binary", body: bytes.Repeat([]byte("OTS\x00\x80\xff"), 128)},
		} {
			t.Run(flags.name+"/"+input.name, func(t *testing.T) {
				original := append([]byte(nil), input.body...)
				header := http.Header{"X-Unrelated": {"keep"}}
				wire := EncodeRequest(input.body, header, flags.request, flags.response)
				if !bytes.Equal(input.body, original) {
					t.Fatal("EncodeRequest mutated the input body")
				}
				wantHeader := http.Header{"X-Unrelated": {"keep"}}
				if flags.response {
					wantHeader.Set("x-ots-response-compress-type", "deflate")
				}
				if flags.request && len(original) > 0 {
					wantHeader.Set("x-ots-request-compress-type", "deflate")
					wantHeader.Set("x-ots-request-compress-size", strconv.Itoa(len(original)))
					// The caller hashes the returned wire body, not the original input.
					if md5.Sum(wire) == md5.Sum(original) {
						t.Error("wire MD5 still matches the uncompressed input")
					}
					reader, err := zlib.NewReader(bytes.NewReader(wire))
					if err != nil {
						t.Fatal(err)
					}
					decoded, err := ioutil.ReadAll(reader)
					closeErr := reader.Close()
					if err != nil || closeErr != nil {
						t.Fatalf("decode returned body: read = %v, close = %v", err, closeErr)
					}
					if !bytes.Equal(decoded, original) {
						t.Fatal("returned zlib body does not decode to the original input")
					}
				} else if !bytes.Equal(wire, original) || (wire == nil) != (input.body == nil) {
					t.Fatal("uncompressed body changed")
				}
				if !reflect.DeepEqual(header, wantHeader) {
					t.Errorf("headers = %v, want %v (MD5 must remain the caller's responsibility)", header, wantHeader)
				}
			})
		}
	}
}

// httpCompressionSource records ownership and can replace EOF with a read error.
type httpCompressionSource struct {
	io.Reader
	err    error
	closed bool
}

func (s *httpCompressionSource) Read(p []byte) (int, error) {
	n, err := s.Reader.Read(p)
	if err == io.EOF && s.err != nil {
		err = s.err
	}
	return n, err
}

func (s *httpCompressionSource) Close() error {
	s.closed = true
	return nil
}

func TestHTTPCompressionReadResponseBody(t *testing.T) {
	// Generated with Alibaba OpenJDK 1.8.0_66-b60, not with the Go encoder:
	// byte[] input = "OTS Java zlib vector: hello hello hello!\n".getBytes(StandardCharsets.UTF_8);
	// ByteArrayOutputStream out = new ByteArrayOutputStream();
	// try (DeflaterOutputStream z = new DeflaterOutputStream(out)) { z.write(input); }
	// out.toByteArray() is the following zlib stream; input.length == 41.
	plain := []byte("OTS Java zlib vector: hello hello hello!\n")
	compressed, err := hex.DecodeString("789cf30f0956f04a2c4b54a8cac94c52284b4d2ec92fb252c848cdc9c9472615b90029c10e1e")
	if err != nil {
		t.Fatal(err)
	}
	size := strconv.Itoa(len(plain))
	deflate := []string{"deflate"}
	badHeader := append([]byte(nil), compressed...)
	badHeader[0] ^= 0xff
	badChecksum := append([]byte(nil), compressed...)
	badChecksum[len(badChecksum)-1] ^= 0xff
	// Remove the two-byte zlib header and four-byte Adler-32 trailer.
	rawDeflate := compressed[2 : len(compressed)-4]
	var gzipped bytes.Buffer
	writer := gzip.NewWriter(&gzipped)
	if _, err := writer.Write(plain); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	readErr := errors.New("source read failure")
	networkErr := &net.DNSError{Err: "context deadline exceeded", IsTimeout: true}
	for _, tc := range []struct {
		name      string
		types     []string
		size      string
		body      []byte
		want      []byte
		wantErr   string
		cause     error
		sourceErr error
	}{
		{name: "uncompressed", body: plain, want: plain},
		{name: "uncompressed-empty"},
		{name: "size-without-type", size: size, body: compressed, want: compressed},
		{name: "invalid-size-without-type", size: "invalid", body: plain, want: plain},
		{name: "zlib-without-type", body: compressed, want: compressed},
		{name: "java-zlib", types: deflate, size: size, body: compressed, want: plain},
		{name: "type-case-and-trim", types: []string{" \tDeFlAtE\r\n"}, size: size, body: compressed, want: plain},
		{name: "unknown-type", types: []string{"gzip"}, size: size, body: compressed, wantErr: "unsupported compress type"},
		{name: "empty-type", types: []string{""}, size: size, body: compressed, wantErr: "unsupported compress type"},
		{name: "whitespace-type", types: []string{" \t"}, size: size, body: compressed, wantErr: "unsupported compress type"},
		{name: "no-type-values", types: []string{}, size: size, body: compressed, wantErr: "unsupported compress type"},
		{name: "multiple-types", types: []string{"deflate", "gzip"}, size: size, body: compressed, wantErr: "unsupported compress type"},
		{name: "duplicate-types", types: []string{"deflate", "deflate"}, size: size, body: compressed, wantErr: "unsupported compress type"},
		{name: "comma-separated-types", types: []string{"deflate, deflate"}, size: size, body: compressed, wantErr: "unsupported compress type"},
		{name: "missing-size", types: deflate, body: compressed, wantErr: "invalid x-ots-response-compress-size"},
		{name: "invalid-size", types: deflate, size: "invalid", body: compressed, wantErr: "invalid x-ots-response-compress-size"},
		{name: "fractional-size", types: deflate, size: "41.0", body: compressed, wantErr: "invalid x-ots-response-compress-size"},
		{name: "whitespace-size", types: deflate, size: " 41 ", body: compressed, wantErr: "invalid x-ots-response-compress-size"},
		{name: "zero-size", types: deflate, size: "0", body: compressed, wantErr: "invalid x-ots-response-compress-size"},
		{name: "negative-size", types: deflate, size: "-1", body: compressed, wantErr: "invalid x-ots-response-compress-size"},
		{name: "size-over-int32", types: deflate, size: "2147483648", body: compressed, wantErr: "invalid x-ots-response-compress-size"},
		{name: "size-over-int64", types: deflate, size: "9223372036854775808", body: compressed, wantErr: "invalid x-ots-response-compress-size"},
		{name: "raw-deflate", types: deflate, size: size, body: rawDeflate, cause: zlib.ErrHeader},
		{name: "gzip", types: deflate, size: size, body: gzipped.Bytes(), cause: zlib.ErrHeader},
		{name: "bad-header", types: deflate, size: size, body: badHeader, cause: zlib.ErrHeader},
		{name: "bad-checksum", types: deflate, size: size, body: badChecksum, cause: zlib.ErrChecksum},
		{name: "empty-stream", types: deflate, size: size, cause: io.ErrUnexpectedEOF},
		{name: "truncated-header", types: deflate, size: size, body: compressed[:1], cause: io.ErrUnexpectedEOF},
		{name: "truncated-deflate", types: deflate, size: size, body: compressed[:len(compressed)/2], cause: io.ErrUnexpectedEOF},
		{name: "truncated-checksum", types: deflate, size: size, body: compressed[:len(compressed)-1], cause: io.ErrUnexpectedEOF},
		{name: "size-too-small", types: deflate, size: "40", body: compressed, wantErr: "expected 40 bytes, got 41"},
		{name: "size-too-large", types: deflate, size: "42", body: compressed, wantErr: "expected 42 bytes, got 41"},
		{name: "output-limited-to-size-plus-one", types: deflate, size: "1", body: compressed, wantErr: "expected 1 bytes, got 2"},
		{name: "max-int32-no-preallocation", types: deflate, size: "2147483647", body: compressed, wantErr: "expected 2147483647 bytes, got 41"},
		{name: "uncompressed-read-error", body: plain, want: plain, sourceErr: readErr, cause: readErr},
		{name: "zlib-header-read-error", types: deflate, size: size, sourceErr: readErr, cause: readErr},
		{name: "zlib-body-read-error", types: deflate, size: size, body: compressed[:len(compressed)/2], sourceErr: readErr, cause: readErr},
		{name: "zlib-checksum-read-error", types: deflate, size: size, body: compressed[:len(compressed)-1], sourceErr: readErr, cause: readErr},
		{name: "uncompressed-timeout", body: plain, want: plain, sourceErr: networkErr, cause: networkErr},
		{name: "zlib-header-timeout", types: deflate, size: size, sourceErr: networkErr, cause: networkErr},
		{name: "zlib-body-timeout", types: deflate, size: size, body: compressed[:len(compressed)/2], sourceErr: networkErr, cause: networkErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			header := make(http.Header)
			if tc.types != nil {
				header[http.CanonicalHeaderKey("x-ots-response-compress-type")] = tc.types
			}
			if tc.size != "" {
				header.Set("x-ots-response-compress-size", tc.size)
			}
			source := &httpCompressionSource{Reader: bytes.NewReader(tc.body), err: tc.sourceErr}
			resp := &http.Response{Header: header, Body: source}
			defer resp.Body.Close()
			var before, after runtime.MemStats
			if tc.size == "2147483647" {
				runtime.ReadMemStats(&before)
			}
			got, err := ReadResponseBody(resp)
			if tc.size == "2147483647" {
				runtime.ReadMemStats(&after)
				// A tiny stream must not allocate according to the untrusted 2 GiB size.
				if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 8<<20 {
					t.Errorf("allocated %d bytes for a tiny compressed stream", allocated)
				}
			}
			if source.closed {
				t.Error("ReadResponseBody closed the caller-owned source")
			}
			if tc.wantErr != "" || tc.cause != nil {
				if err == nil {
					t.Fatal("expected an error")
				}
				if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("error = %q, want %q", err, tc.wantErr)
				}
				if tc.cause != nil && !errors.Is(err, tc.cause) {
					t.Errorf("error = %v, want cause %v", err, tc.cause)
				}
				if tc.cause == networkErr {
					if netErr, ok := err.(net.Error); !ok || !netErr.Temporary() {
						t.Errorf("retryable network error lost its type: %T", err)
					}
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Errorf("body = %q, want %q", got, tc.want)
			}
			if err := resp.Body.Close(); err != nil || !source.closed {
				t.Errorf("caller close: error = %v, closed = %v", err, source.closed)
			}
		})
	}
}
