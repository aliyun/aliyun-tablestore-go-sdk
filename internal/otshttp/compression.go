package otshttp

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

// EncodeRequest applies OTS compression before the caller computes MD5 and signs.
func EncodeRequest(body []byte, header http.Header, enableRequestCompression, enableResponseCompression bool) []byte {
	if enableRequestCompression && len(body) > 0 {
		var compressed bytes.Buffer
		writer := zlib.NewWriter(&compressed)
		_, _ = writer.Write(body)
		_ = writer.Close()
		header.Set("x-ots-request-compress-type", "deflate")
		// The OTS size header records the original, uncompressed byte count.
		header.Set("x-ots-request-compress-size", strconv.Itoa(len(body)))
		body = compressed.Bytes()
	}
	if enableResponseCompression {
		header.Set("x-ots-response-compress-type", "deflate")
	}
	return body
}

// ReadResponseBody decodes both successful and error responses. The caller owns
// resp.Body and must close it, including when decoding fails.
func ReadResponseBody(resp *http.Response) ([]byte, error) {
	types, compressed := resp.Header[http.CanonicalHeaderKey("x-ots-response-compress-type")]
	if !compressed {
		return ioutil.ReadAll(resp.Body)
	}
	if len(types) != 1 || !strings.EqualFold(strings.TrimSpace(types[0]), "deflate") {
		return nil, fmt.Errorf("decompress response failed: unsupported compress type %q", types)
	}
	// The uncompressed size must fit a positive signed 32-bit integer, as in the Java SDK.
	rawSize := resp.Header.Get("x-ots-response-compress-size")
	size, err := strconv.ParseInt(rawSize, 10, 32)
	if err != nil || size <= 0 {
		return nil, fmt.Errorf("decompress response failed: invalid x-ots-response-compress-size %q", rawSize)
	}
	// Preserve reader error types for the callers' existing net.Error retry checks.
	var reader io.ReadCloser
	defer func() {
		if reader != nil {
			reader.Close()
		}
	}()
	reader, err = zlib.NewReader(resp.Body)
	if err != nil {
		return nil, err
	}
	// Bound output by the declared size without allocating from an untrusted header.
	body, err := ioutil.ReadAll(io.LimitReader(reader, size+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) != size {
		return nil, fmt.Errorf("decompress response failed: expected %d bytes, got %d", size, len(body))
	}
	return body, nil
}
