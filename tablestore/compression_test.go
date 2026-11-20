package tablestore

import (
	"bytes"
	"compress/zlib"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliyun/aliyun-tablestore-go-sdk/common"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
)

func readCompressionRequest(t *testing.T, r *http.Request, requestCompression, responseCompression bool) []byte {
	t.Helper()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		t.Error(err)
		return nil
	}
	if r.ContentLength != int64(len(body)) {
		t.Errorf("ContentLength = %d, wire size = %d", r.ContentLength, len(body))
	}
	digest := md5.Sum(body)
	if r.Header.Get(xOtsContentmd5) != base64.StdEncoding.EncodeToString(digest[:]) {
		t.Error("MD5 does not cover the wire body")
	}
	// Compute independently of the SDK's header whitelist and signing helpers.
	var headers []string
	for key := range r.Header {
		key = strings.ToLower(key)
		if strings.HasPrefix(key, "x-ots-") && key != xOtsSignature && key != xOtsSignatureV4 {
			headers = append(headers, key+":"+strings.TrimSpace(r.Header.Get(key))+"\n")
		}
	}
	sort.Strings(headers)
	toSign := r.URL.Path + "\nPOST\n\n" + strings.Join(headers, "")
	signatureHeader := xOtsSignature
	mac := hmac.New(sha1.New, []byte("secret"))
	if r.Header.Get(xOtsSignatureV4) != "" {
		signatureHeader = xOtsSignatureV4
		mac = hmac.New(sha256.New, []byte("secret"))
		toSign += "ots"
	}
	mac.Write([]byte(toSign))
	if r.Header.Get(signatureHeader) != base64.StdEncoding.EncodeToString(mac.Sum(nil)) {
		t.Error("signature does not cover all OTS headers")
	}
	wantResponseType := ""
	if responseCompression {
		wantResponseType = "deflate"
	}
	if r.Header.Get(xOtsResponseCompressTye) != wantResponseType {
		t.Error("unexpected response compression negotiation")
	}
	if requestCompression && len(body) > 0 {
		if r.Header.Get(xOtsRequestCompressType) != "deflate" {
			t.Error("missing request compression type")
		}
		reader, err := zlib.NewReader(bytes.NewReader(body))
		if err != nil {
			t.Error(err)
			return nil
		}
		defer reader.Close()
		body, err = ioutil.ReadAll(reader)
		if err != nil {
			t.Error(err)
		}
		if r.Header.Get(xOtsRequestCompressSize) != strconv.Itoa(len(body)) {
			t.Error("request compression size does not match decoded body")
		}
	} else if r.Header.Get(xOtsRequestCompressType) != "" || r.Header.Get(xOtsRequestCompressSize) != "" {
		t.Error("unexpected compression headers on empty or uncompressed request")
	}
	return body
}

func writeCompressionResponse(t *testing.T, w http.ResponseWriter, status int, message proto.Message, compressed bool) {
	t.Helper()
	body, err := proto.Marshal(message)
	if err != nil {
		t.Error(err)
		return
	}
	w.Header().Set("x-ots-requestid", "compression-request-id")
	if compressed {
		w.Header().Set("x-ots-response-compress-type", "deflate")
		w.Header().Set("x-ots-response-compress-size", strconv.Itoa(len(body)))
		var buf bytes.Buffer
		writer := zlib.NewWriter(&buf)
		if _, err := writer.Write(body); err != nil {
			t.Error(err)
			return
		}
		if err := writer.Close(); err != nil {
			t.Error(err)
			return
		}
		body = buf.Bytes()
	}
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		t.Error(err)
	}
}

func TestHTTPCompressionTableStore(t *testing.T) {
	for _, tc := range []struct {
		name              string
		request, response bool
		v4, plain, fail   bool
	}{
		{name: "default-unsolicited-compression"},
		{name: "request", request: true},
		{name: "response", response: true},
		{name: "both", request: true, response: true},
		{name: "v4", request: true, response: true, v4: true},
		{name: "plain-fallback", request: true, response: true, plain: true},
		{name: "server-error", request: true, response: true, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var attempts int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body := readCompressionRequest(t, r, tc.request, tc.response)
				switch r.URL.Path {
				case deleteTableUri:
					attempt := atomic.AddInt32(&attempts, 1)
					var request otsprotocol.DeleteTableRequest
					if err := proto.Unmarshal(body, &request); err != nil || request.GetTableName() != "table" {
						t.Errorf("attempt %d: invalid request after one decompression: %v", attempt, err)
					}
					if tc.fail || attempt == 1 {
						code := SERVER_BUSY
						if tc.fail {
							code = "OTSParameterInvalid"
						}
						writeCompressionResponse(t, w, http.StatusBadRequest, &otsprotocol.Error{
							Code: proto.String(code), Message: proto.String("compressed error"),
						}, true)
						return
					}
					w.WriteHeader(http.StatusNoContent)
				case listTableUri:
					if len(body) != 0 {
						t.Error("ListTable should send an empty body")
					}
					writeCompressionResponse(t, w, http.StatusOK, &otsprotocol.ListTableResponse{
						TableNames: []string{"table"},
					}, !tc.plain)
				default:
					t.Errorf("unexpected path: %s", r.URL.Path)
				}
			}))
			defer server.Close()
			config := NewDefaultTableStoreConfig()
			if config.EnableRequestCompression || config.EnableResponseCompression {
				t.Fatal("compression must default to disabled")
			}
			config.EnableRequestCompression, config.EnableResponseCompression = tc.request, tc.response
			config.RetryTimes = 1
			config.DefaultRetryInterval = time.Millisecond
			var provider common.CredentialsProvider = &common.DefaultCredentialsProvider{AccessKeyID: "key", AccessKeySecret: "secret"}
			if tc.v4 {
				provider = &common.V4Credentials{AccessKeyID: "key", Region: "cn-hangzhou", SigningDate: "20260908", V4SigningAccessKey: "secret"}
			}
			client := NewClientWithCredentialsProvider(server.URL, "instance", provider, config)
			_, err := client.DeleteTable(&DeleteTableRequest{TableName: "table"})
			if tc.fail {
				otsErr, ok := err.(*OtsError)
				if !ok || otsErr.Code != "OTSParameterInvalid" || otsErr.Message != "compressed error" || otsErr.RequestId != "compression-request-id" || otsErr.HttpStatusCode != http.StatusBadRequest {
					t.Fatalf("unexpected server error: %v", err)
				}
				if atomic.LoadInt32(&attempts) != 1 {
					t.Fatal("non-retryable error was retried")
				}
				return
			}
			if err != nil || atomic.LoadInt32(&attempts) != 2 {
				t.Fatalf("compressed error retry failed: attempts=%d, err=%v", attempts, err)
			}
			response, err := client.ListTable()
			if err != nil {
				t.Fatal(err)
			}
			if len(response.TableNames) != 1 || response.TableNames[0] != "table" || response.RequestId != "compression-request-id" {
				t.Fatalf("unexpected decoded response: %+v", response)
			}
		})
	}
}

func TestHTTPCompressionBatchRetry(t *testing.T) {
	criteria := newBatchGetCriteria("table", 1)
	criteria.AddRow(newBatchGetPrimaryKey("one"))
	criteria.AddRow(newBatchGetPrimaryKey("two"))
	batchRequest := &BatchGetRowRequest{MultiRowQueryCriteria: []*MultiRowQueryCriteria{criteria}}
	original, _, err := buildBatchGetRowRequest(batchRequest)
	if err != nil {
		t.Fatal(err)
	}
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := atomic.AddInt32(&attempts, 1)
		body := readCompressionRequest(t, r, true, true)
		var request otsprotocol.BatchGetRowRequest
		if err := proto.Unmarshal(body, &request); err != nil {
			t.Error(err)
			return
		}
		wantRows := 1
		if attempt == 1 {
			wantRows = 2
		}
		if r.URL.Path != batchGetRowUri || len(request.Tables) != 1 || len(request.Tables[0].PrimaryKey) != wantRows {
			t.Errorf("unexpected batch retry request: %+v", request)
			return
		}
		if !bytes.Equal(request.Tables[0].PrimaryKey[0], original.Tables[0].PrimaryKey[0]) {
			t.Error("wrong primary key retried")
		}
		rows := []*otsprotocol.RowInBatchGetRowResponse{newSuccessfulBatchGetRow(newBatchGetRowBytes("one"))}
		if attempt == 1 {
			rows = []*otsprotocol.RowInBatchGetRowResponse{
				{IsOk: proto.Bool(false), Error: &otsprotocol.Error{Code: proto.String(SERVER_BUSY), Message: proto.String("retry")}},
				newSuccessfulBatchGetRow(newBatchGetRowBytes("two")),
			}
		}
		writeCompressionResponse(t, w, http.StatusOK, &otsprotocol.BatchGetRowResponse{
			Tables: []*otsprotocol.TableInBatchGetRowResponse{{TableName: proto.String("table"), Rows: rows}},
		}, true)
	}))
	defer server.Close()
	config := NewDefaultTableStoreConfig()
	config.EnableRequestCompression, config.EnableResponseCompression = true, true
	config.RetryTimes, config.DefaultRetryInterval = 1, time.Millisecond
	client := NewClientWithConfig(server.URL, "instance", "key", "secret", "", config)
	response, err := client.BatchGetRow(batchRequest)
	if err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&attempts) != 2 || len(response.TableToRowsResult["table"]) != 2 {
		t.Fatalf("batch retry did not merge both rows: %+v", response)
	}
	for _, row := range response.TableToRowsResult["table"] {
		if !row.IsSucceed {
			t.Fatalf("batch row failed after retry: %+v", row)
		}
	}
}

func TestHTTPCompressionTimeseries(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := readCompressionRequest(t, r, true, true)
		var request otsprotocol.DeleteTimeseriesTableRequest
		if err := proto.Unmarshal(body, &request); err != nil || request.GetTableName() != "table" || r.URL.Path != deleteTimeseriesTable {
			t.Errorf("unexpected timeseries request: %v, %s", err, r.URL.Path)
		}
		writeCompressionResponse(t, w, http.StatusBadRequest, &otsprotocol.Error{
			Code: proto.String("OTSParameterInvalid"), Message: proto.String("compressed error"),
		}, true)
	}))
	defer server.Close()
	config := NewDefaultTableStoreConfig()
	config.EnableRequestCompression, config.EnableResponseCompression = true, true
	client := NewTimeseriesClientWithConfig(server.URL, "instance", "key", "secret", "", config, nil)
	_, err := client.DeleteTimeseriesTable(&DeleteTimeseriesTableRequest{timeseriesTableName: "table"})
	if otsErr, ok := err.(*OtsError); !ok || otsErr.Code != "OTSParameterInvalid" || otsErr.RequestId != "compression-request-id" {
		t.Fatalf("unexpected compressed timeseries error: %v", err)
	}
}

func TestHTTPCompressionResponseReadError(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		readCompressionRequest(t, r, true, true)
		w.Header().Set("x-ots-requestid", "compression-request-id")
		w.Header().Set("x-ots-response-compress-type", "deflate")
		w.Header().Set("x-ots-response-compress-size", "42")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not-zlib"))
	}))
	defer server.Close()
	config := NewDefaultTableStoreConfig()
	config.EnableRequestCompression, config.EnableResponseCompression = true, true
	config.RetryTimes = 1
	config.DefaultRetryInterval = time.Millisecond
	client := NewClientWithConfig(server.URL, "instance", "key", "secret", "", config)
	_, err := client.DeleteTable(&DeleteTableRequest{TableName: "table"})
	if !errors.Is(err, zlib.ErrHeader) {
		t.Fatalf("unexpected decompression error: %v", err)
	}
	if atomic.LoadInt32(&attempts) != 1 {
		t.Fatalf("invalid compressed response was retried: attempts=%d", attempts)
	}
}
