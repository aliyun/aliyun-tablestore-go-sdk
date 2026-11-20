package tablestore

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aliyun/aliyun-tablestore-go-sdk/testConfig"
)

var errCompressionFTNotReady = &OtsError{Code: TABLE_NOT_READY, Message: "compression FT data is not ready"}

type compressionFTRecord struct {
	path             string
	status           int
	requestType      string
	requestSize      int
	requestWireSize  int
	responseType     string
	responseSize     int
	responseWireSize int
}

type compressionFTTransport struct {
	mu      sync.Mutex
	records []compressionFTRecord
}

func (transport *compressionFTTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	requestBody, err := readCompressionFTBody(req.Body)
	if err != nil {
		return nil, err
	}
	if req.Body != nil {
		req.Body = ioutil.NopCloser(bytes.NewReader(requestBody))
		req.ContentLength = int64(len(requestBody))
	}
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	responseBody, err := readCompressionFTBody(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.Body != nil {
		resp.Body = ioutil.NopCloser(bytes.NewReader(responseBody))
	}
	requestSize, _ := strconv.Atoi(req.Header.Get(xOtsRequestCompressSize))
	responseSize, _ := strconv.Atoi(resp.Header.Get("x-ots-response-compress-size"))
	record := compressionFTRecord{
		path:             req.URL.Path,
		status:           resp.StatusCode,
		requestType:      req.Header.Get(xOtsRequestCompressType),
		requestSize:      requestSize,
		requestWireSize:  len(requestBody),
		responseType:     resp.Header.Get(xOtsResponseCompressTye),
		responseSize:     responseSize,
		responseWireSize: len(responseBody),
	}
	transport.mu.Lock()
	transport.records = append(transport.records, record)
	transport.mu.Unlock()
	return resp, nil
}

func readCompressionFTBody(body io.ReadCloser) ([]byte, error) {
	if body == nil {
		return nil, nil
	}
	defer body.Close()
	return ioutil.ReadAll(body)
}

func (transport *compressionFTTransport) last(path string) (compressionFTRecord, bool) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	for i := len(transport.records) - 1; i >= 0; i-- {
		if transport.records[i].path == path {
			return transport.records[i], true
		}
	}
	return compressionFTRecord{}, false
}

func (transport *compressionFTTransport) mustLast(t *testing.T, path string) compressionFTRecord {
	t.Helper()
	record, ok := transport.last(path)
	if !ok {
		t.Fatalf("no HTTP record for %s", path)
	}
	return record
}

func skipCompressionFT(t *testing.T) {
	t.Helper()
	if testConfig.OtsEndpoint == "" || testConfig.InstanceName == "" || testConfig.OtsAccessId == "" || testConfig.OtsAccessKey == "" {
		t.Skip("TableStore FT credentials are not configured")
	}
}

func newCompressionFTConfig(transport http.RoundTripper) *TableStoreConfig {
	config := NewDefaultTableStoreConfig()
	config.EnableRequestCompression = true
	config.EnableResponseCompression = true
	config.RetryTimes = 5
	config.MaxRetryTime = 90 * time.Second
	config.Transport = transport
	return config
}

func compressionFTTableName(prefix string) string {
	return strings.Replace(runtime.Version(), ".", "", -1) + prefix + strconv.FormatInt(time.Now().UnixNano(), 10)
}

func compressionFTRetryable(err error) bool {
	otsErr, ok := err.(*OtsError)
	if !ok {
		return false
	}
	switch otsErr.Code {
	case "OTSObjectNotExist", TABLE_NOT_READY, PARTITION_UNAVAILABLE, SERVER_BUSY, STORAGE_SERVER_BUSY:
		return true
	}
	return false
}

func isCompressionFTNotExist(err error) bool {
	otsErr, ok := err.(*OtsError)
	return ok && otsErr.Code == "OTSObjectNotExist"
}

func retryCompressionFT(t *testing.T, timeout time.Duration, operation func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := operation()
		if err == nil {
			return
		}
		if !compressionFTRetryable(err) || time.Now().After(deadline) {
			t.Fatalf("compression FT operation failed: %v", err)
		}
		time.Sleep(2 * time.Second)
	}
}

func retryCompressionFTCleanup(t *testing.T, timeout time.Duration, operation func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := operation()
		if err == nil {
			return
		}
		if !compressionFTRetryable(err) || time.Now().After(deadline) {
			t.Errorf("compression FT cleanup failed: %v", err)
			return
		}
		time.Sleep(2 * time.Second)
	}
}

func assertCompressionFTRequest(t *testing.T, transport *compressionFTTransport, path string, expectSmaller bool) {
	t.Helper()
	record := transport.mustLast(t, path)
	if record.status < 200 || record.status >= 300 {
		t.Fatalf("%s status = %d", path, record.status)
	}
	if record.requestType != "deflate" {
		t.Fatalf("%s request compression type = %q", path, record.requestType)
	}
	if record.requestSize <= 0 {
		t.Fatalf("%s request compression size = %d", path, record.requestSize)
	}
	if expectSmaller && record.requestWireSize >= record.requestSize {
		t.Fatalf("%s request wire size = %d, original size = %d", path, record.requestWireSize, record.requestSize)
	}
}

func assertCompressionFTResponse(t *testing.T, transport *compressionFTTransport, path string, expectSmaller bool) {
	t.Helper()
	record := transport.mustLast(t, path)
	if record.status < 200 || record.status >= 300 {
		t.Fatalf("%s status = %d", path, record.status)
	}
	if record.responseType != "deflate" {
		t.Fatalf("%s response compression type = %q", path, record.responseType)
	}
	if record.responseSize <= 0 {
		t.Fatalf("%s response compression size = %d", path, record.responseSize)
	}
	if expectSmaller && record.responseWireSize >= record.responseSize {
		t.Fatalf("%s response wire size = %d, original size = %d", path, record.responseWireSize, record.responseSize)
	}
}

func deleteCompressionFTTable(t *testing.T, client *TableStoreClient, table string) {
	t.Helper()
	retryCompressionFTCleanup(t, 60*time.Second, func() error {
		_, err := client.DeleteTable(&DeleteTableRequest{TableName: table})
		if isCompressionFTNotExist(err) {
			return nil
		}
		return err
	})
}

func deleteCompressionFTTimeseriesTable(t *testing.T, client *TimeseriesClient, table string) {
	t.Helper()
	retryCompressionFTCleanup(t, 60*time.Second, func() error {
		_, err := client.DeleteTimeseriesTable(NewDeleteTimeseriesTableRequest(table))
		if isCompressionFTNotExist(err) {
			return nil
		}
		return err
	})
}

func TestCompressionTableStoreFT(t *testing.T) {
	skipCompressionFT(t)
	transport := &compressionFTTransport{}
	client := NewClientWithConfig(
		testConfig.OtsEndpoint,
		testConfig.InstanceName,
		testConfig.OtsAccessId,
		testConfig.OtsAccessKey,
		"",
		newCompressionFTConfig(transport),
	)
	table := compressionFTTableName("compressionft")
	payload := strings.Repeat("tablestore-go-sdk-compression-ft ", 2048)

	tableMeta := new(TableMeta)
	tableMeta.TableName = table
	tableMeta.AddPrimaryKeyColumn("pk", PrimaryKeyType_STRING)
	_, err := client.CreateTable(&CreateTableRequest{
		TableMeta:          tableMeta,
		TableOption:        &TableOption{TimeToAlive: -1, MaxVersion: 1},
		ReservedThroughput: &ReservedThroughput{Readcap: 0, Writecap: 0},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	defer deleteCompressionFTTable(t, client, table)

	retryCompressionFT(t, 60*time.Second, func() error {
		_, err := client.DescribeTable(&DescribeTableRequest{TableName: table})
		return err
	})
	assertCompressionFTRequest(t, transport, createTableUri, false)
	assertCompressionFTRequest(t, transport, describeTableUri, false)

	retryCompressionFT(t, 90*time.Second, func() error {
		primaryKey := new(PrimaryKey)
		primaryKey.AddPrimaryKeyColumn("pk", "compressed")
		change := &PutRowChange{TableName: table, PrimaryKey: primaryKey}
		change.AddColumn("payload", payload)
		change.SetCondition(RowExistenceExpectation_IGNORE)
		_, err := client.PutRow(&PutRowRequest{PutRowChange: change})
		return err
	})
	assertCompressionFTRequest(t, transport, putRowUri, true)

	var got string
	retryCompressionFT(t, 60*time.Second, func() error {
		primaryKey := new(PrimaryKey)
		primaryKey.AddPrimaryKeyColumn("pk", "compressed")
		resp, err := client.GetRow(&GetRowRequest{
			SingleRowQueryCriteria: &SingleRowQueryCriteria{
				TableName:  table,
				PrimaryKey: primaryKey,
				MaxVersion: 1,
			},
		})
		if err != nil {
			return err
		}
		for _, column := range resp.Columns {
			if column.ColumnName == "payload" {
				value, ok := column.Value.(string)
				if !ok {
					return fmt.Errorf("payload type = %T", column.Value)
				}
				got = value
				return nil
			}
		}
		return errCompressionFTNotReady
	})
	if got != payload {
		t.Fatalf("payload length = %d, want %d", len(got), len(payload))
	}
	assertCompressionFTResponse(t, transport, getRowUri, true)
}

func TestCompressionTimeseriesFT(t *testing.T) {
	skipCompressionFT(t)
	transport := &compressionFTTransport{}
	client := NewTimeseriesClientWithConfig(
		testConfig.OtsEndpoint,
		testConfig.InstanceName,
		testConfig.OtsAccessId,
		testConfig.OtsAccessKey,
		"",
		newCompressionFTConfig(transport),
		nil,
	)
	table := compressionFTTableName("tscompressionft")
	payload := strings.Repeat("tablestore-go-sdk-timeseries-compression-ft ", 1024)

	tableMeta := NewTimeseriesTableMeta(table)
	tableMeta.SetTimeseriesTableOptions(NewTimeseriesTableOptions(86400))
	createRequest := NewCreateTimeseriesTableRequest()
	createRequest.SetTimeseriesTableMeta(tableMeta)
	_, err := client.CreateTimeseriesTable(createRequest)
	if err != nil {
		t.Fatalf("CreateTimeseriesTable: %v", err)
	}
	defer deleteCompressionFTTimeseriesTable(t, client, table)

	retryCompressionFT(t, 120*time.Second, func() error {
		_, err := client.DescribeTimeseriesTable(NewDescribeTimeseriesTableRequset(table))
		return err
	})
	assertCompressionFTRequest(t, transport, createTimeseriesTable, false)
	assertCompressionFTRequest(t, transport, describeTimeseriesTable, false)

	now := time.Now().UnixNano() / 1000
	timeseriesKey := NewTimeseriesKey()
	timeseriesKey.SetMeasurementName("compression")
	timeseriesKey.SetDataSource("gosdk")
	timeseriesKey.AddTag("case", "ft")
	timeseriesRow := NewTimeseriesRow(timeseriesKey)
	timeseriesRow.SetTimeInus(now)
	timeseriesRow.AddField("payload", NewColumnValue(ColumnType_STRING, payload))
	putRequest := NewPutTimeseriesDataRequest(table)
	putRequest.AddTimeseriesRows(timeseriesRow)

	retryCompressionFT(t, 120*time.Second, func() error {
		resp, err := client.PutTimeseriesData(putRequest)
		if err != nil {
			return err
		}
		if failedRows := resp.GetFailedRowResults(); len(failedRows) > 0 {
			return &OtsError{Code: failedRows[0].ErrorCode, Message: failedRows[0].Error.Error()}
		}
		return nil
	})
	assertCompressionFTRequest(t, transport, putTimeseriesData, true)

	var got string
	retryCompressionFT(t, 60*time.Second, func() error {
		getRequest := NewGetTimeseriesDataRequest(table)
		getRequest.SetTimeseriesKey(timeseriesKey)
		getRequest.SetTimeRange(now-1000000, now+1000000)
		getRequest.SetLimit(1)
		resp, err := client.GetTimeseriesData(getRequest)
		if err != nil {
			return err
		}
		rows := resp.GetRows()
		if len(rows) != 1 {
			return errCompressionFTNotReady
		}
		field, ok := rows[0].GetFieldsMap()["payload"]
		if !ok {
			return errCompressionFTNotReady
		}
		value, ok := field.Value.(string)
		if !ok {
			return fmt.Errorf("payload type = %T", field.Value)
		}
		got = value
		return nil
	})
	if got != payload {
		t.Fatalf("payload length = %d, want %d", len(got), len(payload))
	}
	assertCompressionFTResponse(t, transport, getTimeseriesData, true)
}
