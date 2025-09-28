package tunnel

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliyun/aliyun-tablestore-go-sdk/common"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/testConfig"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tunnel/protocol"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

var (
	alwaysFailUri            = "/tunnel/alwaysFail"
	badGateWayUri            = "/tunnel/badGateway"
	fail3timesUri            = "/tunnel/3timesFail"
	eof4timesUri             = "/tunnel/4timesEOF"
	successUri               = "/tunnel/success"
	reflectTokenUri          = "/tunnel/reflectToken"
	readRecordsAlwaysFailUri = "/tunnel/readrecords"

	requestId = "abcd-123"

	testTableName  = "testTableNameForTunnel"
	testTunnelName = "testTunnelName"
)

func TestNewTunnelApi(t *testing.T) {
	log.Println("TestNewTunnelApi started")

	c := assert.New(t)

	testClient := tablestore.NewClientWithConfig(testConfig.OtsEndpoint, testConfig.InstanceName, testConfig.OtsAccessId, testConfig.OtsAccessKey, "", nil)
	api := NewTunnelApi(testConfig.OtsEndpoint, testConfig.InstanceName, testConfig.OtsAccessId, testConfig.OtsAccessKey, nil)

	_, err := testClient.CreateTable(getCreateTableRequest())
	c.Equal(err, nil)

	_, err = api.CreateTunnel(getCreateTunnelRequest())
	c.Equal(err, nil)

	_, err = api.DeleteTunnel(getDeleteTunnelRequest())
	c.Equal(err, nil)

	_, err = testClient.DeleteTable(getDeleteTableRequest())
	c.Equal(err, nil)

	log.Println("TestNewTunnelApi finished")
}

func TestNewTunnelApiWithCredentialsProvider(t *testing.T) {
	log.Println("TestNewTunnelApiWithCredentialsProvider started")

	c := assert.New(t)

	provider := &common.DefaultCredentialsProvider{AccessKeyID: testConfig.OtsAccessId, AccessKeySecret: testConfig.OtsAccessKey}

	testClient := tablestore.NewClientWithCredentialsProvider(testConfig.OtsEndpoint, testConfig.InstanceName, provider, nil)
	api := NewTunnelApiWithCredentialsProvider(testConfig.OtsEndpoint, testConfig.InstanceName, provider, nil)

	_, err := testClient.CreateTable(getCreateTableRequest())
	c.Equal(err, nil)

	_, err = api.CreateTunnel(getCreateTunnelRequest())
	c.Equal(err, nil)

	_, err = api.DeleteTunnel(getDeleteTunnelRequest())
	c.Equal(err, nil)

	_, err = testClient.DeleteTable(getDeleteTableRequest())
	c.Equal(err, nil)

	log.Println("TestNewTunnelApiWithCredentialsProvider finished")
}

func TestNewTunnelApiWithV4Credentials(t *testing.T) {
	log.Println("TestNewTunnelApiWithV4Credentials started")

	c := assert.New(t)

	provider := &common.DefaultCredentialsProvider{AccessKeyID: testConfig.OtsAccessId, AccessKeySecret: testConfig.OtsAccessKey}
	v4Credentials := common.CreateByCredentials(provider.GetCredentials(), testConfig.Region)

	testClient := tablestore.NewClientWithCredentialsProvider(testConfig.OtsEndpoint, testConfig.InstanceName, v4Credentials, nil)
	api := NewTunnelApiWithCredentialsProvider(testConfig.OtsEndpoint, testConfig.InstanceName, v4Credentials, nil)

	_, err := testClient.CreateTable(getCreateTableRequest())
	c.Equal(err, nil)

	_, err = api.CreateTunnel(getCreateTunnelRequest())
	c.Equal(err, nil)

	_, err = api.DeleteTunnel(getDeleteTunnelRequest())
	c.Equal(err, nil)

	_, err = testClient.DeleteTable(getDeleteTableRequest())
	c.Equal(err, nil)

	log.Println("TestNewTunnelApiWithV4Credentials finished")
}

func TestNewTunnelApiWithV4CredentialsAndEmptyRegion(t *testing.T) {
	log.Println("TestNewTunnelApiWithV4CredentialsAndEmptyRegion started")

	c := assert.New(t)

	provider := &common.DefaultCredentialsProvider{AccessKeyID: testConfig.OtsAccessId, AccessKeySecret: testConfig.OtsAccessKey}
	v4Credentials := common.CreateByCredentials(provider.GetCredentials(), "")

	testClient := tablestore.NewClientWithCredentialsProvider(testConfig.OtsEndpoint, testConfig.InstanceName, v4Credentials, nil)
	api := NewTunnelApiWithCredentialsProvider(testConfig.OtsEndpoint, testConfig.InstanceName, v4Credentials, nil)

	_, err := testClient.CreateTable(getCreateTableRequest())
	c.NotNil(err)
	c.Equal(err.Error(), errMissMustHeader("x-ots-signregion").Error())

	_, err = api.CreateTunnel(getCreateTunnelRequest())
	c.NotNil(err)
	c.Equal(err.Error(), errMissMustHeader("x-ots-signregion").Error())

	_, err = api.DeleteTunnel(getDeleteTunnelRequest())
	c.NotNil(err)
	c.Equal(err.Error(), errMissMustHeader("x-ots-signregion").Error())

	_, err = testClient.DeleteTable(getDeleteTableRequest())
	c.NotNil(err)
	c.Equal(err.Error(), errMissMustHeader("x-ots-signregion").Error())

	log.Println("TestNewTunnelApiWithV4CredentialsAndEmptyRegion finished")
}

func TestDoRequest_RetryBackoffElapsedTimeForMetaApi(t *testing.T) {
	c := assert.New(t)
	ts := mockServer()
	defer ts.Close()

	ep := ts.URL
	api := NewTunnelApi(ep, "testInstance", "testAkId", "testAkSec", nil)
	s := time.Now()

	traceId, _, err := api.doRequest(alwaysFailUri, nil, nil)
	c.Equal(traceId, requestId)
	tunnelErr, ok := err.(*TunnelError)
	c.True(ok)
	c.Equal(tunnelErr.Code, ErrCodeServerUnavailable)

	dur := time.Now().Sub(s)
	c.True(dur > maxRetryIntervalForMetaApi)
}

func TestDoRequest_RetryBackoffElapsedTimeForDataApi(t *testing.T) {
	c := assert.New(t)
	ts := mockServer()
	defer ts.Close()

	ep := ts.URL
	api := NewTunnelApi(ep, "testInstance", "testAkId", "testAkSec", nil)
	s := time.Now()

	traceId, _, err := api.doRequest(readRecordsAlwaysFailUri, nil, nil)
	c.Equal(traceId, requestId)
	tunnelErr, ok := err.(*TunnelError)
	c.True(ok)
	c.Equal(tunnelErr.Code, ErrCodeServerUnavailable)

	dur := time.Now().Sub(s)
	c.True(dur > api.retryMaxElapsedTime)
}

func TestDoRequest_BadGateWayElapsedTime(t *testing.T) {
	c := assert.New(t)
	ts := mockServer()
	defer ts.Close()

	ep := ts.URL
	api := NewTunnelApi(ep, "testInstance", "testAkId", "testAkSec", nil)
	s := time.Now()

	traceId, _, err := api.doRequest(badGateWayUri, nil, nil)
	c.Equal(traceId, requestId)
	tunnelErr, ok := err.(*TunnelError)
	c.True(ok)
	c.Equal(tunnelErr.Code, ErrCodeServerUnavailable)

	dur := time.Now().Sub(s)
	c.True(dur > retryMaxElapsedTimeForMetaApi)
}

func TestDoRequest_EOFRetry4Times(t *testing.T) {
	c := assert.New(t)
	ts := mockServer()
	defer ts.Close()

	ep := ts.URL
	api := NewTunnelApi(ep, "testInstance", "testAkId", "testAkSec", nil)
	s := time.Now()

	traceId, _, err := api.doRequest(eof4timesUri, nil, nil)
	c.Equal(traceId, requestId)
	c.Nil(err)

	dur := time.Now().Sub(s)

	c.True(dur < 5*time.Second)
	c.True(dur > 1*time.Second)

}

func TestDoRequest_RetryBackoff3Times(t *testing.T) {
	c := assert.New(t)
	ts := mockServer()
	defer ts.Close()

	ep := ts.URL
	api := NewTunnelApi(ep, "testInstance", "testAkId", "testAkSec", nil)
	s := time.Now()

	traceId, _, err := api.doRequest(fail3timesUri, nil, nil)
	c.Equal(traceId, requestId)
	c.Nil(err)

	dur := time.Now().Sub(s)
	fmt.Println(dur)
	c.True(dur < 2*time.Second)
	c.True(dur > 400*time.Millisecond)
}

func TestDoRequest_Succeed(t *testing.T) {
	c := assert.New(t)
	ts := mockServer()
	defer ts.Close()

	ep := ts.URL
	api := NewTunnelApi(ep, "testInstance", "testAkId", "testAkSec", nil)
	s := time.Now()

	_, size, err := api.doRequest(successUri, nil, nil)
	c.Nil(err)

	dur := time.Now().Sub(s)
	fmt.Println(dur)
	c.True(dur < initRetryInterValForMetaApi)
	c.Equal(size, 0)
}

func TestDoRequest_PassTraceId(t *testing.T) {
	c := assert.New(t)
	ts := mockServer()
	defer ts.Close()

	ep := ts.URL
	api := NewTunnelApi(ep, "testInstance", "testAkId", "testAkSec", nil)

	traceId, _, err := api.doRequest(successUri, nil, nil)
	c.Nil(err)
	c.True(traceId != requestId)
}

func TestDoRequest_SetToken(t *testing.T) {
	c := assert.New(t)
	ts := mockServer()
	defer ts.Close()

	ep := ts.URL
	api := NewTunnelApiWithToken(ep, "testInstance", "testAkId", "testAkSec",
		"testToken", nil)

	cp := new(protocol.GetCheckpointResponse)
	_, _, err := api.doRequest(reflectTokenUri, nil, cp)
	c.Nil(err)
	c.EqualValues(cp.GetCheckpoint(), "testToken")
}

func TestDoRequest_AddExternalHeader(t *testing.T) {
	c := assert.New(t)
	ts := mockServer()
	defer ts.Close()

	ep := ts.URL
	header := make(map[string]string)
	header["x-ots-tunnel-type"] = "type/datadelivery"
	api := NewTunnelApiWithExternalHeader(ep, "testInstance", "testAkId", "testAkSec",
		"", nil, header)

	traceId, _, err := api.doRequest(successUri, nil, nil)
	c.Nil(err)
	c.True(traceId != requestId)
}

func mockServer() *httptest.Server {
	handler := http.NewServeMux()
	handler.HandleFunc(readRecordsAlwaysFailUri, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(xOtsRequestId, requestId)
		w.WriteHeader(503)
		errCode := ErrCodeServerUnavailable
		message := "server busy"
		pbErr := &protocol.Error{
			Code:    &errCode,
			Message: &message,
		}
		buf, _ := proto.Marshal(pbErr)
		w.Write(buf)
	})
	handler.HandleFunc(alwaysFailUri, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(xOtsRequestId, requestId)
		w.WriteHeader(503)
		errCode := ErrCodeServerUnavailable
		message := "server busy"
		pbErr := &protocol.Error{
			Code:    &errCode,
			Message: &message,
		}
		buf, _ := proto.Marshal(pbErr)
		w.Write(buf)
	})
	handler.HandleFunc(badGateWayUri, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(xOtsRequestId, requestId)
		w.WriteHeader(502)
		message := "bad gateway"
		w.Write([]byte(message))
	})
	var times int32
	handler.HandleFunc(fail3timesUri, func(w http.ResponseWriter, r *http.Request) {
		t := atomic.AddInt32(&times, 1)
		if t <= 3 {
			w.Header().Set(xOtsRequestId, requestId)
			w.WriteHeader(503)
			errCode := ErrCodeServerUnavailable
			message := "server busy"
			pbErr := &protocol.Error{
				Code:    &errCode,
				Message: &message,
			}
			buf, _ := proto.Marshal(pbErr)
			w.Write(buf)
		} else {
			w.Header().Set(xOtsRequestId, requestId)
			w.Write(nil)
		}
	})
	var eofTimes int32
	handler.HandleFunc(eof4timesUri, func(w http.ResponseWriter, r *http.Request) {
		t := atomic.AddInt32(&eofTimes, 1)
		if t <= 4 {
			hj, ok := w.(http.Hijacker)
			if !ok {
				panic("hijack failed")
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				panic(err)
			}
			conn.Close()
		} else {
			w.Header().Set(xOtsRequestId, requestId)
			w.Write(nil)
		}
	})
	handler.HandleFunc(successUri, func(w http.ResponseWriter, r *http.Request) {
		if id := r.Header.Get(xOtsHeaderTraceID); id != "" {
			w.Header().Set(xOtsRequestId, id)
		} else {
			w.Header().Set(xOtsRequestId, requestId)
		}
		w.Write(nil)
	})
	handler.HandleFunc(reflectTokenUri, func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get(xOtsHeaderStsToken)
		seq := int64(0)
		//reuse checkpoint response for convenient
		resp := new(protocol.GetCheckpointResponse)
		resp.Checkpoint = &token
		resp.SequenceNumber = &seq
		buf, _ := proto.Marshal(resp)
		w.Write(buf)
	})
	handler.HandleFunc(getCheckpointUri, func(w http.ResponseWriter, r *http.Request) {
		var req protocol.GetCheckpointRequest
		readBuf, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(503)
			w.Write([]byte(err.Error()))
		}
		err = proto.Unmarshal(readBuf, &req)
		if err != nil {
			w.WriteHeader(503)
			w.Write([]byte(err.Error()))
		}
		if strings.Contains(req.GetChannelId(), "getCheckpointFailed") {
			code := ErrCodeParamInvalid
			msg := "return get checkpoint failed error"
			perr := &protocol.Error{
				Code:    &code,
				Message: &msg,
			}
			pbuf, _ := proto.Marshal(perr)
			w.WriteHeader(400)
			w.Write(pbuf)
			return
		}
		resp := new(protocol.GetCheckpointResponse)
		token := "token"
		seqNum := int64(1)
		resp.Checkpoint = &token
		resp.SequenceNumber = &seqNum
		buf, _ := proto.Marshal(resp)
		w.Write(buf)
	})
	handler.HandleFunc(checkpointUri, func(w http.ResponseWriter, r *http.Request) {
		if id := r.Header.Get(xOtsHeaderTraceID); id != "" {
			w.Header().Set(xOtsRequestId, id)
		} else {
			w.Header().Set(xOtsRequestId, requestId)
		}
		w.Write(nil)
	})
	return httptest.NewServer(handler)
}

func getCreateTableRequest() *tablestore.CreateTableRequest {
	createtableRequest := new(tablestore.CreateTableRequest)

	tableMeta := new(tablestore.TableMeta)
	tableMeta.TableName = testTableName
	tableMeta.AddPrimaryKeyColumn("pk1", tablestore.PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn("pk2", tablestore.PrimaryKeyType_INTEGER)
	tableMeta.AddPrimaryKeyColumn("pk3", tablestore.PrimaryKeyType_BINARY)
	tableOption := new(tablestore.TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 3
	reservedThroughput := new(tablestore.ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	createtableRequest.TableMeta = tableMeta
	createtableRequest.TableOption = tableOption
	createtableRequest.ReservedThroughput = reservedThroughput

	return createtableRequest
}

func getDeleteTableRequest() *tablestore.DeleteTableRequest {
	deleteRequest := new(tablestore.DeleteTableRequest)
	deleteRequest.TableName = testTableName
	return deleteRequest
}

func getCreateTunnelRequest() *CreateTunnelRequest {
	req := &CreateTunnelRequest{
		TableName:  testTableName,
		TunnelName: testTunnelName,
		Type:       TunnelTypeStream,
	}
	return req
}

func getDeleteTunnelRequest() *DeleteTunnelRequest {
	deleteTunnelRequest := &DeleteTunnelRequest{
		TableName:  testTableName,
		TunnelName: testTunnelName,
	}
	return deleteTunnelRequest
}
