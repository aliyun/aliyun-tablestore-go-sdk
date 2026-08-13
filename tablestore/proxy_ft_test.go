package tablestore

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/aliyun/aliyun-tablestore-go-sdk/testConfig"
	"github.com/elazarl/goproxy"
)

func TestProxyListTableFT(t *testing.T) {
	if testConfig.OtsAccessId == "" || testConfig.OtsAccessKey == "" {
		t.Skip("TableStore FT credentials are not configured")
	}

	var proxyRequests int32
	proxy := goproxy.NewProxyHttpServer()
	proxy.OnRequest().DoFunc(func(req *http.Request, ctx *goproxy.ProxyCtx) (*http.Request, *http.Response) {
		atomic.AddInt32(&proxyRequests, 1)
		return req, nil
	})
	proxy.OnRequest().HandleConnectFunc(func(host string, ctx *goproxy.ProxyCtx) (*goproxy.ConnectAction, string) {
		atomic.AddInt32(&proxyRequests, 1)
		return goproxy.OkConnect, host
	})

	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	config := NewDefaultTableStoreConfig()
	config.ProxyHost = proxyServer.URL
	client := NewClientWithConfig(
		testConfig.OtsEndpoint,
		testConfig.InstanceName,
		testConfig.OtsAccessId,
		testConfig.OtsAccessKey,
		"",
		config,
	)

	response, err := client.ListTable()
	if err != nil {
		t.Fatalf("ListTable through proxy failed: %v", err)
	}
	if response.RequestId == "" {
		t.Fatal("ListTable returned an empty request ID")
	}
	if got := atomic.LoadInt32(&proxyRequests); got == 0 {
		t.Fatal("ListTable did not pass through the proxy")
	}
	t.Logf("ListTable returned %d tables through %d proxy request(s)", len(response.TableNames), atomic.LoadInt32(&proxyRequests))
}
