package tablestore

import (
	"net/http"
	"testing"
)

func TestResolveTableStoreTransportProxy(t *testing.T) {
	config := NewDefaultTableStoreConfig()
	transport, ok := resolveTableStoreTransport(config).(*http.Transport)
	if !ok {
		t.Fatalf("default transport has type %T, want *http.Transport", transport)
	}
	if transport.Proxy != nil {
		t.Fatal("default transport unexpectedly enables a proxy")
	}

	config.ProxyFromEnvironment = true
	transport = resolveTableStoreTransport(config).(*http.Transport)
	if transport.Proxy == nil {
		t.Fatal("environment proxy is not enabled")
	}

	config.ProxyHost = "http://user:password@proxy.example.com:8080"
	transport = resolveTableStoreTransport(config).(*http.Transport)
	request, err := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	proxyURL, err := transport.Proxy(request)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := proxyURL.String(), config.ProxyHost; got != want {
		t.Fatalf("proxy URL = %q, want %q", got, want)
	}
}

func TestResolveTableStoreTransportCustomTransportTakesPrecedence(t *testing.T) {
	config := NewDefaultTableStoreConfig()
	config.Transport = http.DefaultTransport
	config.ProxyHost = "http://proxy.example.com:8080"
	config.ProxyFromEnvironment = true

	if got := resolveTableStoreTransport(config); got != config.Transport {
		t.Fatalf("transport = %T, want configured transport %T", got, config.Transport)
	}
}

func TestClientProxyConfiguration(t *testing.T) {
	const proxyHost = "http://proxy.example.com:8080"
	config := NewDefaultTableStoreConfig()
	config.ProxyHost = proxyHost

	client := NewClientWithConfig("https://example.com", "instance", "access-key-id", "access-key-secret", "", config)
	assertClientProxy(t, client.httpClient, proxyHost)

	timeseriesClient := NewTimeseriesClientWithConfig("https://example.com", "instance", "access-key-id", "access-key-secret", "", config, nil)
	assertClientProxy(t, timeseriesClient.httpClient, proxyHost)
}

func assertClientProxy(t *testing.T, client IHttpClient, want string) {
	t.Helper()
	tableStoreHTTPClient, ok := client.(*TableStoreHttpClient)
	if !ok {
		t.Fatalf("HTTP client has type %T, want *TableStoreHttpClient", client)
	}
	transport, ok := tableStoreHTTPClient.httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport has type %T, want *http.Transport", tableStoreHTTPClient.httpClient.Transport)
	}
	request, err := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	proxyURL, err := transport.Proxy(request)
	if err != nil {
		t.Fatal(err)
	}
	if got := proxyURL.String(); got != want {
		t.Fatalf("proxy URL = %q, want %q", got, want)
	}
}
