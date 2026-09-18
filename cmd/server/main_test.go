package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPSpanName(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "user connectrpc",
			path: "/api/lutra.v1.RunService/GetRun",
			want: "connectrpc /api/lutra.v1.RunService/GetRun",
		},
		{
			name: "worker connectrpc",
			path: "/worker-api/lutra.v1.WorkerService/CreateChildAction",
			want: "connectrpc /worker-api/lutra.v1.WorkerService/CreateChildAction",
		},
		{
			name: "http route",
			path: "/assets/app.js",
			want: http.MethodGet,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, test.path, nil)
			if got := httpSpanName("lutra.http", r); got != test.want {
				t.Fatalf("httpSpanName() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestHTTPTraceFilter(t *testing.T) {
	if httpTraceFilter(httptest.NewRequest(http.MethodPost, "/api/lutra.v1.RunService/GetRun", nil)) {
		t.Fatal("ConnectRPC API request should be traced by otelconnect only")
	}
	if httpTraceFilter(httptest.NewRequest(http.MethodPost, "/worker-api/lutra.v1.WorkerService/CreateChildAction", nil)) {
		t.Fatal("worker ConnectRPC request should be traced by otelconnect only")
	}
	if !httpTraceFilter(httptest.NewRequest(http.MethodGet, "/assets/app.js", nil)) {
		t.Fatal("static HTTP request should retain otelhttp tracing")
	}
}
