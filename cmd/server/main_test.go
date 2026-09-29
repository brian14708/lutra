package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"
)

func TestSPAHandler(t *testing.T) {
	handler := spaHandler(fstest.MapFS{
		"index.html":    &fstest.MapFile{Data: []byte("<html>console</html>")},
		"assets/app.js": &fstest.MapFile{Data: []byte("export {}")},
	})
	tests := []struct {
		name, method, path string
		status             int
		body, cache, allow string
	}{
		{name: "root", method: http.MethodGet, path: "/", status: http.StatusOK, body: "<html>console</html>", cache: "no-cache"},
		{name: "client route", method: http.MethodGet, path: "/settings", status: http.StatusOK, body: "<html>console</html>", cache: "no-cache"},
		{name: "asset", method: http.MethodGet, path: "/assets/app.js", status: http.StatusOK, body: "export {}", cache: "public, max-age=31536000, immutable"},
		{name: "missing asset", method: http.MethodGet, path: "/assets/missing.js", status: http.StatusNotFound},
		{name: "method", method: http.MethodPost, path: "/", status: http.StatusMethodNotAllowed, allow: "GET, HEAD"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(tt.method, tt.path, nil))
			result := recorder.Result()
			defer func() { _ = result.Body.Close() }()
			if result.StatusCode != tt.status {
				t.Fatalf("status: got %d, want %d", result.StatusCode, tt.status)
			}
			body, err := io.ReadAll(result.Body)
			if err != nil {
				t.Fatal(err)
			}
			if tt.body != "" && string(body) != tt.body {
				t.Fatalf("body: got %q, want %q", body, tt.body)
			}
			if got := result.Header.Get("Cache-Control"); got != tt.cache {
				t.Fatalf("cache: got %q, want %q", got, tt.cache)
			}
			if got := result.Header.Get("Allow"); got != tt.allow {
				t.Fatalf("allow: got %q, want %q", got, tt.allow)
			}
		})
	}
}
