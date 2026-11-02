package htnstratum

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// StartPromServer and the health check both serve http.DefaultServeMux
// (ListenAndServe(port, nil)). A blank net/http/pprof import registers the
// /debug/pprof/* handlers on that mux, so the shipped config, whose prom port
// listens on all interfaces, exposed heap, goroutine stacks, cmdline and the
// CPU profiler to anyone who could reach the port. This asserts the import is
// gone, so the profiler is not reachable there.
func TestNoPprofOnDefaultMux(t *testing.T) {
	for _, path := range []string{"/debug/pprof/cmdline", "/debug/pprof/", "/debug/pprof/profile"} {
		rec := httptest.NewRecorder()
		http.DefaultServeMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s is served on DefaultServeMux (status %d); the prom/health ports expose it", path, rec.Code)
		}
	}
}
