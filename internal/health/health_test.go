package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type failChecker struct{}

func (failChecker) Live(context.Context) error {
	return errors.New("live failed")
}

func (failChecker) Ready(context.Context) error {
	return errors.New("ready failed")
}

func (failChecker) Startup(context.Context) error {
	return errors.New("startup failed")
}

func TestRegisterRoutesStaticChecker(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, nil)

	paths := []string{
		"/health",
		"/healthz",
		"/health/live",
		"/health/ready",
		"/health/startup",
	}
	for _, p := range paths {
		req := httptest.NewRequest(http.MethodGet, p, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("path %q expected 200, got %d", p, rec.Code)
		}
	}
}

func TestRegisterRoutesFailingChecker(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRoutes(mux, failChecker{})

	cases := map[string]string{
		"/health":         "ready failed",
		"/healthz":        "ready failed",
		"/health/live":    "live failed",
		"/health/ready":   "ready failed",
		"/health/startup": "startup failed",
	}
	for p, msg := range cases {
		req := httptest.NewRequest(http.MethodGet, p, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("path %q expected 503, got %d", p, rec.Code)
		}
		if body := rec.Body.String(); body == "" || !strings.Contains(body, msg) {
			t.Fatalf("path %q expected body to contain %q, got %q", p, msg, body)
		}
	}
}
