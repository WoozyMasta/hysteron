// Copyright 2026 WoozyMasta
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sentinel

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWebAdminMiddleware_DisabledWithoutAuth(t *testing.T) {
	cfg := &config{}
	cfg.Web.AuthUsername = ""
	cfg.Web.UnsafeNoAuth = false

	handler := webAdminMiddleware(cfg, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/pause", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusForbidden)
	}
}

func TestDecodeAdminJSON_MethodNotAllowed(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/pause", nil)
	rr := httptest.NewRecorder()
	var payload webAdminPausePayload

	ok := decodeAdminJSON(rr, req, &payload)
	if ok {
		t.Fatalf("decodeAdminJSON() = true, want false")
	}
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusMethodNotAllowed)
	}
	if got := rr.Header().Get("Allow"); got != http.MethodPost {
		t.Fatalf("Allow header = %q, want %q", got, http.MethodPost)
	}
}

func TestDecodeAdminJSON_InvalidPayload(t *testing.T) {
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/admin/pause",
		strings.NewReader("{"),
	)
	rr := httptest.NewRecorder()
	var payload webAdminPausePayload

	ok := decodeAdminJSON(rr, req, &payload)
	if ok {
		t.Fatalf("decodeAdminJSON() = true, want false")
	}
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestWriteAdminResponses(t *testing.T) {
	t.Run("error response", func(t *testing.T) {
		rr := httptest.NewRecorder()
		writeAdminError(rr, "boom")

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
		}
		body := rr.Body.String()
		if !strings.Contains(body, `"ok":false`) && !strings.Contains(body, `"ok": false`) {
			t.Fatalf("response missing ok=false: %q", body)
		}
		if !strings.Contains(body, `"error":"boom"`) && !strings.Contains(body, `"error": "boom"`) {
			t.Fatalf("response missing error message: %q", body)
		}
	})

	t.Run("ok response", func(t *testing.T) {
		rr := httptest.NewRecorder()
		writeAdminOK(rr)

		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
		}
		body := rr.Body.String()
		if !strings.Contains(body, `"ok":true`) && !strings.Contains(body, `"ok": true`) {
			t.Fatalf("response missing ok=true: %q", body)
		}
	})
}
