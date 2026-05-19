// Copyright 20[0-9][0-9](?:-20[0-9][0-9])? (?:Sorint\.lab|WoozyMasta)(?:\nCopyright 2026 WoozyMasta)?
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

package health

import (
	"crypto/subtle"
	"net/http"
)

// WrapBasicAuth protects handlers with HTTP Basic auth when username is set.
func WrapBasicAuth(realm, username, password string, next http.Handler) http.Handler {
	if username == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || !compareConstantTime(user, username) || !compareConstantTime(pass, password) {
			w.Header().Set("WWW-Authenticate", `Basic realm="`+realm+`", charset="UTF-8"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func compareConstantTime(got, want string) bool {
	gotBytes := []byte(got)
	wantBytes := []byte(want)
	if len(gotBytes) != len(wantBytes) {
		return false
	}
	return subtle.ConstantTimeCompare(gotBytes, wantBytes) == 1
}
