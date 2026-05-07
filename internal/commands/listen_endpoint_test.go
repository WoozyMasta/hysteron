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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

package commands

import (
	"errors"
	"testing"
)

func TestListenEndpointUnmarshalFlagValid(t *testing.T) {
	tests := []string{
		"",
		":8080",
		"0.0.0.0:5432",
		"[::]:5432",
		"localhost:9090",
		"127.0.0.1:0",
	}

	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			var endpoint ListenEndpoint
			if err := endpoint.UnmarshalFlag(value); err != nil {
				t.Fatalf("expected valid endpoint %q, got error: %v", value, err)
			}
			if string(endpoint) != value {
				t.Fatalf("unexpected normalized value: got %q want %q", endpoint, value)
			}
		})
	}
}

func TestListenEndpointUnmarshalFlagInvalid(t *testing.T) {
	tests := []string{
		"8080",
		"host",
		"host:abc",
		":-1",
		"host:65536",
		"bad host:5432",
		"[::1]",
	}

	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			var endpoint ListenEndpoint
			err := endpoint.UnmarshalFlag(value)
			if err == nil {
				t.Fatalf("expected invalid endpoint error for %q", value)
			}
			if !errors.Is(err, ErrInvalidListenEndpoint) {
				t.Fatalf("expected ErrInvalidListenEndpoint, got: %v", err)
			}
		})
	}
}
