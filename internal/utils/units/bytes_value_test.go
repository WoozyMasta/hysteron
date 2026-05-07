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

package units

import (
	"errors"
	"testing"
)

func TestBytesValueUnmarshalFlagValid(t *testing.T) {
	tests := []struct {
		input string
		want  uint64
	}{
		{input: "0", want: 0},
		{input: "1024", want: 1024},
		{input: "1KB", want: 1000},
		{input: "1KiB", want: 1024},
		{input: "256 MB", want: 256000000},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			var value BytesValue
			if err := value.UnmarshalFlag(tt.input); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if uint64(value) != tt.want {
				t.Fatalf("unexpected parsed value: got %d want %d", value, tt.want)
			}
		})
	}
}

func TestBytesValueUnmarshalFlagInvalid(t *testing.T) {
	tests := []string{
		"",
		"   ",
		"abc",
		"-1",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			var value BytesValue
			err := value.UnmarshalFlag(input)
			if err == nil {
				t.Fatal("expected error")
			}
			if !errors.Is(err, ErrInvalidBytesValue) {
				t.Fatalf("expected ErrInvalidBytesValue, got %v", err)
			}
		})
	}
}
