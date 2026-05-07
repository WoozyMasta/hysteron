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

package postgresql

import "testing"

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "plain", in: "hysteron", want: `"hysteron"`},
		{name: "single quote", in: "sto'lon", want: `"sto'lon"`},
		{name: "double quote", in: `sto"lon`, want: `"sto""lon"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := quoteIdentifier(tt.in); got != tt.want {
				t.Fatalf("got %q, wanted %q", got, tt.want)
			}
		})
	}
}

func TestQuoteLiteral(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "plain", in: "none", want: `'none'`},
		{name: "single quote", in: "pa'ss", want: `'pa''ss'`},
		{name: "backslash", in: `pa\ss`, want: ` E'pa\\ss'`},
		{name: "single quote and backslash", in: `pa'ss\word`, want: ` E'pa''ss\\word'`},
		{name: "double quote", in: `pa"ss`, want: `'pa"ss'`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := quoteLiteral(tt.in); got != tt.want {
				t.Fatalf("got %q, wanted %q", got, tt.want)
			}
		})
	}
}
