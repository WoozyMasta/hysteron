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

import (
	"reflect"
	"testing"
)

func TestConnParamsCopyAndEquals(t *testing.T) {
	original := ConnParams{
		"host":   "127.0.0.1",
		"port":   "5432",
		"dbname": "postgres",
	}

	copied := original.Copy()
	if !original.Equals(copied) {
		t.Fatalf("copy should equal original: %#v != %#v", copied, original)
	}

	copied.Set("host", "localhost")
	copied.Del("port")
	if original.Get("host") != "127.0.0.1" {
		t.Fatalf("copy mutation changed original host: %#v", original)
	}
	if !original.Isset("port") {
		t.Fatalf("copy deletion changed original port: %#v", original)
	}
	if original.Equals(copied) {
		t.Fatalf("mutated copy should not equal original")
	}
}

func TestParseConnString(t *testing.T) {
	tests := []struct {
		want ConnParams
		name string
		in   string
	}{
		{
			name: "plain key value options",
			in:   "host=127.0.0.1 port=5432 dbname=postgres",
			want: ConnParams{
				"host":   "127.0.0.1",
				"port":   "5432",
				"dbname": "postgres",
			},
		},
		{
			name: "whitespace around equals and empty trailing value",
			in:   " host = localhost password = ",
			want: ConnParams{
				"host":     "localhost",
				"password": "",
			},
		},
		{
			name: "quoted and escaped values",
			in:   `user='postgres user' password='pa\'ss' application_name=hysteron\ keeper`,
			want: ConnParams{
				"user":             "postgres user",
				"password":         "pa'ss",
				"application_name": "hysteron keeper",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseConnString(tt.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %#v, wanted %#v", got, tt.want)
			}
		})
	}
}

func TestParseConnStringErrors(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr string
	}{
		{
			name:    "missing equals",
			in:      "host localhost",
			wantErr: `missing "=" after "host" in connection info string"`,
		},
		{
			name:    "missing character after backslash",
			in:      `password=abc\`,
			wantErr: `missing character after backslash`,
		},
		{
			name:    "unterminated quoted value",
			in:      `password='secret`,
			wantErr: `unterminated quoted string literal in connection string`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseConnString(tt.in)
			if err == nil {
				t.Fatalf("expected error %q", tt.wantErr)
			}
			if err.Error() != tt.wantErr {
				t.Fatalf("got error %q, wanted %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestURLToConnParams(t *testing.T) {
	got, err := URLToConnParams("postgres://user:pass@db.example.com:6543/app?sslmode=require&connect_timeout=10")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := ConnParams{
		"user":            "user",
		"password":        "pass",
		"host":            "db.example.com",
		"port":            "6543",
		"dbname":          "app",
		"sslmode":         "require",
		"connect_timeout": "10",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, wanted %#v", got, want)
	}
}

func TestURLToConnParamsRejectsNonPostgresScheme(t *testing.T) {
	_, err := URLToConnParams("http://user:pass@db.example.com/app")
	if err == nil {
		t.Fatal("expected non-postgres scheme error")
	}
	if err.Error() != "invalid connection protocol: http" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConnString(t *testing.T) {
	params := ConnParams{
		"user":             "postgres user",
		"host":             "127.0.0.1",
		"password":         `pa'ss\word`,
		"application_name": "",
	}

	got := params.ConnString()
	want := `host=127.0.0.1 password=pa\'ss\\word user=postgres\ user`
	if got != want {
		t.Fatalf("got %q, wanted %q", got, want)
	}
}
