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

package output

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestWriteClusterListPlain(t *testing.T) {
	var out bytes.Buffer
	err := WriteClusterList(&out, FormatPlain, []string{"dev", "prod"})
	if err != nil {
		t.Fatalf("WriteClusterList() error = %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "CLUSTER") {
		t.Fatalf("table output missing header: %q", got)
	}
	if !strings.Contains(got, "dev") || !strings.Contains(got, "prod") {
		t.Fatalf("table output missing values: %q", got)
	}
}

func TestWriteClusterListJSON(t *testing.T) {
	var out bytes.Buffer
	err := WriteClusterList(&out, FormatJSON, []string{"dev", "prod"})
	if err != nil {
		t.Fatalf("WriteClusterList() error = %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "\"dev\"") || !strings.Contains(got, "\"prod\"") {
		t.Fatalf("json output missing values: %q", got)
	}
}

func TestWriteClusterListYAML(t *testing.T) {
	var out bytes.Buffer
	err := WriteClusterList(&out, FormatYAML, []string{"dev", "prod"})
	if err != nil {
		t.Fatalf("WriteClusterList() error = %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "- dev") || !strings.Contains(got, "- prod") {
		t.Fatalf("yaml output missing values: %q", got)
	}
}

func TestWriteClusterListUnsupportedFormat(t *testing.T) {
	var out bytes.Buffer
	err := WriteClusterList(&out, Format("xml"), []string{"dev"})
	if err == nil {
		t.Fatal("expected unsupported format error")
	}
	if !errors.Is(err, ErrUnsupportedFormat) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWriteValueUnsupportedFormat(t *testing.T) {
	var out bytes.Buffer
	err := WriteValue(&out, Format("xml"), []string{"dev"})
	if err == nil {
		t.Fatal("expected unsupported format error")
	}
	if !errors.Is(err, ErrUnsupportedFormat) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWriteValuePlainFallbackYAML(t *testing.T) {
	var out bytes.Buffer
	err := WriteValue(&out, FormatPlain, []string{"dev", "prod"})
	if err != nil {
		t.Fatalf("WriteValue() error = %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "- dev") || !strings.Contains(got, "- prod") {
		t.Fatalf("plain fallback output missing values: %q", got)
	}
}
