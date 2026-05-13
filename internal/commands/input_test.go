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
	"os"
	"path/filepath"
	"testing"
)

func TestReadDataInputFromFileOrArgRequiresInput(t *testing.T) {
	_, err := readCommandInput("", "", true)
	if !errors.Is(err, ErrCommandInputRequired) {
		t.Fatalf("expected ErrCommandInputRequired, got %v", err)
	}
}

func TestReadDataInputFromFileOrArgRejectsConflictingInput(t *testing.T) {
	_, err := readCommandInput("spec.json", "{}", true)
	if !errors.Is(err, ErrCommandInputConflict) {
		t.Fatalf("expected ErrCommandInputConflict, got %v", err)
	}
}

func TestReadDataInputFromFileOrArgOptionalInputReturnsNil(t *testing.T) {
	got, err := readCommandInput("", "", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil input, got %q", string(got))
	}
}

func TestReadDataInputFromFileOrArgFromPositionalArg(t *testing.T) {
	got, err := readCommandInput("", `{"initMode":"new"}`, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != `{"initMode":"new"}` {
		t.Fatalf("unexpected data: %q", string(got))
	}
}

func TestReadDataInputFromFileOrArgFromFile(t *testing.T) {
	tempDir := t.TempDir()
	specPath := filepath.Join(tempDir, "spec.json")
	if err := os.WriteFile(specPath, []byte(`{"initMode":"new"}`), 0o600); err != nil {
		t.Fatalf("write temp spec: %v", err)
	}

	got, err := readCommandInput(specPath, "", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != `{"initMode":"new"}` {
		t.Fatalf("unexpected data: %q", string(got))
	}
}
