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

package log

import (
	"os"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
)

func TestConfigureInvalidLevel(t *testing.T) {
	saved := zlog.Logger
	t.Cleanup(func() { zlog.Logger = saved })

	_, err := Configure(Options{Level: "nope", Format: "json", Output: "stderr"})
	if err == nil {
		t.Fatal("expected error for invalid level")
	}
	if !strings.Contains(err.Error(), "invalid log level") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConfigureInvalidFormat(t *testing.T) {
	saved := zlog.Logger
	t.Cleanup(func() { zlog.Logger = saved })

	_, err := Configure(Options{Level: "info", Format: "xml", Output: "stderr"})
	if err == nil {
		t.Fatal("expected error for invalid format")
	}
	if !strings.Contains(err.Error(), "invalid log format") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConfigureInvalidFileMode(t *testing.T) {
	saved := zlog.Logger
	t.Cleanup(func() { zlog.Logger = saved })

	_, err := Configure(Options{
		Level:    "info",
		Format:   "json",
		Output:   os.TempDir() + "/stolon-log-test-nonexistent-dir-xyz/db.log",
		FileMode: "rotate",
	})
	if err == nil {
		t.Fatal("expected error for invalid file mode")
	}
	if !strings.Contains(err.Error(), "invalid log file mode") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConfigureEmptyLevelDefaultsInfo(t *testing.T) {
	saved := zlog.Logger
	savedGlobal := zerolog.GlobalLevel()
	t.Cleanup(func() {
		zlog.Logger = saved
		zerolog.SetGlobalLevel(savedGlobal)
	})

	closer, err := Configure(Options{Level: "", Format: "json", Output: "stderr"})
	if closer != nil {
		_ = closer.Close()
	}
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if zerolog.GlobalLevel() != zerolog.InfoLevel {
		t.Fatalf("expected info default, got %v", zerolog.GlobalLevel())
	}
}

func TestConfigureStdoutJSON(t *testing.T) {
	saved := zlog.Logger
	savedGlobal := zerolog.GlobalLevel()
	t.Cleanup(func() {
		zlog.Logger = saved
		zerolog.SetGlobalLevel(savedGlobal)
	})

	closer, err := Configure(Options{
		Level:  "warn",
		Format: "json",
		Output: "stdout",
	})
	if err != nil {
		t.Fatal(err)
	}
	if closer == nil {
		t.Fatal("expected non-nil closer for stdout")
	}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
	if zerolog.GlobalLevel() != zerolog.WarnLevel {
		t.Fatalf("global level: %v", zerolog.GlobalLevel())
	}
}

func TestConfigureTraceLevel(t *testing.T) {
	saved := zlog.Logger
	savedGlobal := zerolog.GlobalLevel()
	t.Cleanup(func() {
		zlog.Logger = saved
		zerolog.SetGlobalLevel(savedGlobal)
	})

	closer, err := Configure(Options{
		Level:  "trace",
		Format: "json",
		Output: "stderr",
	})
	if closer != nil {
		_ = closer.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	if zerolog.GlobalLevel() != zerolog.TraceLevel {
		t.Fatalf("global level: %v", zerolog.GlobalLevel())
	}
	if !IsTrace() {
		t.Fatal("IsTrace() should be true")
	}
}
