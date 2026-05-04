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
	"os"
	"path/filepath"
	"testing"

	"github.com/sorintlab/stolon/internal/common"
	slog "github.com/sorintlab/stolon/internal/log"

	"go.uber.org/zap"
)

var benchmarkConnParams = ConnParams{
	"host":             "127.0.0.1",
	"port":             "5432",
	"user":             "postgres user",
	"password":         `pa'ss\word`,
	"dbname":           "postgres",
	"application_name": "stolon keeper",
	"sslmode":          "disable",
}

func init() {
	slog.SetLevel(zap.ErrorLevel)
}

func BenchmarkParseConnString(b *testing.B) {
	const connString = `host=127.0.0.1 port=5432 user='postgres user' password='pa\'ss\\word' dbname=postgres application_name=stolon\ keeper sslmode=disable`

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		params, err := ParseConnString(connString)
		if err != nil {
			b.Fatal(err)
		}
		if params.Get("host") == "" {
			b.Fatal("empty host")
		}
	}
}

func BenchmarkURLToConnParams(b *testing.B) {
	const connURL = "postgres://postgres:secret@127.0.0.1:5432/postgres?sslmode=disable&application_name=stolon"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		params, err := URLToConnParams(connURL)
		if err != nil {
			b.Fatal(err)
		}
		if params.Get("dbname") == "" {
			b.Fatal("empty dbname")
		}
	}
}

func BenchmarkConnParamsConnString(b *testing.B) {
	params := benchmarkConnParams

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		connString := params.ConnString()
		if connString == "" {
			b.Fatal("empty connstring")
		}
	}
}

func BenchmarkParseBinaryVersion(b *testing.B) {
	const version = "postgres (PostgreSQL) 18.3"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		maj, _, err := ParseBinaryVersion(version)
		if err != nil {
			b.Fatal(err)
		}
		if maj != 18 {
			b.Fatalf("unexpected major version %d", maj)
		}
	}
}

func BenchmarkPGLsnToInt(b *testing.B) {
	const lsn = "16/B374D848"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pos, err := PGLsnToInt(lsn)
		if err != nil {
			b.Fatal(err)
		}
		if pos == 0 {
			b.Fatal("empty position")
		}
	}
}

func BenchmarkXlogPosToWalFileNameNoTimeline(b *testing.B) {
	const xlogPos = uint64(0x16B374D848)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		name := XlogPosToWalFileNameNoTimeline(xlogPos)
		if name == "" {
			b.Fatal("empty wal file name")
		}
	}
}

func BenchmarkManagerWriteConf(b *testing.B) {
	manager := benchmarkManager(b)
	manager.SetParameters(benchmarkPGParameters())
	manager.SetRecoveryOptions(benchmarkRecoveryOptions())

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := manager.writeConf(false, true); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkManagerWriteRecoveryConf(b *testing.B) {
	manager := benchmarkManager(b)
	manager.SetRecoveryOptions(benchmarkRecoveryOptions())

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := manager.writeRecoveryConf(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkManagerWriteSignalFiles(b *testing.B) {
	manager := benchmarkManager(b)
	manager.SetRecoveryOptions(benchmarkRecoveryOptions())

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := manager.writeStandbySignal(); err != nil {
			b.Fatal(err)
		}
		if err := manager.writeRecoverySignal(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkManagerWritePgHba(b *testing.B) {
	manager := benchmarkManager(b)
	manager.SetHba([]string{
		"local all all trust",
		"host all all 127.0.0.1/32 md5",
		"host replication repl 127.0.0.1/32 md5",
		"host replication repl ::1/128 md5",
	})

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := manager.writePgHba(); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkManager(b *testing.B) *Manager {
	b.Helper()

	dataDir := filepath.Join(b.TempDir(), "postgres")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		b.Fatal(err)
	}
	return NewManager("", filepath.Dir(dataDir), nil, nil, "trust", "postgres", "", "trust", "repl", "", 0)
}

func benchmarkPGParameters() common.Parameters {
	return common.Parameters{
		"listen_addresses":          "127.0.0.1",
		"port":                      "5432",
		"wal_level":                 "replica",
		"hot_standby":               "on",
		"max_connections":           "200",
		"max_wal_senders":           "16",
		"max_replication_slots":     "16",
		"shared_buffers":            "256MB",
		"synchronous_standby_names": "2 (stolon_db1,stolon_db2)",
	}
}

func benchmarkRecoveryOptions() *RecoveryOptions {
	return &RecoveryOptions{
		RecoveryMode: RecoveryModeStandby,
		RecoveryParameters: common.Parameters{
			"primary_conninfo":         "host=127.0.0.1 port=5432 user=repl sslmode=prefer",
			"primary_slot_name":        "stolon_db1",
			"restore_command":          "wal-g wal-fetch %f %p",
			"recovery_target_timeline": "latest",
		},
	}
}
