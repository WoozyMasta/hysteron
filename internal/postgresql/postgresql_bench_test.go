// Copyright 2026 Sorint.lab
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

var benchmarkConnParams = ConnParams{
	"host":             "127.0.0.1",
	"port":             "5432",
	"user":             "postgres user",
	"password":         `pa'ss\word`,
	"dbname":           "postgres",
	"application_name": "stolon keeper",
	"sslmode":          "disable",
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
