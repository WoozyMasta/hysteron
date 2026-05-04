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

package configfile

import "testing"

func TestClusterSpecExpansion(t *testing.T) {
	t.Setenv("STOLON_TEST_LOCALE", "C.UTF-8")

	spec, err := ClusterSpec([]byte(`{"initMode":"new","newConfig":{"locale":"${STOLON_TEST_LOCALE}"}}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.NewConfig == nil || spec.NewConfig.Locale != "C.UTF-8" {
		t.Fatalf("expected locale expansion, got %#v", spec.NewConfig)
	}
}

func TestClusterSpecExpansionDefaultAndEscape(t *testing.T) {
	spec, err := ClusterSpec([]byte(`
initMode: new
newConfig:
  locale: ${STOLON_TEST_MISSING_LOCALE:-C}
  encoding: $${STOLON_TEST_ESCAPED_ENCODING}
`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.NewConfig == nil {
		t.Fatal("expected newConfig")
	}
	if spec.NewConfig.Locale != "C" {
		t.Fatalf("expected default locale, got %q", spec.NewConfig.Locale)
	}
	if spec.NewConfig.Encoding != "${STOLON_TEST_ESCAPED_ENCODING}" {
		t.Fatalf("expected escaped encoding, got %q", spec.NewConfig.Encoding)
	}
}

func TestClusterSpecExpansionRequiredError(t *testing.T) {
	_, err := ClusterSpec([]byte(`{"initMode":"new","newConfig":{"locale":"${STOLON_TEST_REQUIRED_LOCALE:?missing locale}"}}`))
	if err == nil {
		t.Fatal("expected required expansion error")
	}
}

// TestClusterSpecExpansionInShellAndPGStrings verifies that expansion is
// applied uniformly to every string scalar, including PostgreSQL command
// strings and pgHBA entries. Users can use the `$${VAR}` escape to keep
// a literal `${VAR}` in their config when needed.
func TestClusterSpecExpansionInShellAndPGStrings(t *testing.T) {
	t.Setenv("STOLON_TEST_RESTORE_BUCKET", "s3://my-bucket")
	t.Setenv("STOLON_TEST_HBA_CIDR", "10.0.0.0/8")
	data := []byte(`
initMode: pitr
pitrConfig:
  dataRestoreCommand: 'aws s3 cp ${STOLON_TEST_RESTORE_BUCKET}/%f %p'
pgParameters:
  archive_command: 'cp $${POSTGRES_RUNTIME_VAR} %p /archive/%f'
pgHBA:
  - 'host all all ${STOLON_TEST_HBA_CIDR} md5'
`)
	spec, err := ClusterSpec(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := spec.PITRConfig.DataRestoreCommand; got != "aws s3 cp s3://my-bucket/%f %p" {
		t.Fatalf("dataRestoreCommand expansion wrong: %q", got)
	}
	if got := spec.PGParameters["archive_command"]; got != "cp ${POSTGRES_RUNTIME_VAR} %p /archive/%f" {
		t.Fatalf("archive_command escape wrong: %q", got)
	}
	if got := spec.PGHBA[0]; got != "host all all 10.0.0.0/8 md5" {
		t.Fatalf("pgHBA expansion wrong: %q", got)
	}
}
