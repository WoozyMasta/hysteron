// Copyright 2015 Sorint.lab
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
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// SyncFromFollowedPGRewind synchronizes from a source using pg_rewind.
func (p *Manager) SyncFromFollowedPGRewind(followedConnParams ConnParams, password string) error {
	// Remove postgresql.auto.conf since pg_rewind will error if it's a symlink to /dev/null.
	pgAutoConfPath := filepath.Join(p.dataDir, postgresAutoConf)
	if err := os.Remove(pgAutoConfPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error removing postgresql.auto.conf file: %v", err)
	}

	// os.CreateTemp creates files with 0600 permissions.
	pgpass, err := os.CreateTemp("", "pgpass")
	if err != nil {
		return err
	}
	defer ignoreRemove(pgpass.Name())
	defer ignoreClose(pgpass)

	host := followedConnParams.Get("host")
	port := followedConnParams.Get("port")
	user := followedConnParams.Get("user")
	if _, err := fmt.Fprintf(pgpass, "%s:%s:*:%s:%s\n", host, port, user, password); err != nil {
		return err
	}

	// Disable synchronous commits. pg_rewind needs to create a
	// temporary table on the master but if synchronous replication is
	// enabled and there're no active standbys it will hang.
	followedConnParams.Set("options", "-c synchronous_commit=off")
	followedConnString := followedConnParams.ConnString()

	zl().Info().Msg("running pg_rewind")
	name := filepath.Join(p.pgBinPath, "pg_rewind")
	cmd := exec.Command(name, "--debug", "-D", p.dataDir, "--source-server="+followedConnString)
	cmd.Env = append(os.Environ(), "PGPASSFILE="+pgpass.Name())
	zl().Debug().Str("path", name).Strs("args", cmd.Args).Msg("execing cmd")

	// Pipe command's std[err|out] to parent.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("error: %v", err)
	}
	return nil
}

// SyncFromFollowed synchronizes from a source using pg_basebackup.
func (p *Manager) SyncFromFollowed(followedConnParams ConnParams, replSlot string) error {
	followedConnCopy := followedConnParams.Copy()

	// os.CreateTemp creates files with 0600 permissions.
	pgpass, err := os.CreateTemp("", "pgpass")
	if err != nil {
		return err
	}
	defer ignoreRemove(pgpass.Name())
	defer ignoreClose(pgpass)

	host := followedConnCopy.Get("host")
	port := followedConnCopy.Get("port")
	user := followedConnCopy.Get("user")
	password := followedConnCopy.Get("password")
	if _, err = fmt.Fprintf(pgpass, "%s:%s:*:%s:%s\n", host, port, user, password); err != nil {
		return err
	}

	// Remove password from the params passed to pg_basebackup.
	followedConnCopy.Del("password")

	// Disable synchronous commits. pg_basebackup calls
	// pg_start_backup()/pg_stop_backup() on the master but if synchronous
	// replication is enabled and there're no active standbys they will hang.
	followedConnCopy.Set("options", "-c synchronous_commit=off")
	followedConnString := followedConnCopy.ConnString()

	tablespaceMappings, err := p.tablespaceMappingsForBasebackup(followedConnParams)
	if err != nil {
		return err
	}

	zl().Info().Msg("running pg_basebackup")
	name := filepath.Join(p.pgBinPath, "pg_basebackup")
	args := []string{"-R", "-v", "-P", "-Xs", "-D", p.dataDir, "-d", followedConnString}
	if p.walDirConfigured {
		args = append(args, "--waldir", p.walDir)
	}
	for _, mapping := range tablespaceMappings {
		args = append(args, "--tablespace-mapping", mapping.oldDir+"="+mapping.newDir)
	}
	if replSlot != "" {
		args = append(args, "--slot", replSlot)
	}
	cmd := exec.Command(name, args...)

	cmd.Env = append(os.Environ(), "PGPASSFILE="+pgpass.Name())
	zl().Debug().Str("path", name).Strs("args", cmd.Args).Msg("execing cmd")

	// Pipe pg_basebackup's stderr to our stderr.
	// We do this indirectly so that pg_basebackup doesn't think it's connected to a tty.
	// This ensures that it doesn't print any bare line feeds, which could corrupt other
	// logs.
	// pg_basebackup uses stderr for diagnostic messages and stdout for streaming the backup
	// itself (in some modes; we don't use this). As a result we only need to deal with
	// stderr.
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	go func() {
		if _, err := io.Copy(os.Stderr, stderr); err != nil {
			zl().Error().Err(err).Msg("pg_basebackup failed to copy stderr")
		}
	}()

	if err := cmd.Wait(); err != nil {
		return err
	}

	return nil
}

type tablespaceMapping struct {
	oldDir string
	newDir string
}

func (p *Manager) tablespaceMappingsForBasebackup(
	followedConnParams ConnParams,
) ([]tablespaceMapping, error) {
	if len(p.tablespaceDirRoots) == 0 {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()

	introspectionConnParams := followedConnParams.Copy()
	if introspectionConnParams.Get("dbname") == "" {
		introspectionConnParams.Set("dbname", "postgres")
	}
	locations, err := getUserTablespaceLocations(ctx, introspectionConnParams)
	if err != nil {
		return nil, err
	}
	if len(locations) == 0 {
		return nil, nil
	}

	sort.Strings(locations)
	mappings := make([]tablespaceMapping, 0, len(locations))
	for _, oldDir := range locations {
		root := p.selectTablespaceRootForPath(oldDir)
		newDir := p.tablespaceMappedPath(root, oldDir)
		if err := os.RemoveAll(newDir); err != nil {
			return nil, fmt.Errorf("cleanup mapped tablespace dir %q: %w", newDir, err)
		}
		if err := os.MkdirAll(newDir, 0700); err != nil {
			return nil, fmt.Errorf("create mapped tablespace dir %q: %w", newDir, err)
		}
		mappings = append(mappings, tablespaceMapping{
			oldDir: oldDir,
			newDir: newDir,
		})
	}
	return mappings, nil
}

func (p *Manager) selectTablespaceRootForPath(path string) string {
	for _, root := range p.tablespaceDirRoots {
		if hasPathPrefix(path, root) {
			return root
		}
	}
	return p.tablespaceDirRoots[0]
}

func (p *Manager) tablespaceMappedPath(root string, oldDir string) string {
	keeperDir := filepath.Base(filepath.Dir(p.dataDir))
	base := sanitizePathComponent(filepath.Base(oldDir))
	hash := pathHash(oldDir)
	return filepath.Join(root, keeperDir, base+"-"+hash)
}

func pathHash(path string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(path))
	return fmt.Sprintf("%08x", h.Sum32())
}

func sanitizePathComponent(name string) string {
	if name == "" || name == "." || name == string(filepath.Separator) {
		return "tablespace"
	}
	replacer := strings.NewReplacer(
		"\\", "_",
		"/", "_",
		":", "_",
		" ", "_",
	)
	s := replacer.Replace(name)
	if s == "" {
		return "tablespace"
	}
	return s
}
