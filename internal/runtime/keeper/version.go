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

package keeper

import (
	"fmt"

	"github.com/woozymasta/hysteron/internal/postgresql"
)

// binaryVersion returns PostgreSQL binary major/minor version used by keeper.
// Tests may override the reader via pgBinaryVersion hook.
func (p *PostgresKeeper) binaryVersion() (int, int, error) {
	if p.pgBinaryVersion != nil {
		return p.pgBinaryVersion()
	}
	return p.pgm.BinaryVersion()
}

// validatePostgresVersion validates runtime PostgreSQL version against supported
// and legacy major sets, honoring explicit "allow newer" configuration.
func (p *PostgresKeeper) validatePostgresVersion() error {
	major, minor, err := p.binaryVersion()
	if err != nil {
		return fmt.Errorf(
			"failed to get postgres binary version: %w",
			err,
		)
	}
	if err := postgresql.ValidateKnownMajorVersion(major); err != nil {
		if p.cfg.AllowNewerPG && major > postgresql.MaxKnownMajorVersion() {
			p.baseLog().Warn().
				Str("pg_version", fmt.Sprintf("%d.%d", major, minor)).
				Str("supported_major_versions", postgresql.SupportedMajorVersionsString()).
				Str("legacy_major_versions", postgresql.SupportedLegacyMajorVersionsString()).
				Msg("newer unsupported PostgreSQL version allowed by configuration")
			return nil
		}
		return fmt.Errorf(
			"%w; use --allow-newer-postgres-version only for newer majors than %d",
			err,
			postgresql.MaxKnownMajorVersion(),
		)
	}
	if postgresql.IsLegacySupportedMajorVersion(major) {
		p.baseLog().Warn().
			Str("pg_version", fmt.Sprintf("%d.%d", major, minor)).
			Str("supported_major_versions", postgresql.SupportedMajorVersionsString()).
			Str("legacy_major_versions", postgresql.SupportedLegacyMajorVersionsString()).
			Msg("PostgreSQL major version is legacy best-effort; behavior is not guaranteed")
		return nil
	}
	p.baseLog().
		Info().
		Str("pg_version", fmt.Sprintf("%d.%d", major, minor)).
		Msg("PostgreSQL version is supported")
	return nil
}
