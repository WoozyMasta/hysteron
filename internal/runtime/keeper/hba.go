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
	"sort"

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	"github.com/woozymasta/hysteron/internal/log"
)

// IsMaster reports whether db should be treated as cluster master.
//
// A database is treated as master when:
//   - it has the `master` role;
//   - it has the `standby` role with external follow type.
func IsMaster(db *cluster.DB) bool {
	switch db.Spec.Role {
	case common.RoleMaster:
		return true

	case common.RoleStandby:
		if db.Spec.FollowConfig.Type == cluster.FollowTypeExternal {
			return true
		}
		return false

	default:
		common.MustNotMsg(true, "invalid db role in db Spec")
		return false
	}
}

// generateHBA generates the instance hba entries depending on the value of
// DefaultSUReplAccessMode.
// When onlyInternal is true only rules needed for replication will be setup
// and the traffic should be permitted only for pgSUUsername standard
// connections and pgReplUsername replication connections.
func (p *PostgresKeeper) generateHBA(
	cd *cluster.ClusterData,
	db *cluster.DB,
	onlyInternal bool,
) []string {
	// Minimal entries for local normal and replication connections needed by the hysteron keeper
	// Matched local connections are for postgres database and suUsername user with md5 auth
	// Matched local replication connections are for replUsername user with md5 auth
	computedHBA := []string{
		fmt.Sprintf(
			"local postgres %s %s",
			p.pgSUUsername,
			p.pgSUAuthMethod,
		),
		fmt.Sprintf(
			"local replication %s %s",
			p.pgReplUsername,
			p.pgReplAuthMethod,
		),
	}

	switch *cd.Cluster.DefSpec().DefaultSUReplAccessMode {
	// all the keepers will accept connections from every host
	case cluster.SUReplAccessAll:
		computedHBA = append(
			computedHBA,
			fmt.Sprintf(
				"host all %s %s %s",
				p.pgSUUsername,
				"0.0.0.0/0",
				p.pgSUAuthMethod,
			),
			fmt.Sprintf(
				"host all %s %s %s",
				p.pgSUUsername,
				"::0/0",
				p.pgSUAuthMethod,
			),
			fmt.Sprintf(
				"host replication %s %s %s",
				p.pgReplUsername,
				"0.0.0.0/0",
				p.pgReplAuthMethod,
			),
			fmt.Sprintf(
				"host replication %s %s %s",
				p.pgReplUsername,
				"::0/0",
				p.pgReplAuthMethod,
			),
		)

	// only the master keeper (primary instance or standby of a remote primary when in standby cluster mode)
	// will accept connections only from the other standby keepers IPs
	case cluster.SUReplAccessStrict:
		if IsMaster(db) {
			addresses := []string{}
			for _, dbElt := range cd.DBs {
				if dbElt.UID != db.UID {
					addresses = append(
						addresses,
						dbElt.Status.ListenAddress,
					)
				}
			}
			sort.Strings(addresses)
			for _, address := range addresses {
				computedHBA = append(
					computedHBA,
					fmt.Sprintf(
						"host all %s %s/32 %s",
						p.pgSUUsername,
						address,
						p.pgReplAuthMethod,
					),
					fmt.Sprintf(
						"host replication %s %s/32 %s",
						p.pgReplUsername,
						address,
						p.pgReplAuthMethod,
					),
				)
			}
		}
	}

	if !onlyInternal {
		// By default, if no custom pg_hba entries are provided, accept
		// connections for all databases and users with default client auth.
		defaultClientAuthMethod := "md5"
		if p.pgSUAuthMethod == "scram-sha-256" || p.pgReplAuthMethod == "scram-sha-256" {
			defaultClientAuthMethod = "scram-sha-256"
		}
		if db.Spec.PGHBA != nil {
			computedHBA = append(computedHBA, db.Spec.PGHBA...)
		} else {
			computedHBA = append(
				computedHBA,
				fmt.Sprintf("host all all 0.0.0.0/0 %s", defaultClientAuthMethod),
				fmt.Sprintf("host all all ::0/0 %s", defaultClientAuthMethod),
			)
		}
	}

	// return generated Hba merged with user Hba
	return computedHBA
}

// ensureStandbyWALReplayRunning resumes WAL replay when it is paused on a
// standby, and logs probe/resume failures without aborting reconciliation.
func (p *PostgresKeeper) ensureStandbyWALReplayRunning(replay standbyReplayController, dbUID string) {
	paused, err := replay.IsWALReplayPaused()
	if err != nil {
		p.baseLog().
			Warn().
			Err(err).
			Str(log.FieldDBUID, dbUID).
			Msg("failed to check WAL replay pause status")
		return
	}
	if !paused {
		return
	}

	p.baseLog().
		Warn().
		Str(log.FieldDBUID, dbUID).
		Msg("WAL replay is paused on standby; attempting resume")

	if err := replay.ResumeWALReplay(); err != nil {
		p.baseLog().
			Warn().
			Err(err).
			Str(log.FieldDBUID, dbUID).
			Msg("failed to resume paused WAL replay on standby")
		return
	}
	p.baseLog().
		Info().
		Str(log.FieldDBUID, dbUID).
		Msg("resumed paused WAL replay on standby")
}
