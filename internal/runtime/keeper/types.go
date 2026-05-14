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
	"maps"

	"github.com/woozymasta/hysteron/internal/common"
)

// LocalState is the keeper state persisted on local disk.
type LocalState struct {
	// UID is persistent keeper UID.
	UID string
	// ClusterUID is current cluster binding for this keeper.
	ClusterUID string
}

// DBLocalState is the local database state persisted by the keeper.
type DBLocalState struct {
	// InitPGParameters contains the postgres parameter after the
	// initialization
	InitPGParameters common.Parameters
	// UID is persistent DB UID assigned to this keeper.
	UID string
	// Generation is desired DB generation persisted locally.
	Generation int64
	// Initializing registers when the db is initializing. Needed to detect
	// when the initialization has failed.
	Initializing bool
}

// DeepCopy returns an independent copy of the local database state.
func (s *DBLocalState) DeepCopy() *DBLocalState {
	if s == nil {
		return nil
	}

	ns := *s
	if s.InitPGParameters != nil {
		ns.InitPGParameters = make(common.Parameters, len(s.InitPGParameters))
		maps.Copy(ns.InitPGParameters, s.InitPGParameters)
	}

	return &ns
}
