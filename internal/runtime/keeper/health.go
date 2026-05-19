// Copyright 20[0-9][0-9](?:-20[0-9][0-9])? (?:Sorint\.lab|WoozyMasta)(?:\nCopyright 2026 WoozyMasta)?
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
	"context"
	"errors"
)

var (
	errKeeperStartupIncomplete = errors.New("keeper startup is not complete yet")
	errKeeperPostgresNotReady  = errors.New("postgres is not healthy")
)

func (p *PostgresKeeper) probeStartup(_ context.Context) error {
	p.pgStateMutex.Lock()
	defer p.pgStateMutex.Unlock()
	if p.lastPGState == nil {
		return errKeeperStartupIncomplete
	}
	return nil
}

func (p *PostgresKeeper) probeReady(_ context.Context) error {
	p.pgStateMutex.Lock()
	defer p.pgStateMutex.Unlock()
	if p.lastPGState == nil {
		return errKeeperStartupIncomplete
	}
	if !p.lastPGState.Healthy {
		return errKeeperPostgresNotReady
	}
	return nil
}
