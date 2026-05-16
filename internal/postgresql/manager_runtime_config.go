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
	"maps"
	"time"

	"github.com/woozymasta/hysteron/internal/common"
)

// SetParameters sets desired PostgreSQL configuration parameters.
func (p *Manager) SetParameters(parameters common.Parameters) {
	p.parameters = parameters
}

// CurParameters returns the current tracked PostgreSQL parameters.
func (p *Manager) CurParameters() common.Parameters {
	return p.curParameters
}

// SetRecoveryOptions sets desired recovery options.
func (p *Manager) SetRecoveryOptions(recoveryOptions *RecoveryOptions) {
	if recoveryOptions == nil {
		p.recoveryOptions = NewRecoveryOptions()
		return
	}

	p.recoveryOptions = recoveryOptions
}

// CurRecoveryOptions returns the current tracked recovery options.
func (p *Manager) CurRecoveryOptions() *RecoveryOptions {
	return p.curRecoveryOptions
}

// SetHba sets desired pg_hba entries.
func (p *Manager) SetHba(hba []string) {
	p.hba = hba
}

// CurHba returns the current tracked pg_hba entries.
func (p *Manager) CurHba() []string {
	return p.curHba
}

// SetIdent sets desired pg_ident entries.
func (p *Manager) SetIdent(ident []string) {
	p.ident = ident
}

// CurIdent returns the current tracked pg_ident entries.
func (p *Manager) CurIdent() []string {
	return p.curIdent
}

// SetRequestTimeout updates timeout used by PostgreSQL operations.
func (p *Manager) SetRequestTimeout(timeout time.Duration) {
	p.requestTimeoutMu.Lock()
	p.requestTimeout = timeout
	p.requestTimeoutMu.Unlock()
}

func (p *Manager) requestTimeoutValue() time.Duration {
	p.requestTimeoutMu.RLock()
	timeout := p.requestTimeout
	p.requestTimeoutMu.RUnlock()
	return timeout
}

// UpdateCurParameters snapshots desired parameters as current parameters.
func (p *Manager) UpdateCurParameters() error {
	if p.parameters == nil {
		p.curParameters = nil
		return nil
	}

	p.curParameters = make(common.Parameters, len(p.parameters))
	maps.Copy(p.curParameters, p.parameters)
	return nil
}

// UpdateCurRecoveryOptions snapshots desired recovery options.
func (p *Manager) UpdateCurRecoveryOptions() {
	p.curRecoveryOptions = p.recoveryOptions.DeepCopy()
}

// UpdateCurHba snapshots desired pg_hba entries.
func (p *Manager) UpdateCurHba() error {
	if p.hba == nil {
		p.curHba = nil
		return nil
	}

	p.curHba = append([]string(nil), p.hba...)
	return nil
}

// UpdateCurIdent snapshots desired pg_ident entries.
func (p *Manager) UpdateCurIdent() error {
	if p.ident == nil {
		p.curIdent = nil
		return nil
	}

	p.curIdent = append([]string(nil), p.ident...)
	return nil
}
