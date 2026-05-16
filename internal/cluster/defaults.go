// Copyright 2016 Sorint.lab
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

package cluster

// DefSpec returns a new ClusterSpec with unspecified values populated with
// their defaults.
func (c *Cluster) DefSpec() *ClusterSpec {
	return c.Spec.WithDefaults()
}

// WithDefaults returns a new ClusterSpec with unspecified values populated
// with their defaults.
func (c *ClusterSpec) WithDefaults() *ClusterSpec {
	// Take a copy of the input ClusterSpec since we don't want to change the original.
	s := c.DeepCopy()
	if s.SleepInterval == nil {
		s.SleepInterval = &Duration{Duration: DefaultSleepInterval}
	}
	if s.RequestTimeout == nil {
		s.RequestTimeout = &Duration{Duration: DefaultRequestTimeout}
	}
	if s.ConvergenceTimeout == nil {
		s.ConvergenceTimeout = &Duration{Duration: DefaultConvergenceTimeout}
	}
	if s.InitTimeout == nil {
		s.InitTimeout = &Duration{Duration: DefaultInitTimeout}
	}
	if s.SyncTimeout == nil {
		s.SyncTimeout = &Duration{Duration: DefaultSyncTimeout}
	}
	if s.DBWaitReadyTimeout == nil {
		s.DBWaitReadyTimeout = &Duration{Duration: DefaultDBWaitReadyTimeout}
	}
	if s.FailInterval == nil {
		s.FailInterval = &Duration{Duration: DefaultFailInterval}
	}
	if s.EnableFailsafeMode == nil {
		s.EnableFailsafeMode = BoolP(DefaultEnableFailsafeMode)
	}
	if s.FailsafeProbeInterval == nil {
		s.FailsafeProbeInterval = &Duration{Duration: DefaultFailsafeProbeInterval}
	}
	if s.FailsafeProbeTimeout == nil {
		s.FailsafeProbeTimeout = &Duration{Duration: DefaultFailsafeProbeTimeout}
	}
	if s.FailsafeMaxMissingPeers == nil {
		s.FailsafeMaxMissingPeers = Uint16P(DefaultFailsafeMaxMissingPeers)
	}
	if s.FailsafeTTL == nil {
		s.FailsafeTTL = &Duration{Duration: DefaultFailsafeTTL}
	}
	if s.DeadKeeperRemovalInterval == nil {
		s.DeadKeeperRemovalInterval = &Duration{Duration: DefaultDeadKeeperRemovalInterval}
	}
	if s.ProxyCheckInterval == nil {
		s.ProxyCheckInterval = &Duration{Duration: DefaultProxyCheckInterval}
	}
	if s.ProxyTimeout == nil {
		s.ProxyTimeout = &Duration{Duration: DefaultProxyTimeout}
	}
	if s.MaxStandbys == nil {
		s.MaxStandbys = new(DefaultMaxStandbys)
	}
	if s.MaxStandbysPerSender == nil {
		s.MaxStandbysPerSender = new(DefaultMaxStandbysPerSender)
	}
	if s.MaxStandbyLag == nil {
		s.MaxStandbyLag = Uint32P(DefaultMaxStandbyLag)
	}
	if s.SynchronousReplication == nil {
		s.SynchronousReplication = new(DefaultSynchronousReplication)
	}
	if s.UnsafeAutoFailback == nil {
		s.UnsafeAutoFailback = new(DefaultUnsafeAutoFailback)
	}
	if s.AutoFailbackMinUptime == nil {
		s.AutoFailbackMinUptime = &Duration{Duration: DefaultAutoFailbackMinUptime}
	}
	if s.AutoFailbackCooldown == nil {
		s.AutoFailbackCooldown = &Duration{Duration: DefaultAutoFailbackCooldown}
	}
	if s.UsePgrewind == nil {
		s.UsePgrewind = new(DefaultUsePgrewind)
	}
	if s.MinSynchronousStandbys == nil {
		s.MinSynchronousStandbys = new(DefaultMinSynchronousStandbys)
	}
	if s.MaxSynchronousStandbys == nil {
		s.MaxSynchronousStandbys = new(DefaultMaxSynchronousStandbys)
	}
	if s.AdditionalWalSenders == nil {
		s.AdditionalWalSenders = Uint16P(DefaultAdditionalWalSenders)
	}
	if s.MergePgParameters == nil {
		s.MergePgParameters = new(DefaultMergePGParameter)
	}
	if s.DefaultSUReplAccessMode == nil {
		v := DefaultSUReplAccess
		s.DefaultSUReplAccessMode = &v
	}
	if s.Role == nil {
		v := DefaultRole
		s.Role = &v
	}
	if s.AutomaticPgRestart == nil {
		s.AutomaticPgRestart = new(DefaultAutomaticPgRestart)
	}
	return s
}
