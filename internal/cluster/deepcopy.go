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

import (
	"maps"
	"slices"
	"time"
)

// DeepCopy returns an independent copy of the cluster.
func (c *Cluster) DeepCopy() *Cluster {
	if c == nil {
		return nil
	}

	nc := *c
	nc.Spec = c.Spec.DeepCopy()
	nc.Status.ManualSwitch = copyManualSwitchRequest(c.Status.ManualSwitch)
	nc.Status.PauseUntil = copyTimePtr(c.Status.PauseUntil)
	return &nc
}

// DeepCopy returns an independent copy of the cluster spec.
func (c *ClusterSpec) DeepCopy() *ClusterSpec {
	if c == nil {
		return nil
	}

	nc := *c
	nc.SleepInterval = copyDurationPtr(c.SleepInterval)
	nc.RequestTimeout = copyDurationPtr(c.RequestTimeout)
	nc.ConvergenceTimeout = copyDurationPtr(c.ConvergenceTimeout)
	nc.InitTimeout = copyDurationPtr(c.InitTimeout)
	nc.SyncTimeout = copyDurationPtr(c.SyncTimeout)
	nc.DBWaitReadyTimeout = copyDurationPtr(c.DBWaitReadyTimeout)
	nc.FailInterval = copyDurationPtr(c.FailInterval)
	nc.EnableFailsafeMode = copyBoolPtr(c.EnableFailsafeMode)
	nc.FailsafeProbeInterval = copyDurationPtr(c.FailsafeProbeInterval)
	nc.FailsafeProbeTimeout = copyDurationPtr(c.FailsafeProbeTimeout)
	nc.FailsafeMaxMissingPeers = copyUint16Ptr(c.FailsafeMaxMissingPeers)
	nc.FailsafeTTL = copyDurationPtr(c.FailsafeTTL)
	nc.DeadKeeperRemovalInterval = copyDurationPtr(c.DeadKeeperRemovalInterval)
	nc.ProxyCheckInterval = copyDurationPtr(c.ProxyCheckInterval)
	nc.ProxyTimeout = copyDurationPtr(c.ProxyTimeout)
	nc.MaxStandbys = copyUint16Ptr(c.MaxStandbys)
	nc.MaxStandbysPerSender = copyUint16Ptr(c.MaxStandbysPerSender)
	nc.MaxStandbyLag = copyUint32Ptr(c.MaxStandbyLag)
	nc.SynchronousReplication = copyBoolPtr(c.SynchronousReplication)
	nc.ReplicationTLSMode = copyReplicationTLSModePtr(c.ReplicationTLSMode)
	nc.UnsafeAutoFailback = copyBoolPtr(c.UnsafeAutoFailback)
	nc.AutoFailbackMinUptime = copyDurationPtr(c.AutoFailbackMinUptime)
	nc.AutoFailbackCooldown = copyDurationPtr(c.AutoFailbackCooldown)
	nc.MinSynchronousStandbys = copyUint16Ptr(c.MinSynchronousStandbys)
	nc.MaxSynchronousStandbys = copyUint16Ptr(c.MaxSynchronousStandbys)
	nc.AdditionalWalSenders = copyUint16Ptr(c.AdditionalWalSenders)
	nc.UsePgrewind = copyBoolPtr(c.UsePgrewind)
	nc.CheckpointBeforePgrewind = copyBoolPtr(c.CheckpointBeforePgrewind)
	nc.InitMode = copyClusterInitModePtr(c.InitMode)
	nc.MergePgParameters = copyBoolPtr(c.MergePgParameters)
	nc.Role = copyClusterRolePtr(c.Role)
	nc.NewConfig = copyNewConfig(c.NewConfig)
	nc.PITRConfig = copyPITRConfig(c.PITRConfig)
	nc.ExistingConfig = copyExistingConfig(c.ExistingConfig)
	nc.StandbyConfig = copyStandbyConfig(c.StandbyConfig)
	nc.DefaultSUReplAccessMode = copySUReplAccessModePtr(c.DefaultSUReplAccessMode)
	nc.PGParameters = maps.Clone(c.PGParameters)
	nc.AutomaticPgRestart = copyBoolPtr(c.AutomaticPgRestart)
	nc.MemberReplicationSlotTTL = copyDurationPtr(c.MemberReplicationSlotTTL)
	nc.AdditionalMasterReplicationSlots = slices.Clone(c.AdditionalMasterReplicationSlots)
	nc.IgnoreMasterReplicationSlots = slices.Clone(c.IgnoreMasterReplicationSlots)
	nc.IgnoreMasterReplicationSlotMatchers = slices.Clone(c.IgnoreMasterReplicationSlotMatchers)
	nc.ManagedLogicalReplicationSlots = slices.Clone(c.ManagedLogicalReplicationSlots)
	nc.PGHBA = slices.Clone(c.PGHBA)
	return &nc
}

// DeepCopy returns an independent copy of cluster data.
func (c *ClusterData) DeepCopy() *ClusterData {
	if c == nil {
		return nil
	}

	nc := *c
	nc.Cluster = c.Cluster.DeepCopy()
	if c.Keepers != nil {
		nc.Keepers = make(Keepers, len(c.Keepers))
		for uid, keeper := range c.Keepers {
			nc.Keepers[uid] = copyKeeper(keeper)
		}
	}
	if c.DBs != nil {
		nc.DBs = make(DBs, len(c.DBs))
		for uid, db := range c.DBs {
			nc.DBs[uid] = copyDB(db)
		}
	}

	nc.Proxy = copyProxy(c.Proxy)
	return &nc
}

func copyDurationPtr(d *Duration) *Duration {
	if d == nil {
		return nil
	}

	nd := *d
	return &nd
}

func copyTimePtr(v *time.Time) *time.Time {
	if v == nil {
		return nil
	}

	nv := *v
	return &nv
}

func copyManualSwitchRequest(v *ManualSwitchRequest) *ManualSwitchRequest {
	if v == nil {
		return nil
	}
	nv := *v
	return &nv
}

func copyBoolPtr(b *bool) *bool {
	if b == nil {
		return nil
	}

	nb := *b
	return &nb
}

func copyUint16Ptr(v *uint16) *uint16 {
	if v == nil {
		return nil
	}

	nv := *v
	return &nv
}

func copyUint32Ptr(v *uint32) *uint32 {
	if v == nil {
		return nil
	}

	nv := *v
	return &nv
}

func copyClusterInitModePtr(v *ClusterInitMode) *ClusterInitMode {
	if v == nil {
		return nil
	}

	nv := *v
	return &nv
}

func copyClusterRolePtr(v *ClusterRole) *ClusterRole {
	if v == nil {
		return nil
	}

	nv := *v
	return &nv
}

func copySUReplAccessModePtr(v *SUReplAccessMode) *SUReplAccessMode {
	if v == nil {
		return nil
	}

	nv := *v
	return &nv
}

func copyReplicationTLSModePtr(v *ReplicationTLSMode) *ReplicationTLSMode {
	if v == nil {
		return nil
	}

	nv := *v
	return &nv
}

func copyNewConfig(v *NewConfig) *NewConfig {
	if v == nil {
		return nil
	}

	nv := *v
	return &nv
}

func copyPITRConfig(v *PITRConfig) *PITRConfig {
	if v == nil {
		return nil
	}

	nv := *v
	nv.ArchiveRecoverySettings = copyArchiveRecoverySettings(v.ArchiveRecoverySettings)
	nv.RecoveryTargetSettings = copyRecoveryTargetSettings(v.RecoveryTargetSettings)
	return &nv
}

func copyExistingConfig(v *ExistingConfig) *ExistingConfig {
	if v == nil {
		return nil
	}

	nv := *v
	return &nv
}

func copyStandbyConfig(v *StandbyConfig) *StandbyConfig {
	if v == nil {
		return nil
	}

	nv := *v
	nv.StandbySettings = copyStandbySettings(v.StandbySettings)
	nv.ArchiveRecoverySettings = copyArchiveRecoverySettings(v.ArchiveRecoverySettings)
	return &nv
}

func copyArchiveRecoverySettings(v *ArchiveRecoverySettings) *ArchiveRecoverySettings {
	if v == nil {
		return nil
	}

	nv := *v
	return &nv
}

func copyRecoveryTargetSettings(v *RecoveryTargetSettings) *RecoveryTargetSettings {
	if v == nil {
		return nil
	}

	nv := *v
	return &nv
}

func copyStandbySettings(v *StandbySettings) *StandbySettings {
	if v == nil {
		return nil
	}

	nv := *v
	return &nv
}

func copyKeeper(k *Keeper) *Keeper {
	if k == nil {
		return nil
	}

	nk := *k
	if k.Spec != nil {
		nk.Spec = &KeeperSpec{}
	}
	nk.Status.CanBeMaster = copyBoolPtr(k.Status.CanBeMaster)
	nk.Status.CanBeSynchronousReplica = copyBoolPtr(k.Status.CanBeSynchronousReplica)

	return &nk
}

func copyDB(db *DB) *DB {
	if db == nil {
		return nil
	}

	ndb := *db
	ndb.Spec = copyDBSpec(db.Spec)
	ndb.Status = copyDBStatus(db.Status)

	return &ndb
}

func copyDBSpec(s *DBSpec) *DBSpec {
	if s == nil {
		return nil
	}

	ns := *s
	ns.NewConfig = copyNewConfig(s.NewConfig)
	ns.PITRConfig = copyPITRConfig(s.PITRConfig)
	ns.PGParameters = maps.Clone(s.PGParameters)
	ns.FollowConfig = copyFollowConfig(s.FollowConfig)
	ns.AdditionalReplicationSlots = slices.Clone(s.AdditionalReplicationSlots)
	ns.IgnoreReplicationSlots = slices.Clone(s.IgnoreReplicationSlots)
	ns.IgnoreReplicationSlotMatchers = slices.Clone(s.IgnoreReplicationSlotMatchers)
	ns.ManagedLogicalReplicationSlots = slices.Clone(s.ManagedLogicalReplicationSlots)
	ns.PGHBA = slices.Clone(s.PGHBA)
	ns.Followers = slices.Clone(s.Followers)
	ns.SynchronousStandbys = slices.Clone(s.SynchronousStandbys)
	ns.ExternalSynchronousStandbys = slices.Clone(s.ExternalSynchronousStandbys)

	return &ns
}

func copyFollowConfig(v *FollowConfig) *FollowConfig {
	if v == nil {
		return nil
	}

	nv := *v
	nv.StandbySettings = copyStandbySettings(v.StandbySettings)
	nv.ArchiveRecoverySettings = copyArchiveRecoverySettings(v.ArchiveRecoverySettings)

	return &nv
}

func copyDBStatus(s DBStatus) DBStatus {
	ns := s
	ns.PGParameters = maps.Clone(s.PGParameters)
	ns.OrphanMemberSlots = maps.Clone(s.OrphanMemberSlots)
	ns.ManagedLogicalSlots = maps.Clone(s.ManagedLogicalSlots)
	ns.TimelinesHistory = copyPostgresTimelinesHistory(s.TimelinesHistory)
	ns.CurSynchronousStandbys = slices.Clone(s.CurSynchronousStandbys)
	ns.SynchronousStandbys = slices.Clone(s.SynchronousStandbys)
	return ns
}

func copyPostgresTimelinesHistory(h PostgresTimelinesHistory) PostgresTimelinesHistory {
	if h == nil {
		return nil
	}

	nh := make(PostgresTimelinesHistory, len(h))
	for i, entry := range h {
		if entry == nil {
			continue
		}
		entryCopy := *entry
		nh[i] = &entryCopy
	}

	return nh
}

func copyProxy(p *Proxy) *Proxy {
	if p == nil {
		return nil
	}

	np := *p
	np.Spec.EnabledProxies = slices.Clone(p.Spec.EnabledProxies)
	return &np
}
