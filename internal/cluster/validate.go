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

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/woozymasta/hysteron/internal/common"
	"github.com/woozymasta/hysteron/internal/postgresql"
)

// Validate validates a cluster spec.
func (c *ClusterSpec) Validate() error {
	s := c.WithDefaults()
	if s.SleepInterval.Duration < 0 {
		return errors.New("sleepInterval must be positive")
	}
	if s.RequestTimeout.Duration < 0 {
		return errors.New("requestTimeout must be positive")
	}
	if s.ConvergenceTimeout.Duration < 0 {
		return errors.New("convergenceTimeout must be positive")
	}
	if s.InitTimeout.Duration < 0 {
		return errors.New("initTimeout must be positive")
	}
	if s.SyncTimeout.Duration < 0 {
		return errors.New("syncTimeout must be positive")
	}
	if s.DBWaitReadyTimeout.Duration < 0 {
		return errors.New("dbWaitReadyTimeout must be positive")
	}
	if s.FailInterval.Duration < 0 {
		return errors.New("failInterval must be positive")
	}
	if s.AutoFailbackMinUptime.Duration < 0 {
		return errors.New("autoFailbackMinUptime must be positive")
	}
	if s.AutoFailbackCooldown.Duration < 0 {
		return errors.New("autoFailbackCooldown must be positive")
	}
	if s.FailsafeProbeInterval.Duration < 0 {
		return errors.New("failsafeProbeInterval must be positive")
	}
	if s.FailsafeProbeTimeout.Duration < 0 {
		return errors.New("failsafeProbeTimeout must be positive")
	}
	if s.FailsafeTTL.Duration < 0 {
		return errors.New("failsafeTTL must be positive")
	}
	if s.FailsafeProbeTimeout.Duration > s.FailsafeProbeInterval.Duration {
		return errors.New("failsafeProbeTimeout should be less than or equal to failsafeProbeInterval")
	}
	if s.FailsafeTTL.Duration < s.FailsafeProbeInterval.Duration {
		return errors.New("failsafeTTL should be greater than or equal to failsafeProbeInterval")
	}
	if s.DeadKeeperRemovalInterval.Duration < 0 {
		return errors.New("deadKeeperRemovalInterval must be positive")
	}
	if s.ProxyCheckInterval.Duration < 0 {
		return errors.New("proxyCheckInterval must be positive")
	}
	if s.ProxyTimeout.Duration < 0 {
		return errors.New("proxyTimeout must be positive")
	}
	if s.ProxyCheckInterval.Duration >= s.ProxyTimeout.Duration {
		return errors.New("proxyCheckInterval should be less than proxyTimeout")
	}
	if err := validateHATiming(
		s.SleepInterval.Duration,
		s.RequestTimeout.Duration,
		s.FailInterval.Duration,
	); err != nil {
		return err
	}
	if *s.MaxStandbys < 1 {
		return errors.New("maxStandbys must be at least 1")
	}
	if *s.MaxStandbysPerSender < 1 {
		return errors.New("maxStandbysPerSender must be at least 1")
	}
	if *s.MaxSynchronousStandbys < 1 {
		return errors.New("maxSynchronousStandbys must be at least 1")
	}
	if *s.MaxSynchronousStandbys < *s.MinSynchronousStandbys {
		return errors.New("maxSynchronousStandbys must be greater or equal to minSynchronousStandbys")
	}
	if s.InitMode == nil {
		return errors.New("initMode undefined")
	}
	for _, replicationSlot := range s.AdditionalMasterReplicationSlots {
		if err := validateReplicationSlot(replicationSlot); err != nil {
			return err
		}
	}
	for _, replicationSlot := range s.IgnoreMasterReplicationSlots {
		if err := validateReplicationSlotName(replicationSlot); err != nil {
			return err
		}
	}
	for _, matcher := range s.IgnoreMasterReplicationSlotMatchers {
		if err := validateReplicationSlotMatcher(matcher); err != nil {
			return err
		}
	}
	if s.MemberReplicationSlotTTL != nil && s.MemberReplicationSlotTTL.Duration < 0 {
		return errors.New("memberReplicationSlotTTL must be positive")
	}
	logicalSlotsSeen := map[string]struct{}{}
	for _, slot := range s.ManagedLogicalReplicationSlots {
		if err := validateReplicationSlotName(slot.Name); err != nil {
			return err
		}
		if _, ok := logicalSlotsSeen[slot.Name]; ok {
			return fmt.Errorf("duplicated managedLogicalReplicationSlots name: %q", slot.Name)
		}
		logicalSlotsSeen[slot.Name] = struct{}{}
		if strings.TrimSpace(slot.Database) == "" {
			return fmt.Errorf("managedLogicalReplicationSlots database undefined for slot %q", slot.Name)
		}
		if strings.TrimSpace(slot.Plugin) == "" {
			return fmt.Errorf("managedLogicalReplicationSlots plugin undefined for slot %q", slot.Name)
		}
	}
	if len(s.ManagedLogicalReplicationSlots) > 0 {
		walLevel := strings.ToLower(strings.TrimSpace(s.PGParameters["wal_level"]))
		if walLevel != "logical" {
			return errors.New(
				`managedLogicalReplicationSlots requires pgParameters.wal_level to be set to "logical"`,
			)
		}
	}
	if s.EnableLogicalSlotFailover && len(s.ManagedLogicalReplicationSlots) == 0 {
		return errors.New(
			`enableLogicalSlotFailover requires managedLogicalReplicationSlots to be configured`,
		)
	}
	if s.EnableLogicalSlotFailover {
		if raw, ok := s.PGParameters["hot_standby_feedback"]; ok {
			normalized := strings.ToLower(strings.TrimSpace(raw))
			if normalized != "on" && normalized != "true" && normalized != "1" {
				return errors.New(
					`enableLogicalSlotFailover requires pgParameters.hot_standby_feedback to be enabled (on/true/1)`,
				)
			}
		}
	}

	// The unique validation we're doing on pgHBA entries is that they don't contain a newline character.
	for _, entry := range s.PGHBA {
		if strings.Contains(entry, "\n") {
			return errors.New("pgHBA entries cannot contain newline characters")
		}
	}

	switch *s.InitMode {
	case ClusterInitModeNew:
		if *s.Role == ClusterRoleStandby {
			return errors.New("invalid cluster role standby when initMode is \"new\"")
		}

	case ClusterInitModeExisting:
		if s.ExistingConfig == nil {
			return errors.New("existingConfig undefined. Required when initMode is \"existing\"")
		}
		if s.ExistingConfig.KeeperUID == "" {
			return errors.New("existingConfig.keeperUID undefined")
		}

	case ClusterInitModePITR:
		if s.PITRConfig == nil {
			return errors.New("pitrConfig undefined. Required when initMode is \"pitr\"")
		}
		if s.PITRConfig.DataRestoreCommand == "" {
			return errors.New("pitrConfig.DataRestoreCommand undefined")
		}
		if s.PITRConfig.RecoveryTargetSettings != nil && *s.Role == ClusterRoleStandby {
			return errors.New("cannot define pitrConfig.RecoveryTargetSettings when required cluster role is standby")
		}
		if err := validateRecoveryTargetSettings(s.PITRConfig.RecoveryTargetSettings); err != nil {
			return err
		}

	default:
		return fmt.Errorf("unknown initMode: %q", *s.InitMode)
	}

	switch *s.DefaultSUReplAccessMode {
	case SUReplAccessAll:
	case SUReplAccessStrict:
	default:
		return fmt.Errorf("unknown defaultSUReplAccessMode: %q", *s.DefaultSUReplAccessMode)
	}

	switch *s.Role {
	case ClusterRoleMaster:
	case ClusterRoleStandby:
		if s.StandbyConfig == nil {
			return errors.New("standbyConfig undefined. Required when cluster role is \"standby\"")
		}
	default:
		return fmt.Errorf("unknown role: %q", *s.InitMode)
	}
	return nil
}

func validateHATiming(
	sleepInterval time.Duration,
	requestTimeout time.Duration,
	failInterval time.Duration,
) error {
	// Keep sentinel loop and request retries bounded by fail interval to reduce
	// self-inflicted false unhealthy/failover conditions.
	if sleepInterval+2*requestTimeout > failInterval {
		return errors.New(
			"invalid HA timing: sleepInterval + 2*requestTimeout must be less than or equal to failInterval",
		)
	}
	return nil
}

func validateReplicationSlot(replicationSlot string) error {
	if err := validateReplicationSlotName(replicationSlot); err != nil {
		return err
	}
	if common.IsHysteronName(replicationSlot) {
		return fmt.Errorf("replication slot name is reserved: %q", replicationSlot)
	}
	return nil
}

func validateReplicationSlotName(replicationSlot string) error {
	if !postgresql.IsValidReplSlotName(replicationSlot) {
		return fmt.Errorf("wrong replication slot name: %q", replicationSlot)
	}
	return nil
}

func validateReplicationSlotMatcher(matcher ReplicationSlotMatcher) error {
	if matcher.Name != "" {
		if err := validateReplicationSlotName(matcher.Name); err != nil {
			return err
		}
	}
	switch matcher.Type {
	case "":
	case ReplicationSlotTypePhysical, ReplicationSlotTypeLogical:
	default:
		return fmt.Errorf("wrong replication slot matcher type: %q", matcher.Type)
	}
	if matcher.Type == ReplicationSlotTypePhysical {
		if strings.TrimSpace(matcher.Database) != "" || strings.TrimSpace(matcher.Plugin) != "" {
			return errors.New("physical replication slot matcher cannot define database or plugin")
		}
	}
	if strings.TrimSpace(matcher.Database) != "" || strings.TrimSpace(matcher.Plugin) != "" {
		if matcher.Type != "" && matcher.Type != ReplicationSlotTypeLogical {
			return errors.New("replication slot matcher with database or plugin must have logical type")
		}
	}
	if matcher.Name == "" && matcher.Type == "" &&
		strings.TrimSpace(matcher.Database) == "" &&
		strings.TrimSpace(matcher.Plugin) == "" {
		return errors.New("empty replication slot matcher is not allowed")
	}
	return nil
}

func validateRecoveryTargetSettings(settings *RecoveryTargetSettings) error {
	if settings == nil {
		return nil
	}

	recoveryTarget := strings.TrimSpace(settings.RecoveryTarget)
	if recoveryTarget != "" && recoveryTarget != "immediate" {
		return fmt.Errorf("recoveryTarget must be \"immediate\" when defined, got %q", settings.RecoveryTarget)
	}

	targets := 0
	for _, value := range []string{
		recoveryTarget,
		strings.TrimSpace(settings.RecoveryTargetLsn),
		strings.TrimSpace(settings.RecoveryTargetName),
		strings.TrimSpace(settings.RecoveryTargetTime),
		strings.TrimSpace(settings.RecoveryTargetXid),
	} {
		if value != "" {
			targets++
		}
	}
	if targets > 1 {
		return errors.New(
			"only one recovery target selector can be set among recoveryTarget, recoveryTargetLsn, recoveryTargetName, recoveryTargetTime, recoveryTargetXid",
		)
	}

	return nil
}
