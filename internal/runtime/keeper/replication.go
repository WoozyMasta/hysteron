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
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	"github.com/woozymasta/hysteron/internal/postgresql"
)

// updateReplSlots reconciles managed physical replication slots with current
// slot state in PostgreSQL.
func (p *PostgresKeeper) updateReplSlots(
	curReplSlots []string,
	uid string,
	followersUIDs, additionalReplSlots, ignoredReplSlots []string,
	ignoredSlotMatchers []cluster.ReplicationSlotMatcher,
	memberSlotTTL time.Duration,
	orphanMemberSlots map[string]time.Time,
	physicalSlotState map[string]postgresql.PhysicalReplicationSlot,
	knownDBUIDs map[string]struct{},
) error {
	internalReplSlots, ignoredSlots := managedReplicationSlots(
		uid,
		followersUIDs,
		additionalReplSlots,
		ignoredReplSlots,
		ignoredSlotMatchers,
	)

	// Drop internal replication slots
	for _, slot := range curReplSlots {
		if !common.IsHysteronName(slot) {
			continue
		}
		if _, ignored := ignoredSlots[slot]; ignored {
			continue
		}
		if isIgnoredPhysicalSlotByMatcher(slot, ignoredSlotMatchers) {
			continue
		}
		if _, ok := internalReplSlots[slot]; !ok {
			shouldDrop, reason := shouldDropUnmanagedHysteronSlot(
				slot,
				memberSlotTTL,
				orphanMemberSlots,
				physicalSlotState,
				knownDBUIDs,
				time.Now(),
			)
			if !shouldDrop {
				p.baseLog().
					Debug().
					Str("slot", slot).
					Str("reason", reason).
					Msg("skipping replication slot drop")
				continue
			}
			p.baseLog().
				Info().
				Str("slot", slot).
				Msg("dropping replication slot")
			if err := p.pgm.DropReplicationSlot(slot); err != nil {
				p.baseLog().
					Error().
					Str("slot", slot).
					Err(err).
					Msg("failed to drop replication slot")

				// don't return the error but continue also if drop failed (standby still connected)
			}
		}
	}

	// Create internal replication slots
	for slot := range internalReplSlots {
		if !slices.Contains(curReplSlots, slot) {
			p.baseLog().
				Info().
				Str("slot", slot).
				Msg("creating replication slot")
			if err := p.pgm.CreateReplicationSlot(slot); err != nil {
				p.baseLog().
					Error().
					Str("slot", slot).
					Err(err).
					Msg("failed to create replication slot")
				return err
			}
		}
	}
	return nil
}

// managedReplicationSlots builds desired managed slot set and explicit ignore
// set from cluster spec inputs.
func managedReplicationSlots(
	uid string,
	followersUIDs, additionalReplSlots, ignoredReplSlots []string,
	ignoredSlotMatchers []cluster.ReplicationSlotMatcher,
) (map[string]struct{}, map[string]struct{}) {
	internalReplSlots := map[string]struct{}{}
	ignoredSlots := map[string]struct{}{}

	for _, slot := range ignoredReplSlots {
		ignoredSlots[slot] = struct{}{}
	}

	// Create a list of the wanted internal replication slots.
	for _, followerUID := range followersUIDs {
		if followerUID == uid {
			continue
		}
		slot := common.HysteronName(followerUID)
		if _, ignored := ignoredSlots[slot]; ignored {
			continue
		}
		if isIgnoredPhysicalSlotByMatcher(slot, ignoredSlotMatchers) {
			continue
		}
		internalReplSlots[slot] = struct{}{}
	}

	// Add AdditionalReplicationSlots.
	for _, slot := range additionalReplSlots {
		hysteronSlot := common.HysteronName(slot)
		if _, ignored := ignoredSlots[hysteronSlot]; ignored {
			continue
		}
		if isIgnoredPhysicalSlotByMatcher(hysteronSlot, ignoredSlotMatchers) {
			continue
		}
		internalReplSlots[hysteronSlot] = struct{}{}
	}

	return internalReplSlots, ignoredSlots
}

// isIgnoredPhysicalSlotByMatcher reports whether a physical slot matches any
// configured ignore matcher.
func isIgnoredPhysicalSlotByMatcher(
	slotName string,
	matchers []cluster.ReplicationSlotMatcher,
) bool {
	for _, matcher := range matchers {
		if matcher.Type != "" && matcher.Type != cluster.ReplicationSlotTypePhysical {
			continue
		}
		if matcher.Name != "" && matcher.Name != slotName {
			continue
		}
		if matcher.Database != "" || matcher.Plugin != "" {
			continue
		}
		return true
	}
	return false
}

// isIgnoredLogicalSlotByMatcher reports whether a logical slot matches any
// configured ignore matcher.
func isIgnoredLogicalSlotByMatcher(
	slot postgresql.LogicalReplicationSlot,
	matchers []cluster.ReplicationSlotMatcher,
) bool {
	for _, matcher := range matchers {
		if matcher.Type != "" && matcher.Type != cluster.ReplicationSlotTypeLogical {
			continue
		}
		if matcher.Name != "" && matcher.Name != slot.Name {
			continue
		}
		if matcher.Database != "" && matcher.Database != slot.Database {
			continue
		}
		if matcher.Plugin != "" && matcher.Plugin != slot.Plugin {
			continue
		}
		return true
	}
	return false
}

// shouldDropUnmanagedHysteronSlot applies TTL and safety checks before
// allowing drop of an unmanaged Hysteron slot.
func shouldDropUnmanagedHysteronSlot(
	slot string,
	memberSlotTTL time.Duration,
	orphanMemberSlots map[string]time.Time,
	physicalSlotState map[string]postgresql.PhysicalReplicationSlot,
	knownDBUIDs map[string]struct{},
	now time.Time,
) (bool, string) {
	if memberSlotTTL <= 0 {
		return true, "ttl_disabled"
	}

	if common.IsHysteronName(slot) {
		slotUID := common.NameFromHysteronName(slot)
		if _, known := knownDBUIDs[slotUID]; known {
			if _, tracked := orphanMemberSlots[slot]; !tracked {
				return false, "awaiting_orphan_tracking"
			}
		}
	}

	orphanSince, orphanTracked := orphanMemberSlots[slot]
	if !orphanTracked {
		return true, "not_tracked_orphan"
	}
	if now.Sub(orphanSince) < memberSlotTTL {
		return false, "ttl_not_elapsed"
	}

	slotState, ok := physicalSlotState[slot]
	if !ok {
		return false, "slot_state_missing"
	}
	if slotState.Active {
		return false, "slot_active"
	}
	if slotState.HasXmin {
		return false, "slot_has_xmin"
	}

	return true, "ttl_elapsed"
}

// staleSlotsWithXmin returns unmanaged inactive physical slots retaining xmin.
func staleSlotsWithXmin(
	slots []postgresql.PhysicalReplicationSlot,
	managedSlots, ignoredSlots map[string]struct{},
	ignoredSlotMatchers []cluster.ReplicationSlotMatcher,
) []string {
	stale := []string{}
	for _, slot := range slots {
		if !common.IsHysteronName(slot.Name) {
			continue
		}
		if slot.Active || !slot.HasXmin {
			continue
		}
		if _, ignored := ignoredSlots[slot.Name]; ignored {
			continue
		}
		if isIgnoredPhysicalSlotByMatcher(slot.Name, ignoredSlotMatchers) {
			continue
		}
		if _, managed := managedSlots[slot.Name]; managed {
			continue
		}
		stale = append(stale, slot.Name)
	}
	slices.Sort(stale)
	return stale
}

// managedLogicalSlotsDecision holds logical-slot reconcile actions for master.
type managedLogicalSlotsDecision struct {
	create   []cluster.ManagedLogicalReplicationSlot // Slots to create with desired settings.
	drop     []string                                // Unmanaged slots safe to drop.
	mismatch []string                                // Existing slots with definition mismatch.
	active   []string                                // Drop candidates currently active.
}

// managedLogicalSlotReadiness summarizes standby slot readiness mismatches.
type managedLogicalSlotReadiness struct {
	missing  []string // Desired slots missing on standby.
	mismatch []string // Present slots with database/plugin mismatch.
}

// evaluateBrokenNativeFailoverLogicalSlots finds native failover logical slots
// that are in inconsistent state on standby.
func evaluateBrokenNativeFailoverLogicalSlots(
	desired []cluster.ManagedLogicalReplicationSlot,
	current []postgresql.LogicalReplicationSlot,
	ignoredSlotMatchers []cluster.ReplicationSlotMatcher,
) []string {
	if len(desired) == 0 || len(current) == 0 {
		return nil
	}
	desiredByName := make(map[string]cluster.ManagedLogicalReplicationSlot, len(desired))
	for _, d := range desired {
		desiredByName[d.Name] = d
	}
	broken := make([]string, 0)
	for _, slot := range current {
		want, ok := desiredByName[slot.Name]
		if !ok {
			continue
		}
		if !common.IsHysteronName(slot.Name) {
			continue
		}
		if isIgnoredLogicalSlotByMatcher(slot, ignoredSlotMatchers) {
			continue
		}
		if slot.Database != want.Database || slot.Plugin != want.Plugin {
			continue
		}
		if slot.Failover && !slot.Synced {
			broken = append(broken, slot.Name)
		}
	}
	slices.Sort(broken)
	return broken
}

// logicalSlotAdvanceOperation defines one standby advance action to target LSN.
type logicalSlotAdvanceOperation struct {
	Name      string // Logical slot name.
	Database  string // Database owning the slot.
	TargetLSN uint64 // Target confirmed_flush_lsn to advance to.
}

// queuedLogicalSlotAdvanceOperation stores enqueued standby advance state.
type queuedLogicalSlotAdvanceOperation struct {
	Name       string // Logical slot name.
	Database   string // Database owning the slot.
	DesiredLSN uint64 // Master-side desired slot LSN at enqueue time.
	ReplayLSN  uint64 // Standby replay LSN at enqueue time.
	TargetLSN  uint64 // Bounded target LSN chosen for advance.
}

// shouldEmitLogicalSlotGateNotice gates one-time warning emission for logical
// slot failover mode.
func shouldEmitLogicalSlotGateNotice(enabled, alreadyEmitted bool) bool {
	return enabled && !alreadyEmitted
}

// managedLogicalSlotReadinessSignature builds a stable signature used to avoid
// duplicate readiness warnings.
func managedLogicalSlotReadinessSignature(
	readiness managedLogicalSlotReadiness,
) string {
	parts := make([]string, 0, len(readiness.missing)+len(readiness.mismatch))
	for _, slot := range readiness.missing {
		parts = append(parts, "missing:"+slot)
	}
	for _, slot := range readiness.mismatch {
		parts = append(parts, "mismatch:"+slot)
	}
	if len(parts) == 0 {
		return ""
	}
	slices.Sort(parts)
	return strings.Join(parts, "|")
}

// shouldReconcileManagedLogicalSlots checks whether logical-slot reconciliation
// can run under current PG settings.
func shouldReconcileManagedLogicalSlots(
	desired []cluster.ManagedLogicalReplicationSlot,
	currentPGParameters cluster.PGParameters,
) (bool, string) {
	if len(desired) == 0 {
		return false, "not_configured"
	}
	walLevel := strings.ToLower(strings.TrimSpace(currentPGParameters["wal_level"]))
	if walLevel != "logical" {
		return false, "wal_level_not_logical"
	}
	return true, "enabled"
}

// shouldUseNativeLogicalSlotFailover reports whether PG native failover slots
// are available for current major version.
func shouldUseNativeLogicalSlotFailover(enableLogicalSlotFailover bool, pgMajor int) bool {
	return enableLogicalSlotFailover && pgMajor >= 17
}

// shouldUseStandbyLogicalSlotAdvance reports whether standby advance path can
// run for current version/configuration.
func shouldUseStandbyLogicalSlotAdvance(
	enableLogicalSlotFailover bool,
	pgMajor int,
	noStream bool,
) bool {
	return enableLogicalSlotFailover && pgMajor >= 16 && !noStream
}

// logicalSlotAdvanceRetryKey builds retry key for slot/database pair.
func logicalSlotAdvanceRetryKey(slotName, database string) string {
	return slotName + "@" + database
}

// shouldAttemptLogicalSlotAdvance checks retry backoff before attempting slot
// advance.
func shouldAttemptLogicalSlotAdvance(
	retryAfter map[string]time.Time,
	key string,
	now time.Time,
) bool {
	if retryAfter == nil {
		return true
	}
	next, ok := retryAfter[key]
	if !ok {
		return true
	}
	return !now.Before(next)
}

// markLogicalSlotAdvanceFailure stores next retry time for a failed advance.
func markLogicalSlotAdvanceFailure(
	retryAfter map[string]time.Time,
	key string,
	now time.Time,
	retryDelay time.Duration,
) {
	if retryAfter == nil {
		return
	}
	retryAfter[key] = now.Add(retryDelay)
}

// clearLogicalSlotAdvanceFailure clears retry backoff state for operation key.
func clearLogicalSlotAdvanceFailure(
	retryAfter map[string]time.Time,
	key string,
) {
	if retryAfter == nil {
		return
	}
	delete(retryAfter, key)
}

// pruneLogicalSlotAdvanceRetryAfter removes expired retry backoff entries.
func pruneLogicalSlotAdvanceRetryAfter(
	retryAfter map[string]time.Time,
	now time.Time,
) {
	if retryAfter == nil {
		return
	}
	for key, next := range retryAfter {
		if !now.Before(next) {
			delete(retryAfter, key)
		}
	}
}

// resetLogicalSlotAdvanceRetryState clears retry map and updates retry gauges.
func resetLogicalSlotAdvanceRetryState(
	retryAfter map[string]time.Time,
) {
	if retryAfter == nil {
		logicalSlotStandbyAdvanceRetrySlots.Set(0)
		return
	}
	for key := range retryAfter {
		delete(retryAfter, key)
	}
	logicalSlotStandbyAdvanceRetrySlots.Set(0)
}

// isReplicationSlotActiveError reports SQLSTATE 55006 active-slot conflicts.
func isReplicationSlotActiveError(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "55006"
	}
	return strings.Contains(err.Error(), "SQLSTATE 55006")
}

// notifyLogicalSlotAdvanceWorker signals standby advance worker about new
// queued operations.
func (p *PostgresKeeper) notifyLogicalSlotAdvanceWorker() {
	select {
	case p.logicalSlotAdvanceNotify <- struct{}{}:
	default:
	}
}

// enqueueLogicalSlotAdvanceOperations deduplicates and enqueues standby slot
// advance operations.
func (p *PostgresKeeper) enqueueLogicalSlotAdvanceOperations(
	ops []logicalSlotAdvanceOperation,
	masterLSN map[string]uint64,
	replayLSN uint64,
) int {
	if len(ops) == 0 {
		return 0
	}
	now := time.Now()
	queued := 0

	p.logicalSlotAdvanceMutex.Lock()
	defer p.logicalSlotAdvanceMutex.Unlock()

	pruneLogicalSlotAdvanceRetryAfter(p.logicalSlotStandbyAdvanceRetryAfter, now)
	for _, op := range ops {
		key := logicalSlotAdvanceRetryKey(op.Name, op.Database)
		if !shouldAttemptLogicalSlotAdvance(p.logicalSlotStandbyAdvanceRetryAfter, key, now) {
			logicalSlotStandbyAdvanceSkippedBackoffTotal.Inc()
			continue
		}
		next := queuedLogicalSlotAdvanceOperation{
			Name:       op.Name,
			Database:   op.Database,
			DesiredLSN: masterLSN[op.Name],
			ReplayLSN:  replayLSN,
			TargetLSN:  op.TargetLSN,
		}
		if current, ok := p.logicalSlotAdvancePending[key]; ok && current.TargetLSN >= next.TargetLSN {
			continue
		}
		p.logicalSlotAdvancePending[key] = next
		queued++
	}
	logicalSlotStandbyAdvanceRetrySlots.Set(float64(len(p.logicalSlotStandbyAdvanceRetryAfter)))
	logicalSlotStandbyAdvancePendingSlots.Set(float64(len(p.logicalSlotAdvancePending)))
	if queued > 0 {
		p.notifyLogicalSlotAdvanceWorker()
	}
	return queued
}

// resetLogicalSlotAdvanceState clears pending and retry standby-advance state.
func (p *PostgresKeeper) resetLogicalSlotAdvanceState() {
	p.logicalSlotAdvanceMutex.Lock()
	defer p.logicalSlotAdvanceMutex.Unlock()
	resetLogicalSlotAdvanceRetryState(p.logicalSlotStandbyAdvanceRetryAfter)
	for key := range p.logicalSlotAdvancePending {
		delete(p.logicalSlotAdvancePending, key)
	}
	logicalSlotStandbyAdvancePendingSlots.Set(0)
}

// standbyLogicalSlotAdvanceWorker executes queued logical-slot advances with
// retry backoff.
func (p *PostgresKeeper) standbyLogicalSlotAdvanceWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.logicalSlotAdvanceNotify:
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			p.logicalSlotAdvanceMutex.Lock()
			if len(p.logicalSlotAdvancePending) == 0 {
				logicalSlotStandbyAdvancePendingSlots.Set(0)
				p.logicalSlotAdvanceMutex.Unlock()
				break
			}
			now := time.Now()
			pruneLogicalSlotAdvanceRetryAfter(p.logicalSlotStandbyAdvanceRetryAfter, now)
			var selectedKey string
			var selected queuedLogicalSlotAdvanceOperation
			for key, op := range p.logicalSlotAdvancePending {
				if !shouldAttemptLogicalSlotAdvance(p.logicalSlotStandbyAdvanceRetryAfter, key, now) {
					continue
				}
				selectedKey = key
				selected = op
				delete(p.logicalSlotAdvancePending, key)
				break
			}
			logicalSlotStandbyAdvanceRetrySlots.Set(float64(len(p.logicalSlotStandbyAdvanceRetryAfter)))
			logicalSlotStandbyAdvancePendingSlots.Set(float64(len(p.logicalSlotAdvancePending)))
			p.logicalSlotAdvanceMutex.Unlock()

			if selectedKey == "" {
				break
			}

			logicalSlotStandbyAdvanceAttemptsTotal.Inc()
			if err := p.pgm.AdvanceLogicalReplicationSlot(
				selected.Name,
				selected.Database,
				selected.TargetLSN,
			); err != nil {
				p.logicalSlotAdvanceMutex.Lock()
				markLogicalSlotAdvanceFailure(
					p.logicalSlotStandbyAdvanceRetryAfter,
					selectedKey,
					now,
					p.logicalSlotStandbyAdvanceRetryDelay,
				)
				logicalSlotStandbyAdvanceRetrySlots.Set(float64(len(p.logicalSlotStandbyAdvanceRetryAfter)))
				p.logicalSlotAdvanceMutex.Unlock()
				logicalSlotStandbyAdvanceFailuresTotal.Inc()
				activeConflict := isReplicationSlotActiveError(err)
				if activeConflict {
					logicalSlotStandbyAdvanceActiveConflictsTotal.Inc()
				}
				logEvt := p.baseLog().
					Warn().
					Err(err).
					Str("slot", selected.Name).
					Uint64("desired_lsn", selected.DesiredLSN).
					Uint64("replay_lsn", selected.ReplayLSN).
					Uint64("target_lsn", selected.TargetLSN)
				if activeConflict {
					logEvt = p.baseLog().
						Debug().
						Err(err).
						Str("slot", selected.Name).
						Uint64("desired_lsn", selected.DesiredLSN).
						Uint64("replay_lsn", selected.ReplayLSN).
						Uint64("target_lsn", selected.TargetLSN)
				}
				logEvt.Msg("failed to advance managed logical replication slot on standby")
				continue
			}

			p.logicalSlotAdvanceMutex.Lock()
			clearLogicalSlotAdvanceFailure(p.logicalSlotStandbyAdvanceRetryAfter, selectedKey)
			logicalSlotStandbyAdvanceRetrySlots.Set(float64(len(p.logicalSlotStandbyAdvanceRetryAfter)))
			p.logicalSlotAdvanceMutex.Unlock()
			logicalSlotStandbyAdvanceSuccessTotal.Inc()
		}
	}
}

// computeLogicalSlotAdvanceTarget bounds target LSN by replay and desired
// positions.
func computeLogicalSlotAdvanceTarget(
	desiredLSN uint64,
	replayLSN uint64,
	currentConfirmedFlushLSN uint64,
) (uint64, bool) {
	if desiredLSN == 0 || replayLSN == 0 {
		return 0, false
	}
	target := min(replayLSN, desiredLSN)
	if target <= currentConfirmedFlushLSN {
		return 0, false
	}
	return target, true
}

// masterManagedLogicalSlotLSN returns managed slot LSN map from current master
// DB state.
func masterManagedLogicalSlotLSN(
	dbs cluster.DBs,
) map[string]uint64 {
	for _, db := range dbs {
		if db == nil || db.Spec == nil {
			continue
		}
		if db.Spec.Role != common.RoleMaster {
			continue
		}
		return db.Status.ManagedLogicalSlots
	}
	return nil
}

// logicalSlotLSNMap converts logical slots to a name->confirmed_flush_lsn map.
func logicalSlotLSNMap(
	current []postgresql.LogicalReplicationSlot,
) map[string]uint64 {
	if len(current) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(current))
	for _, slot := range current {
		out[slot.Name] = slot.ConfirmedFlushLSN
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// evaluateManagedLogicalSlotAdvanceOperations plans standby advance operations
// for desired managed logical slots.
func evaluateManagedLogicalSlotAdvanceOperations(
	desired []cluster.ManagedLogicalReplicationSlot,
	current []postgresql.LogicalReplicationSlot,
	masterLSN map[string]uint64,
	replayLSN uint64,
	ignoredSlotMatchers []cluster.ReplicationSlotMatcher,
) []logicalSlotAdvanceOperation {
	if len(desired) == 0 || len(current) == 0 || len(masterLSN) == 0 || replayLSN == 0 {
		return nil
	}
	currentByName := make(map[string]postgresql.LogicalReplicationSlot, len(current))
	for _, slot := range current {
		currentByName[slot.Name] = slot
	}
	ops := make([]logicalSlotAdvanceOperation, 0, len(desired))
	for _, desiredSlot := range desired {
		desiredCurrent := postgresql.LogicalReplicationSlot{
			Name:     desiredSlot.Name,
			Database: desiredSlot.Database,
			Plugin:   desiredSlot.Plugin,
		}
		if isIgnoredLogicalSlotByMatcher(desiredCurrent, ignoredSlotMatchers) {
			continue
		}
		desiredLSN, ok := masterLSN[desiredSlot.Name]
		if !ok {
			continue
		}
		currentSlot, ok := currentByName[desiredSlot.Name]
		if !ok {
			continue
		}
		if isIgnoredLogicalSlotByMatcher(currentSlot, ignoredSlotMatchers) {
			continue
		}
		if currentSlot.Database != desiredSlot.Database || currentSlot.Plugin != desiredSlot.Plugin {
			continue
		}
		target, shouldAdvance := computeLogicalSlotAdvanceTarget(
			desiredLSN,
			replayLSN,
			currentSlot.ConfirmedFlushLSN,
		)
		if !shouldAdvance {
			continue
		}
		ops = append(ops, logicalSlotAdvanceOperation{
			Name:      desiredSlot.Name,
			Database:  desiredSlot.Database,
			TargetLSN: target,
		})
	}
	return ops
}

// enforceHotStandbyFeedbackForLogicalSlotFailover forces
// hot_standby_feedback=on when logical slot failover is enabled.
func enforceHotStandbyFeedbackForLogicalSlotFailover(
	parameters common.Parameters,
	enableLogicalSlotFailover bool,
) {
	if !enableLogicalSlotFailover {
		return
	}
	parameters["hot_standby_feedback"] = "on"
}

// evaluateManagedLogicalSlotsDecision computes create/drop/mismatch sets for
// managed logical slots.
func evaluateManagedLogicalSlotsDecision(
	desired []cluster.ManagedLogicalReplicationSlot,
	current []postgresql.LogicalReplicationSlot,
	ignoredSlotMatchers []cluster.ReplicationSlotMatcher,
) managedLogicalSlotsDecision {
	decision := managedLogicalSlotsDecision{
		create:   make([]cluster.ManagedLogicalReplicationSlot, 0),
		drop:     make([]string, 0),
		mismatch: make([]string, 0),
		active:   make([]string, 0),
	}

	desiredByName := make(map[string]cluster.ManagedLogicalReplicationSlot, len(desired))
	for _, slot := range desired {
		desiredByName[slot.Name] = slot
	}

	currentByName := make(map[string]postgresql.LogicalReplicationSlot, len(current))
	for _, slot := range current {
		currentByName[slot.Name] = slot
	}

	for _, desiredSlot := range desired {
		desiredCurrent := postgresql.LogicalReplicationSlot{
			Name:     desiredSlot.Name,
			Database: desiredSlot.Database,
			Plugin:   desiredSlot.Plugin,
		}
		if isIgnoredLogicalSlotByMatcher(desiredCurrent, ignoredSlotMatchers) {
			continue
		}
		currentSlot, ok := currentByName[desiredSlot.Name]
		if !ok {
			decision.create = append(decision.create, desiredSlot)
			continue
		}
		if currentSlot.Database != desiredSlot.Database || currentSlot.Plugin != desiredSlot.Plugin {
			decision.mismatch = append(decision.mismatch, desiredSlot.Name)
		}
	}

	for _, currentSlot := range current {
		if _, ok := desiredByName[currentSlot.Name]; ok {
			continue
		}
		// Safety-first: clean up only reserved hysteron namespace slots.
		if !common.IsHysteronName(currentSlot.Name) {
			continue
		}
		if isIgnoredLogicalSlotByMatcher(currentSlot, ignoredSlotMatchers) {
			continue
		}
		if currentSlot.Active {
			decision.active = append(decision.active, currentSlot.Name)
			continue
		}
		decision.drop = append(decision.drop, currentSlot.Name)
	}

	slices.Sort(decision.mismatch)
	slices.Sort(decision.drop)
	slices.Sort(decision.active)
	return decision
}

// evaluateManagedLogicalSlotReadiness reports missing or mismatched managed
// logical slots on current node.
func evaluateManagedLogicalSlotReadiness(
	desired []cluster.ManagedLogicalReplicationSlot,
	current []postgresql.LogicalReplicationSlot,
	ignoredSlotMatchers []cluster.ReplicationSlotMatcher,
) managedLogicalSlotReadiness {
	readiness := managedLogicalSlotReadiness{
		missing:  make([]string, 0),
		mismatch: make([]string, 0),
	}

	currentByName := make(map[string]postgresql.LogicalReplicationSlot, len(current))
	for _, slot := range current {
		currentByName[slot.Name] = slot
	}

	for _, desiredSlot := range desired {
		desiredCurrent := postgresql.LogicalReplicationSlot{
			Name:     desiredSlot.Name,
			Database: desiredSlot.Database,
			Plugin:   desiredSlot.Plugin,
		}
		if isIgnoredLogicalSlotByMatcher(desiredCurrent, ignoredSlotMatchers) {
			continue
		}
		currentSlot, ok := currentByName[desiredSlot.Name]
		if !ok {
			readiness.missing = append(readiness.missing, desiredSlot.Name)
			continue
		}
		if isIgnoredLogicalSlotByMatcher(currentSlot, ignoredSlotMatchers) {
			continue
		}
		if currentSlot.Database != desiredSlot.Database || currentSlot.Plugin != desiredSlot.Plugin {
			readiness.mismatch = append(readiness.mismatch, desiredSlot.Name)
		}
	}

	slices.Sort(readiness.missing)
	slices.Sort(readiness.mismatch)
	return readiness
}

// refreshReplicationSlots orchestrates physical and logical slot reconciliation
// for the current DB.
func (p *PostgresKeeper) refreshReplicationSlots(
	cspec *cluster.ClusterSpec,
	db *cluster.DB,
	dbs cluster.DBs,
) error {
	followersUIDs := db.Spec.Followers
	managedSlots, ignoredSlots := managedReplicationSlots(
		db.UID,
		followersUIDs,
		db.Spec.AdditionalReplicationSlots,
		db.Spec.IgnoreReplicationSlots,
		db.Spec.IgnoreReplicationSlotMatchers,
	)
	physicalSlots, err := p.pgm.GetPhysicalReplicationSlots()
	if err != nil {
		p.baseLog().
			Debug().
			Err(err).
			Msg("failed to inspect physical replication slots")
		physicalSlots = nil
	}

	currentReplicationSlots := make([]string, 0, len(physicalSlots))
	for _, slot := range physicalSlots {
		currentReplicationSlots = append(currentReplicationSlots, slot.Name)
	}
	physicalSlotState := map[string]postgresql.PhysicalReplicationSlot{}
	for _, slot := range physicalSlots {
		physicalSlotState[slot.Name] = slot
	}
	memberSlotTTL := time.Duration(0)
	if cspec != nil && cspec.MemberReplicationSlotTTL != nil {
		memberSlotTTL = cspec.MemberReplicationSlotTTL.Duration
	}
	knownDBUIDs := map[string]struct{}{}
	for dbUID := range dbs {
		knownDBUIDs[dbUID] = struct{}{}
	}

	if err = p.updateReplSlots(
		currentReplicationSlots,
		db.UID,
		followersUIDs,
		db.Spec.AdditionalReplicationSlots,
		db.Spec.IgnoreReplicationSlots,
		db.Spec.IgnoreReplicationSlotMatchers,
		memberSlotTTL,
		db.Status.OrphanMemberSlots,
		physicalSlotState,
		knownDBUIDs,
	); err != nil {
		p.baseLog().
			Error().
			Err(err).
			Msg("error updating replication slots")
		return err
	}

	if stale := staleSlotsWithXmin(
		physicalSlots,
		managedSlots,
		ignoredSlots,
		db.Spec.IgnoreReplicationSlotMatchers,
	); len(stale) > 0 {
		p.baseLog().
			Warn().
			Strs("stale_slots", stale).
			Msg("detected inactive unmanaged hysteron physical slots with xmin; consider cleanup to avoid vacuum horizon retention")
	}

	reconcileLogicalSlots, reason := shouldReconcileManagedLogicalSlots(
		db.Spec.ManagedLogicalReplicationSlots,
		db.Status.PGParameters,
	)
	if shouldEmitLogicalSlotGateNotice(
		db.Spec.EnableLogicalSlotFailover,
		p.logicalSlotGateNoticeEmitted,
	) {
		p.baseLog().
			Warn().
			Msg("enableLogicalSlotFailover is experimental: standby path is readiness-only; no standby logical slot create/drop before promotion")
		p.logicalSlotGateNoticeEmitted = true
	}
	if !db.Spec.EnableLogicalSlotFailover {
		p.logicalSlotGateNoticeEmitted = false
	}
	currentLogicalSlots, err := p.pgm.GetLogicalReplicationSlots()
	if err != nil {
		p.baseLog().
			Debug().
			Err(err).
			Msg("failed to inspect logical replication slots")
		return nil
	}

	if !reconcileLogicalSlots {
		if reason == "wal_level_not_logical" {
			p.baseLog().
				Warn().
				Str("wal_level", db.Status.PGParameters["wal_level"]).
				Msg("managed logical replication slots configured but wal_level is not logical; skipping logical slot reconcile")
		}
		if reason == "not_configured" && db.Spec.Role == common.RoleMaster {
			logicalDecision := evaluateManagedLogicalSlotsDecision(
				nil,
				currentLogicalSlots,
				db.Spec.IgnoreReplicationSlotMatchers,
			)
			for _, slot := range logicalDecision.active {
				p.baseLog().
					Warn().
					Str("slot", slot).
					Msg("logical replication slot scheduled for cleanup is active; skipping drop")
			}
			for _, slot := range logicalDecision.drop {
				p.baseLog().Info().
					Str("slot", slot).
					Msg("dropping unmanaged hysteron logical replication slot")
				if err := p.pgm.DropLogicalReplicationSlot(slot); err != nil {
					return fmt.Errorf("failed to drop logical replication slot %q: %w", slot, err)
				}
			}
		}
		return nil
	}

	if db.Spec.Role != common.RoleMaster {
		if db.Spec.EnableLogicalSlotFailover {
			if db.Spec.NoStream {
				if !p.logicalSlotNoStreamNoticeEmitted {
					p.baseLog().Warn().
						Msg("logical slot failover gate enabled but standby noStream=true; skipping standby logical-slot sync/advance path")
					p.logicalSlotNoStreamNoticeEmitted = true
				}
				p.logicalSlotReadinessLast = ""
				p.logicalSlotStandbyAdvanceUnavailableNoticeEmitted = false
				p.resetLogicalSlotAdvanceState()
				return nil
			}
			p.logicalSlotNoStreamNoticeEmitted = false
			pgMajor, _, versionErr := p.pgm.BinaryVersion()
			if versionErr != nil {
				p.baseLog().
					Debug().
					Err(versionErr).
					Msg("failed to detect PostgreSQL binary version for standby logical-slot advance")
			}
			if versionErr == nil && shouldUseStandbyLogicalSlotAdvance(
				db.Spec.EnableLogicalSlotFailover,
				pgMajor,
				db.Spec.NoStream,
			) {
				p.logicalSlotStandbyAdvanceUnavailableNoticeEmitted = false
				masterLSN := masterManagedLogicalSlotLSN(dbs)
				ops := evaluateManagedLogicalSlotAdvanceOperations(
					db.Spec.ManagedLogicalReplicationSlots,
					currentLogicalSlots,
					masterLSN,
					db.Status.XLogPos,
					db.Spec.IgnoreReplicationSlotMatchers,
				)
				if len(ops) > 0 {
					p.baseLog().
						Debug().
						Int("advance_ops", len(ops)).
						Uint64("replay_lsn", db.Status.XLogPos).
						Msg("planned managed logical slot standby advance operations")
				}
				queued := p.enqueueLogicalSlotAdvanceOperations(
					ops,
					masterLSN,
					db.Status.XLogPos,
				)
				if len(ops)-queued > 0 {
					p.baseLog().
						Debug().
						Int("skipped_by_backoff", len(ops)-queued).
						Msg("skipped standby logical-slot advance operations due to retry backoff or dedup")
				}
			} else if versionErr == nil && !p.logicalSlotStandbyAdvanceUnavailableNoticeEmitted {
				p.baseLog().
					Warn().
					Int("pg_major", pgMajor).
					Msg("logical slot failover gate enabled but standby logical-slot advance is unavailable on PostgreSQL < 16")
				p.logicalSlotStandbyAdvanceUnavailableNoticeEmitted = true
			}
			if versionErr == nil && shouldUseNativeLogicalSlotFailover(
				db.Spec.EnableLogicalSlotFailover,
				pgMajor,
			) {
				broken := evaluateBrokenNativeFailoverLogicalSlots(
					db.Spec.ManagedLogicalReplicationSlots,
					currentLogicalSlots,
					db.Spec.IgnoreReplicationSlotMatchers,
				)
				for _, slot := range broken {
					p.baseLog().
						Warn().
						Str("slot", slot).
						Msg("detected inconsistent native failover logical slot on standby (failover=true, synced=false); attempting cleanup")
					if dropErr := p.pgm.DropLogicalReplicationSlot(slot); dropErr != nil {
						p.baseLog().
							Warn().
							Err(dropErr).
							Str("slot", slot).
							Msg("failed to drop inconsistent native failover logical slot on standby")
					}
				}
			}

			readiness := evaluateManagedLogicalSlotReadiness(
				db.Spec.ManagedLogicalReplicationSlots,
				currentLogicalSlots,
				db.Spec.IgnoreReplicationSlotMatchers,
			)
			currentSignature := managedLogicalSlotReadinessSignature(readiness)
			if currentSignature != p.logicalSlotReadinessLast {
				p.logicalSlotReadinessLast = currentSignature
				for _, slot := range readiness.missing {
					p.baseLog().
						Warn().
						Str("slot", slot).
						Msg("logical slot failover gate enabled: standby readiness missing managed logical slot")
				}
				for _, slot := range readiness.mismatch {
					p.baseLog().
						Warn().
						Str("slot", slot).
						Msg("logical slot failover gate enabled: standby logical slot mismatch")
				}
			}
		} else {
			p.logicalSlotReadinessLast = ""
			p.logicalSlotStandbyAdvanceUnavailableNoticeEmitted = false
			p.logicalSlotNoStreamNoticeEmitted = false
			p.resetLogicalSlotAdvanceState()
		}
		return nil
	}
	p.resetLogicalSlotAdvanceState()
	p.logicalSlotReadinessLast = ""

	logicalDecision := evaluateManagedLogicalSlotsDecision(
		db.Spec.ManagedLogicalReplicationSlots,
		currentLogicalSlots,
		db.Spec.IgnoreReplicationSlotMatchers,
	)
	createFailoverSlot := false
	if db.Spec.EnableLogicalSlotFailover {
		pgMajor, _, versionErr := p.pgm.BinaryVersion()
		switch {
		case versionErr != nil:
			p.baseLog().
				Warn().
				Err(versionErr).
				Msg("failed to detect PostgreSQL binary version; creating logical slots without native failover flag")
		case shouldUseNativeLogicalSlotFailover(db.Spec.EnableLogicalSlotFailover, pgMajor):
			createFailoverSlot = true
			if !p.logicalSlotNativeModeNoticeEmitted {
				p.baseLog().
					Info().
					Int("pg_major", pgMajor).
					Msg("logical slot failover gate enabled: using PostgreSQL native logical failover slots")
				p.logicalSlotNativeModeNoticeEmitted = true
			}
			p.logicalSlotLegacyModeNoticeEmitted = false
		default:
			if !p.logicalSlotLegacyModeNoticeEmitted {
				p.baseLog().
					Warn().
					Int("pg_major", pgMajor).
					Msg("enableLogicalSlotFailover is enabled on PostgreSQL < 17; native logical slot failover is unavailable and behavior is experimental")
				p.logicalSlotLegacyModeNoticeEmitted = true
			}
			p.logicalSlotNativeModeNoticeEmitted = false
		}
	} else {
		p.logicalSlotLegacyModeNoticeEmitted = false
		p.logicalSlotNativeModeNoticeEmitted = false
	}

	for _, slot := range logicalDecision.mismatch {
		p.baseLog().
			Warn().
			Str("slot", slot).
			Msg("managed logical replication slot exists with different database or plugin; skipping destructive action")
	}
	for _, slot := range logicalDecision.active {
		p.baseLog().
			Warn().
			Str("slot", slot).
			Msg("logical replication slot scheduled for cleanup is active; skipping drop")
	}
	for _, desiredSlot := range logicalDecision.create {
		p.baseLog().Info().
			Str("slot", desiredSlot.Name).
			Str("database", desiredSlot.Database).
			Str("plugin", desiredSlot.Plugin).
			Msg("creating managed logical replication slot")
		if err := p.pgm.CreateLogicalReplicationSlot(
			desiredSlot.Name,
			desiredSlot.Database,
			desiredSlot.Plugin,
			createFailoverSlot,
		); err != nil {
			return fmt.Errorf(
				"failed to create managed logical replication slot %q: %w",
				desiredSlot.Name,
				err,
			)
		}
	}

	for _, slot := range logicalDecision.drop {
		p.baseLog().Info().
			Str("slot", slot).
			Msg("dropping unmanaged hysteron logical replication slot")
		if err := p.pgm.DropLogicalReplicationSlot(slot); err != nil {
			return fmt.Errorf("failed to drop logical replication slot %q: %w", slot, err)
		}
	}

	return nil
}
