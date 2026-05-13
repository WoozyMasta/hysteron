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

import "fmt"

// LogSummaryClusterSpec returns coarse metadata about a cluster spec for structured logs.
// It omits PostgreSQL parameter values, pg_hba bodies, and any connection secrets.
func LogSummaryClusterSpec(s *ClusterSpec) map[string]any {
	if s == nil {
		return map[string]any{"cluster_spec": nil}
	}
	d := s.WithDefaults()
	initMode := ""
	if d.InitMode != nil {
		initMode = string(*d.InitMode)
	}
	role := ""
	if d.Role != nil {
		role = string(*d.Role)
	}
	syncRepl := false
	if d.SynchronousReplication != nil {
		syncRepl = *d.SynchronousReplication
	}
	usePgRewind := false
	if d.UsePgrewind != nil {
		usePgRewind = *d.UsePgrewind
	}
	out := map[string]any{
		"init_mode":                  initMode,
		"role":                       role,
		"pg_parameter_count":         len(d.PGParameters),
		"pg_hba_rule_count":          len(d.PGHBA),
		"synchronous_replication":    syncRepl,
		"use_pg_rewind":              usePgRewind,
		"additional_repl_slot_count": len(d.AdditionalMasterReplicationSlots),
	}
	if d.NewConfig != nil {
		out["new_config_locale"] = d.NewConfig.Locale
		out["new_config_encoding"] = d.NewConfig.Encoding
		out["new_config_data_checksums"] = d.NewConfig.DataChecksums
	}
	if d.ExistingConfig != nil {
		out["existing_keeper_uid"] = d.ExistingConfig.KeeperUID
	}
	if d.PITRConfig != nil {
		out["pitr_config_present"] = true
	}
	if d.StandbyConfig != nil {
		out["standby_config_present"] = true
	}
	return out
}

// LogSummaryClusterData returns a compact, non-sensitive view of cluster data for structured logs.
// It omits specifications, connection strings, credentials, and full PostgreSQL parameter maps.
func LogSummaryClusterData(cd *ClusterData) map[string]any {
	if cd == nil {
		return map[string]any{"cluster_data": nil}
	}
	sum := map[string]any{
		"format_version": cd.FormatVersion,
		"db_count":       len(cd.DBs),
		"keeper_count":   len(cd.Keepers),
	}
	if cd.Cluster != nil {
		sum["cluster_uid"] = cd.Cluster.UID
		sum["cluster_phase"] = string(cd.Cluster.Status.Phase)
		sum["master_db_uid"] = cd.Cluster.Status.Master
	}
	if cd.Proxy != nil {
		sum["proxy_generation"] = cd.Proxy.Generation
		sum["proxy_master_db_uid"] = cd.Proxy.Spec.MasterDBUID
		sum["enabled_proxies_count"] = len(cd.Proxy.Spec.EnabledProxies)
	}
	return sum
}

// LogSummaryPostgresState returns a compact, non-sensitive view of keeper-reported PostgreSQL state.
// PGParameters content is not included (only a count).
func LogSummaryPostgresState(ps *PostgresState) map[string]any {
	if ps == nil {
		return map[string]any{"postgres_state": nil}
	}
	return map[string]any{
		"db_uid":             ps.UID,
		"generation":         ps.Generation,
		"timeline_id":        ps.TimelineID,
		"xlog_pos":           ps.XLogPos,
		"healthy":            ps.Healthy,
		"listen_address":     ps.ListenAddress,
		"port":               ps.Port,
		"system_id":          ps.SystemID,
		"sync_standby_count": len(ps.SynchronousStandbys),
		"pg_parameter_count": len(ps.PGParameters),
	}
}

// LogSummaryDBBrief returns a compact, non-sensitive view of one database object for logs.
func LogSummaryDBBrief(db *DB) map[string]any {
	if db == nil {
		return nil
	}
	m := map[string]any{
		"db_uid":            db.UID,
		"generation":        db.Generation,
		"status_generation": db.Status.CurrentGeneration,
		"healthy":           db.Status.Healthy,
		"timeline_id":       db.Status.TimelineID,
		"xlog_pos":          db.Status.XLogPos,
	}
	if db.Spec != nil {
		m["keeper_uid"] = db.Spec.KeeperUID
		m["role"] = string(db.Spec.Role)
		m["init_mode"] = string(db.Spec.InitMode)
	}
	return m
}

// LogSummaryProxyInfo returns scalar proxy registration fields safe for logs.
func LogSummaryProxyInfo(pi *ProxyInfo) map[string]any {
	if pi == nil {
		return map[string]any{"proxy_info": nil}
	}
	return map[string]any{
		"info_uid":       pi.InfoUID,
		"proxy_uid":      pi.UID,
		"generation":     pi.Generation,
		"proxy_timeout":  pi.ProxyTimeout.String(),
		"listener_count": len(pi.Listeners),
	}
}

// LogSummaryDBList returns brief summaries for an ordered list of databases (e.g. election candidates).
func LogSummaryDBList(dbs []*DB) []any {
	if len(dbs) == 0 {
		return nil
	}
	out := make([]any, 0, len(dbs))
	for _, db := range dbs {
		if m := LogSummaryDBBrief(db); m != nil {
			out = append(out, m)
		}
	}
	return out
}

// LogSummaryDBMap returns brief summaries for every database in a map (order not stable).
func LogSummaryDBMap(dbs map[string]*DB) []any {
	if len(dbs) == 0 {
		return nil
	}
	out := make([]any, 0, len(dbs))
	for _, db := range dbs {
		if m := LogSummaryDBBrief(db); m != nil {
			out = append(out, m)
		}
	}
	return out
}

// LogSummaryKeepersInfo returns a compact view of keeper heartbeats (no full spec blobs).
func LogSummaryKeepersInfo(kinf KeepersInfo) []any {
	if len(kinf) == 0 {
		return nil
	}
	out := make([]any, 0, len(kinf))
	for _, ki := range kinf {
		if ki == nil {
			continue
		}
		m := map[string]any{
			"keeper_uid":  ki.UID,
			"info_uid":    ki.InfoUID,
			"cluster_uid": ki.ClusterUID,
			"boot_uuid":   ki.BootUUID,
		}
		if ki.CanBeMaster != nil {
			m["can_be_master"] = *ki.CanBeMaster
		}
		if ki.CanBeSynchronousReplica != nil {
			m["can_be_synchronous_replica"] = *ki.CanBeSynchronousReplica
		}
		m["pg_binary_version"] = fmt.Sprintf("%d.%d", ki.PostgresBinaryVersion.Maj, ki.PostgresBinaryVersion.Min)
		if ki.PostgresState != nil {
			m["postgres_state"] = LogSummaryPostgresState(ki.PostgresState)
		}
		out = append(out, m)
	}
	return out
}
