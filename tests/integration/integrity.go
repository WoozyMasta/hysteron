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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build integration

package integration

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

var sqlIdentRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func tableFingerprint(q Querier, table string) (string, error) {
	if !sqlIdentRe.MatchString(table) {
		return "", fmt.Errorf("invalid table identifier %q", table)
	}
	rows, err := q.Query(
		fmt.Sprintf(
			`SELECT COALESCE(md5(string_agg(format('%%s:%%s', id, value), ',' ORDER BY id)), '')
			FROM %s`,
			table,
		),
	)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	if !rows.Next() {
		return "", fmt.Errorf("no rows returned")
	}
	var fingerprint string
	if err := rows.Scan(&fingerprint); err != nil {
		return "", err
	}
	return fingerprint, rows.Err()
}

func tableRowCount(q Querier, table string) (int, error) {
	if !sqlIdentRe.MatchString(table) {
		return 0, fmt.Errorf("invalid table identifier %q", table)
	}
	rows, err := q.Query(fmt.Sprintf("SELECT count(*) FROM %s", table))
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, fmt.Errorf("no rows returned")
	}
	var count int
	if err := rows.Scan(&count); err != nil {
		return 0, err
	}
	return count, rows.Err()
}

func waitTableRowCount(q Querier, table string, expected int, timeout time.Duration) error {
	start := time.Now()
	lastCount := -1
	var lastErr error
	for time.Now().Add(-timeout).Before(start) {
		count, err := tableRowCount(q, table)
		if err == nil {
			lastCount = count
			if count == expected {
				return nil
			}
		} else {
			lastErr = err
		}
		time.Sleep(2 * time.Second)
	}
	if lastErr != nil {
		return fmt.Errorf("timeout waiting for %d rows in %s (last error: %v)", expected, table, lastErr)
	}
	return fmt.Errorf("timeout waiting for %d rows in %s, got: %d", expected, table, lastCount)
}

func assertTableFingerprintEqual(master Querier, standby Querier, table string) error {
	masterFP, err := tableFingerprint(master, table)
	if err != nil {
		return fmt.Errorf("read %s fingerprint from master: %w", table, err)
	}
	standbyFP, err := tableFingerprint(standby, table)
	if err != nil {
		return fmt.Errorf("read %s fingerprint from standby: %w", table, err)
	}
	if masterFP != standbyFP {
		return fmt.Errorf(
			"data integrity mismatch in %s: standby fingerprint %q differs from master %q",
			table,
			standbyFP,
			masterFP,
		)
	}
	return nil
}

func waitTableFingerprintEqual(master Querier, standby Querier, table string, timeout time.Duration) error {
	start := time.Now()
	var lastErr error
	for time.Now().Add(-timeout).Before(start) {
		if err := assertTableFingerprintEqual(master, standby, table); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timeout waiting for %s fingerprint equality: %v", table, lastErr)
}

type tableIntegritySnapshot struct {
	RowCount    int
	Fingerprint string
}

func captureTableIntegritySnapshot(q Querier, table string) (tableIntegritySnapshot, error) {
	count, err := tableRowCount(q, table)
	if err != nil {
		return tableIntegritySnapshot{}, fmt.Errorf("read %s row count: %w", table, err)
	}
	fingerprint, err := tableFingerprint(q, table)
	if err != nil {
		return tableIntegritySnapshot{}, fmt.Errorf("read %s fingerprint: %w", table, err)
	}
	return tableIntegritySnapshot{
		RowCount:    count,
		Fingerprint: fingerprint,
	}, nil
}

func assertTableIntegritySnapshotEqual(got tableIntegritySnapshot, want tableIntegritySnapshot, table string) error {
	if got.RowCount != want.RowCount {
		return fmt.Errorf(
			"%s row count mismatch: got %d, want %d",
			table,
			got.RowCount,
			want.RowCount,
		)
	}
	if got.Fingerprint != want.Fingerprint {
		return fmt.Errorf(
			"%s fingerprint mismatch: got %q, want %q",
			table,
			got.Fingerprint,
			want.Fingerprint,
		)
	}
	return nil
}

func formatTableIntegritySnapshot(snapshot tableIntegritySnapshot) string {
	return fmt.Sprintf(
		"rows=%d fingerprint=%s",
		snapshot.RowCount,
		snapshot.Fingerprint,
	)
}

func formatTableSnapshotDiff(got tableIntegritySnapshot, want tableIntegritySnapshot) string {
	parts := make([]string, 0, 2)
	if got.RowCount != want.RowCount {
		parts = append(parts, fmt.Sprintf("rows got=%d want=%d", got.RowCount, want.RowCount))
	}
	if got.Fingerprint != want.Fingerprint {
		parts = append(parts, fmt.Sprintf("fingerprint got=%q want=%q", got.Fingerprint, want.Fingerprint))
	}
	if len(parts) == 0 {
		return "no diff"
	}
	return strings.Join(parts, ", ")
}
