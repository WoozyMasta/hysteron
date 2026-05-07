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
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/woozymasta/hysteron/internal/common"

	"github.com/jackc/pgx/v5/pgtype"

	"os"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// WalSegSize is the assumed WAL segment size in bytes.
	WalSegSize = (16 * 1024 * 1024) // 16MiB
)

var (
	supportedMajorVersions = []int{14, 15, 16, 17, 18}

	// ValidReplSlotName validates PostgreSQL replication slot names.
	ValidReplSlotName           = regexp.MustCompile("^[a-z0-9_]+$")
	timelineHistoryLineRegexp   = regexp.MustCompile(`(\S+)\s+(\S+)\s+(.*)$`)
	postgresBinaryVersionRegexp = regexp.MustCompile(`.* \(PostgreSQL\) ([0-9\.]+).*`)
)

func dbExec(ctx context.Context, db *sql.DB, query string) (sql.Result, error) {
	return db.ExecContext(ctx, query)
}

func query(ctx context.Context, db *sql.DB, query string) (*sql.Rows, error) {
	return db.QueryContext(ctx, query)
}

func openDB(connParams ConnParams) (*sql.DB, error) {
	return sql.Open(SQLDriverName, connParams.ConnString())
}

func execReplication(ctx context.Context, connParams ConnParams, command string) ([][][]byte, error) {
	conn, err := pgconn.Connect(ctx, replicationConnParams(connParams).ConnString())
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(context.Background()) }()

	results, err := conn.Exec(ctx, command).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, errors.New("query returned 0 results")
	}
	if results[0].Err != nil {
		return nil, results[0].Err
	}
	return results[0].Rows, nil
}

func replicationConnParams(connParams ConnParams) ConnParams {
	replConnParams := connParams.Copy()
	replConnParams["replication"] = "1"
	return replConnParams
}

func stringArrayParam(values []string) pgtype.FlatArray[string] {
	return pgtype.FlatArray[string](values)
}

func ping(ctx context.Context, connParams ConnParams) error {
	db, err := openDB(connParams)
	if err != nil {
		return err
	}
	defer ignoreClose(db)

	_, err = dbExec(ctx, db, "select 1")
	if err != nil {
		return err
	}
	return nil
}

func setPassword(ctx context.Context, connParams ConnParams, username, password string) error {
	db, err := openDB(connParams)
	if err != nil {
		return err
	}
	defer ignoreClose(db)

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	query := fmt.Sprintf("set local log_statement = %s", quoteLiteral("none")) //nolint:perfsprint
	if _, err = tx.ExecContext(ctx, query); err != nil {
		_ = tx.Rollback()
		return err
	}

	query = fmt.Sprintf("alter role %s with encrypted password %s", quoteIdentifier(username), quoteLiteral(password))
	if _, err = tx.ExecContext(ctx, query); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func createRole(ctx context.Context, connParams ConnParams, username, password string) error {
	db, err := openDB(connParams)
	if err != nil {
		return err
	}
	defer ignoreClose(db)

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	query := fmt.Sprintf("set local log_statement = %s", quoteLiteral("none")) //nolint:perfsprint
	if _, err = tx.ExecContext(ctx, query); err != nil {
		_ = tx.Rollback()
		return err
	}

	query = fmt.Sprintf("create role %s with login replication encrypted password %s", quoteIdentifier(username), quoteLiteral(password))
	if _, err = tx.ExecContext(ctx, query); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func createPasswordlessRole(ctx context.Context, connParams ConnParams, username string) error {
	db, err := openDB(connParams)
	if err != nil {
		return err
	}
	defer ignoreClose(db)

	_, err = dbExec(ctx, db, fmt.Sprintf("create role %s with login replication", quoteIdentifier(username)))
	return err
}

func alterRole(ctx context.Context, connParams ConnParams, username, password string) error {
	db, err := openDB(connParams)
	if err != nil {
		return err
	}
	defer ignoreClose(db)

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	query := fmt.Sprintf("set local log_statement = %s", quoteLiteral("none")) //nolint:perfsprint
	if _, err = tx.ExecContext(ctx, query); err != nil {
		_ = tx.Rollback()
		return err
	}

	query = fmt.Sprintf("alter role %s with login replication encrypted password %s", quoteIdentifier(username), quoteLiteral(password))
	if _, err = tx.ExecContext(ctx, query); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func alterPasswordlessRole(ctx context.Context, connParams ConnParams, username string) error {
	db, err := openDB(connParams)
	if err != nil {
		return err
	}
	defer ignoreClose(db)

	_, err = dbExec(ctx, db, fmt.Sprintf("alter role %s with login replication", quoteIdentifier(username)))
	return err
}

// getReplicatinSlots return existing replication slots. On PostgreSQL > 10 we
// skip temporary slots.
func getReplicationSlots(ctx context.Context, connParams ConnParams, maj int) ([]string, error) {
	var q string
	if maj < 10 {
		q = "select slot_name from pg_replication_slots"
	} else {
		q = "select slot_name from pg_replication_slots where temporary is false"
	}

	db, err := openDB(connParams)
	if err != nil {
		return nil, err
	}
	defer ignoreClose(db)

	replSlots := []string{}

	rows, err := query(ctx, db, q)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var slotName string
		if err := rows.Scan(&slotName); err != nil {
			return nil, err
		}
		replSlots = append(replSlots, slotName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return replSlots, nil
}

func createReplicationSlot(ctx context.Context, connParams ConnParams, name string) error {
	db, err := openDB(connParams)
	if err != nil {
		return err
	}
	defer ignoreClose(db)

	_, err = dbExec(ctx, db, fmt.Sprintf("select pg_create_physical_replication_slot(%s)", quoteLiteral(name)))
	return err
}

func dropReplicationSlot(ctx context.Context, connParams ConnParams, name string) error {
	db, err := openDB(connParams)
	if err != nil {
		return err
	}
	defer ignoreClose(db)

	_, err = dbExec(ctx, db, fmt.Sprintf("select pg_drop_replication_slot(%s)", quoteLiteral(name)))
	return err
}

func getSyncStandbys(ctx context.Context, connParams ConnParams) ([]string, error) {
	db, err := openDB(connParams)
	if err != nil {
		return nil, err
	}
	defer ignoreClose(db)

	rows, err := query(ctx, db, "select application_name, sync_state from pg_stat_replication")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	syncStandbys := []string{}
	for rows.Next() {
		var applicationName, syncState string
		if err := rows.Scan(&applicationName, &syncState); err != nil {
			return nil, err
		}

		if syncState == "sync" {
			syncStandbys = append(syncStandbys, applicationName)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return syncStandbys, nil
}

// PGLsnToInt converts a PostgreSQL LSN string into an integer value.
func PGLsnToInt(lsn string) (uint64, error) {
	parts := strings.Split(lsn, "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("bad pg_lsn: %s", lsn)
	}
	a, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, err
	}
	b, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, err
	}
	v := a<<32 | b
	return v, nil
}

// GetSystemData returns system identifier and WAL position from IDENTIFY_SYSTEM.
func GetSystemData(ctx context.Context, replConnParams ConnParams) (*SystemData, error) {
	rows, err := execReplication(ctx, replConnParams, "IDENTIFY_SYSTEM")
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errors.New("query returned 0 rows")
	}
	if len(rows[0]) < 3 {
		return nil, fmt.Errorf("IDENTIFY_SYSTEM returned %d columns, wanted at least 3", len(rows[0]))
	}

	timelineID, err := strconv.ParseUint(string(rows[0][1]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("cannot parse timelineID %q: %w", rows[0][1], err)
	}
	xLogPos, err := PGLsnToInt(string(rows[0][2]))
	if err != nil {
		return nil, err
	}

	return &SystemData{
		SystemID:   string(rows[0][0]),
		TimelineID: timelineID,
		XLogPos:    xLogPos,
	}, nil
}

func parseTimelinesHistory(contents string) ([]*TimelineHistory, error) {
	tlsh := []*TimelineHistory{}

	scanner := bufio.NewScanner(strings.NewReader(contents))
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		m := timelineHistoryLineRegexp.FindStringSubmatch(scanner.Text())
		if len(m) == 4 {
			var tlh TimelineHistory
			var err error
			if tlh.TimelineID, err = strconv.ParseUint(m[1], 10, 64); err != nil {
				return nil, fmt.Errorf("cannot parse timelineID in timeline history line %q: %v", scanner.Text(), err)
			}
			if tlh.SwitchPoint, err = PGLsnToInt(m[2]); err != nil {
				return nil, fmt.Errorf("cannot parse start lsn in timeline history line %q: %v", scanner.Text(), err)
			}
			tlh.Reason = m[3]
			tlsh = append(tlsh, &tlh)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return tlsh, nil
}

func getTimelinesHistory(ctx context.Context, timeline uint64, replConnParams ConnParams) ([]*TimelineHistory, error) {
	rows, err := execReplication(ctx, replConnParams, fmt.Sprintf("TIMELINE_HISTORY %d", timeline))
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errors.New("query returned 0 rows")
	}
	if len(rows[0]) < 2 {
		return nil, fmt.Errorf("TIMELINE_HISTORY returned %d columns, wanted at least 2", len(rows[0]))
	}
	return parseTimelinesHistory(string(rows[0][1]))
}

// IsValidReplSlotName reports whether name is a valid replication slot name.
func IsValidReplSlotName(name string) bool {
	return ValidReplSlotName.MatchString(name)
}

func fileExists(path string) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func expand(s, dataDir string) string {
	buf := make([]byte, 0, 2*len(s))
	// %d %% are all ASCII, so bytes are fine for this operation.
	i := 0
	for j := 0; j < len(s); j++ {
		if s[j] == '%' && j+1 < len(s) {
			switch s[j+1] {
			case 'd':
				buf = append(buf, s[i:j]...)
				buf = append(buf, []byte(dataDir)...)
				j++
				i = j + 1

			case '%':
				j++
				buf = append(buf, s[i:j]...)
				i = j + 1

			default:
			}
		}
	}
	return string(buf) + s[i:]
}

func getConfigFilePGParameters(ctx context.Context, connParams ConnParams) (common.Parameters, error) {
	var pgParameters = common.Parameters{}
	db, err := openDB(connParams)
	if err != nil {
		return nil, err
	}
	defer ignoreClose(db)

	// We prefer pg_file_settings since pg_settings returns archive_command = '(disabled)' when archive_mode is off so we'll lose its value
	// Check if pg_file_settings exists (pg >= 9.5)
	usePGFileSettings, err := hasPGFileSettings(ctx, db)
	if err != nil {
		return nil, err
	}

	if usePGFileSettings {
		// NOTE If some pg_parameters that cannot be changed without a restart
		// are removed from the postgresql.conf file the view will contain some
		// rows with null name and setting and the error field set to the cause.
		// So we have to filter out these or the Scan will fail.
		rows, err := query(ctx, db, "select name, setting from pg_file_settings where name IS NOT NULL and setting IS NOT NULL")
		if err != nil {
			return nil, err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var name, setting string
			if err = rows.Scan(&name, &setting); err != nil {
				return nil, err
			}
			pgParameters[name] = setting
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return pgParameters, nil
	}

	// Fallback to pg_settings
	rows, err := query(ctx, db, "select name, setting, source from pg_settings")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name, setting, source string
		if err = rows.Scan(&name, &setting, &source); err != nil {
			return nil, err
		}
		if source == "configuration file" {
			pgParameters[name] = setting
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return pgParameters, nil
}

func hasPGFileSettings(ctx context.Context, db *sql.DB) (bool, error) {
	rows, err := query(ctx, db, "select 1 from information_schema.tables where table_schema = 'pg_catalog' and table_name = 'pg_file_settings'")
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		return true, nil
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func isRestartRequiredUsingPendingRestart(ctx context.Context, connParams ConnParams) (bool, error) {
	isRestartRequired := false
	db, err := openDB(connParams)
	if err != nil {
		return isRestartRequired, err
	}
	defer ignoreClose(db)

	rows, err := query(ctx, db, "select count(*) > 0 from pg_settings where pending_restart;")
	if err != nil {
		return isRestartRequired, err
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		if err := rows.Scan(&isRestartRequired); err != nil {
			return isRestartRequired, err
		}
	}
	if err := rows.Err(); err != nil {
		return isRestartRequired, err
	}

	return isRestartRequired, nil
}

func isRestartRequiredUsingPgSettingsContext(ctx context.Context, connParams ConnParams, changedParams []string) (bool, error) {
	isRestartRequired := false
	db, err := openDB(connParams)
	if err != nil {
		return isRestartRequired, err
	}
	defer ignoreClose(db)

	stmt, err := db.PrepareContext(ctx, "select count(*) > 0 from pg_settings where context = 'postmaster' and name = ANY($1)")

	if err != nil {
		return false, err
	}
	defer func() { _ = stmt.Close() }()

	rows, err := stmt.QueryContext(ctx, stringArrayParam(changedParams))
	if err != nil {
		return isRestartRequired, err
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		if err := rows.Scan(&isRestartRequired); err != nil {
			return isRestartRequired, err
		}
	}
	if err := rows.Err(); err != nil {
		return isRestartRequired, err
	}

	return isRestartRequired, nil
}

// ParseBinaryVersion parses postgres --version output.
func ParseBinaryVersion(v string) (int, int, error) {
	// extract version (removing beta*, rc* etc...)
	m := postgresBinaryVersionRegexp.FindStringSubmatch(v)
	if len(m) != 2 {
		return 0, 0, fmt.Errorf("failed to parse postgres binary version: %q", v)
	}
	return ParseVersion(m[1])
}

// ParseVersion parses a PostgreSQL major.minor version string.
func ParseVersion(v string) (int, int, error) {
	parts := strings.Split(v, ".")
	if len(parts) < 1 {
		return 0, 0, fmt.Errorf("bad version: %q", v)
	}
	maj, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse major %q: %v", parts[0], err)
	}
	minor := 0
	if len(parts) > 1 {
		minor, err = strconv.Atoi(parts[1])
		if err != nil {
			return 0, 0, fmt.Errorf("failed to parse minor %q: %v", parts[1], err)
		}
	}

	return maj, minor, nil
}

// IsSupportedMajorVersion reports whether major has default support.
func IsSupportedMajorVersion(major int) bool {
	return slices.Contains(supportedMajorVersions, major)
}

// SupportedMajorVersions returns PostgreSQL major versions with default support.
func SupportedMajorVersions() []int {
	return slices.Clone(supportedMajorVersions)
}

// ValidateSupportedMajorVersion checks whether major has default support.
func ValidateSupportedMajorVersion(major int) error {
	if IsSupportedMajorVersion(major) {
		return nil
	}
	return fmt.Errorf("unsupported PostgreSQL major version %d; supported major versions are %s", major, SupportedMajorVersionsString())
}

// SupportedMajorVersionsString returns the supported major versions for messages.
func SupportedMajorVersionsString() string {
	versions := make([]string, 0, len(supportedMajorVersions))
	for _, version := range supportedMajorVersions {
		versions = append(versions, strconv.Itoa(version))
	}
	return strings.Join(versions, ", ")
}

// IsWalFileName reports whether name matches a WAL segment filename.
func IsWalFileName(name string) bool {
	walChars := "0123456789ABCDEF"
	if len(name) != 24 {
		return false
	}
	for _, c := range name {
		ok := false
		for _, v := range walChars {
			if c == v {
				ok = true
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

// XlogPosToWalFileNameNoTimeline converts an LSN to WAL file suffix without timeline.
func XlogPosToWalFileNameNoTimeline(xLogPos uint64) string {
	id := uint32(xLogPos >> 32)
	// The WAL segment offset is defined by the lower 32 bits of the LSN.
	offset := uint32(xLogPos) //nolint:gosec
	// TODO(sgotti) for now we assume wal size is the default 16M size
	seg := offset / WalSegSize
	return fmt.Sprintf("%08X%08X", id, seg)
}

// WalFileNameNoTimeLine strips timeline prefix from a WAL filename.
func WalFileNameNoTimeLine(name string) (string, error) {
	if !IsWalFileName(name) {
		return "", errors.New("bad wal file name")
	}
	return name[8:24], nil
}
