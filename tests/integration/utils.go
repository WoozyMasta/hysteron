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

//go:build integration

package integration

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sorintlab/stolon/internal/cluster"
	"github.com/sorintlab/stolon/internal/common"
	pg "github.com/sorintlab/stolon/internal/postgresql"
	"github.com/sorintlab/stolon/internal/store"

	"github.com/google/uuid"
)

const (
	sleepInterval = 500 * time.Millisecond

	MinPort = 2048
	MaxPort = 16384
)

var (
	defaultPGParameters = cluster.PGParameters{"log_destination": "stderr", "logging_collector": "false"}

	defaultStoreTimeout = 1 * time.Second
)

var curPort = MinPort
var portMutex = sync.Mutex{}

func pgParametersWithDefaults(p cluster.PGParameters) cluster.PGParameters {
	pd := cluster.PGParameters{}
	for k, v := range defaultPGParameters {
		pd[k] = v
	}
	for k, v := range p {
		pd[k] = v
	}
	return pd
}

type Querier interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

type ReplQuerier interface {
	ReplConnParams() pg.ConnParams
}

func GetPGParameters(q Querier) (common.Parameters, error) {
	var pgParameters = common.Parameters{}
	rows, err := q.Query("select name, setting, source from pg_settings")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var name, setting, source string
		if err = rows.Scan(&name, &setting, &source); err != nil {
			return nil, err
		}
		if source == "configuration file" {
			pgParameters[name] = setting
		}
	}
	return pgParameters, nil
}

func GetSystemData(q ReplQuerier) (*pg.SystemData, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return pg.GetSystemData(ctx, q.ReplConnParams())
}

func GetXLogPos(q ReplQuerier) (uint64, error) {
	// get the current master XLogPos
	systemData, err := GetSystemData(q)
	if err != nil {
		return 0, err
	}
	return systemData.XLogPos, nil
}

// getReplicatinSlots return existing replication slots (also temporary
// replication slots on PostgreSQL > 10)
func getReplicationSlots(q Querier) ([]string, error) {
	replSlots := []string{}

	rows, err := q.Query("select slot_name from pg_replication_slots")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var slotName string
		if err := rows.Scan(&slotName); err != nil {
			return nil, err
		}
		replSlots = append(replSlots, slotName)
	}

	return replSlots, nil
}

/*
// currently unused, keep for future needs

func waitReplicationSlots(q Querier, replSlots []string, timeout time.Duration) error {
	sort.Strings(replSlots)

	start := time.Now()
	curReplSlots := []string{}
	var err error
	for time.Now().Add(-timeout).Before(start) {
		curReplSlots, err := getReplicationSlots(q)
		if err != nil {
			goto end
		}
		sort.Strings(curReplSlots)
		if reflect.DeepEqual(replSlots, curReplSlots) {
			return nil
		}
	end:
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timeout waiting for replSlots %v, got: %v, last err: %v", replSlots, curReplSlots, err)
}
*/

func waitStolonReplicationSlots(q Querier, replSlots []string, timeout time.Duration) error {
	// prefix with stolon_
	for i, slot := range replSlots {
		replSlots[i] = common.StolonName(slot)
	}
	sort.Strings(replSlots)

	start := time.Now()
	var curReplSlots []string
	var err error
	for time.Now().Add(-timeout).Before(start) {
		allReplSlots, err := getReplicationSlots(q)
		if err != nil {
			goto end
		}
		curReplSlots = []string{}
		for _, s := range allReplSlots {
			if common.IsStolonName(s) {
				curReplSlots = append(curReplSlots, s)
			}
		}
		sort.Strings(curReplSlots)
		if reflect.DeepEqual(replSlots, curReplSlots) {
			return nil
		}
	end:
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timeout waiting for replSlots %v, got: %v, last err: %v", replSlots, curReplSlots, err)
}

func waitNotStolonReplicationSlots(q Querier, replSlots []string, timeout time.Duration) error {
	sort.Strings(replSlots)

	start := time.Now()
	var curReplSlots []string
	var err error
	for time.Now().Add(-timeout).Before(start) {
		allReplSlots, err := getReplicationSlots(q)
		if err != nil {
			goto end
		}
		curReplSlots = []string{}
		for _, s := range allReplSlots {
			if !common.IsStolonName(s) {
				curReplSlots = append(curReplSlots, s)
			}
		}
		sort.Strings(curReplSlots)
		if reflect.DeepEqual(replSlots, curReplSlots) {
			return nil
		}
	end:
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timeout waiting for replSlots %v, got: %v, last err: %v", replSlots, curReplSlots, err)
}

type Process struct {
	t    *testing.T
	uid  string
	name string
	args []string
	cmd  *managedProcess
	bin  string
}

type managedProcess struct {
	Cmd *exec.Cmd

	done chan error
}

func newManagedProcess(cmd *exec.Cmd) *managedProcess {
	return &managedProcess{
		Cmd:  cmd,
		done: make(chan error, 1),
	}
}

func (p *managedProcess) Start(output io.WriteCloser) error {
	pr, pw, err := os.Pipe()
	if err != nil {
		return err
	}
	p.Cmd.Stdout = pw
	p.Cmd.Stderr = pw

	if err := p.Cmd.Start(); err != nil {
		_ = pr.Close()
		_ = pw.Close()
		_ = output.Close()
		return err
	}
	_ = pw.Close()

	go p.captureOutput(pr, output)
	go func() {
		p.done <- p.Cmd.Wait()
	}()
	return nil
}

func (p *managedProcess) captureOutput(r io.ReadCloser, output io.Writer) {
	defer r.Close()
	if closer, ok := output.(io.Closer); ok {
		defer closer.Close()
	}

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		_, _ = fmt.Fprintln(output, scanner.Text())
	}
}

func (p *managedProcess) Wait() error {
	err := <-p.done
	p.done <- err
	return err
}

func (p *Process) Start() error {
	if p.cmd != nil {
		panic(fmt.Errorf("%s: cmd not cleanly stopped", p.uid))
	}
	cmd := exec.Command(p.bin, p.args...)
	pr, pw, err := os.Pipe()
	if err != nil {
		return err
	}
	p.cmd = newManagedProcess(cmd)
	if err := p.cmd.Start(pw); err != nil {
		return err
	}
	go func() {
		defer pr.Close()
		scanner := bufio.NewScanner(pr)
		for scanner.Scan() {
			p.t.Logf("[%s %s]: %s", p.name, p.uid, scanner.Text())
		}
	}()

	return nil
}

func (p *Process) Signal(sig os.Signal) error {
	p.t.Logf("signalling %s %s with %s", p.name, p.uid, sig)
	if p.cmd == nil {
		panic(fmt.Errorf("p: %s, cmd is empty", p.uid))
	}
	return p.cmd.Cmd.Process.Signal(sig)
}

func (p *Process) Freeze() error {
	sig, ok := freezeSignal()
	if !ok {
		p.t.Skip("process freezing is not supported on this platform")
	}
	p.t.Logf("freezing %s %s", p.name, p.uid)
	return p.Signal(sig)
}

func (p *Process) Resume() error {
	sig, ok := resumeSignal()
	if !ok {
		p.t.Skip("process resuming is not supported on this platform")
	}
	p.t.Logf("resuming %s %s", p.name, p.uid)
	return p.Signal(sig)
}

func (p *Process) Kill() {
	p.t.Logf("killing %s %s", p.name, p.uid)
	if p.cmd == nil {
		panic(fmt.Errorf("p: %s, cmd is empty", p.uid))
	}
	_ = p.cmd.Cmd.Process.Signal(os.Kill)
	_ = p.cmd.Wait()
	p.cmd = nil
}

func (p *Process) Stop() {
	p.t.Logf("stopping %s %s", p.name, p.uid)
	if p.cmd == nil {
		panic(fmt.Errorf("p: %s, cmd is empty", p.uid))
	}
	_ = p.cmd.Cmd.Process.Signal(os.Interrupt)
	_ = p.cmd.Wait()
	p.cmd = nil
}

func (p *Process) Wait(timeout time.Duration) error {
	timeoutCh := time.NewTimer(timeout).C
	endCh := make(chan error)
	go func() {
		err := p.cmd.Wait()
		endCh <- err
	}()
	select {
	case <-timeoutCh:
		return fmt.Errorf("timeout waiting on process")
	case err := <-endCh:
		return err
	}
}

type TestKeeper struct {
	t *testing.T
	Process
	dataDir         string
	pgListenAddress string
	pgPort          string
	pgSUUsername    string
	pgSUPassword    string
	pgReplUsername  string
	pgReplPassword  string
	replConnParams  pg.ConnParams
	db              *sql.DB
}

func NewTestKeeperWithID(t *testing.T, dir, uid, clusterName, pgSUUsername, pgSUPassword, pgReplUsername, pgReplPassword string, storeBackend store.Backend, storeEndpoints string, a ...string) (*TestKeeper, error) {
	args := []string{}

	dataDir := filepath.Join(dir, fmt.Sprintf("st%s", uid))

	pgListenAddress, pgPort, err := getFreePort(true, false)
	if err != nil {
		return nil, err
	}

	args = append(args, fmt.Sprintf("--uid=%s", uid))
	args = append(args, fmt.Sprintf("--cluster-name=%s", clusterName))
	args = append(args, fmt.Sprintf("--pg-listen-address=%s", pgListenAddress))
	args = append(args, fmt.Sprintf("--pg-port=%s", pgPort))
	args = append(args, fmt.Sprintf("--data-dir=%s", dataDir))
	args = append(args, fmt.Sprintf("--store-backend=%s", storeBackend))
	args = append(args, fmt.Sprintf("--store-endpoints=%s", storeEndpoints))
	args = append(args, fmt.Sprintf("--pg-su-username=%s", pgSUUsername))
	if pgSUPassword != "" {
		args = append(args, fmt.Sprintf("--pg-su-password=%s", pgSUPassword))
	}
	args = append(args, fmt.Sprintf("--pg-repl-username=%s", pgReplUsername))
	args = append(args, fmt.Sprintf("--pg-repl-password=%s", pgReplPassword))
	if os.Getenv("DEBUG") != "" {
		args = append(args, "--debug")
	}
	args = append(args, a...)

	connParams := pg.ConnParams{
		"user":     pgSUUsername,
		"password": pgSUPassword,
		"host":     pgListenAddress,
		"port":     pgPort,
		"dbname":   "postgres",
		"sslmode":  "disable",
	}

	replConnParams := pg.ConnParams{
		"user":        pgReplUsername,
		"password":    pgReplPassword,
		"host":        pgListenAddress,
		"port":        pgPort,
		"dbname":      "postgres",
		"sslmode":     "disable",
		"replication": "1",
	}

	connString := connParams.ConnString()
	db, err := sql.Open(pg.SQLDriverName, connString)
	if err != nil {
		return nil, err
	}

	bin := os.Getenv("STKEEPER_BIN")
	if bin == "" {
		return nil, fmt.Errorf("missing STKEEPER_BIN env")
	}
	tk := &TestKeeper{
		t: t,
		Process: Process{
			t:    t,
			uid:  uid,
			name: "keeper",
			bin:  bin,
			args: args,
		},
		dataDir:         dataDir,
		pgListenAddress: pgListenAddress,
		pgPort:          pgPort,
		pgSUUsername:    pgSUUsername,
		pgSUPassword:    pgSUPassword,
		pgReplUsername:  pgReplUsername,
		pgReplPassword:  pgReplPassword,
		replConnParams:  replConnParams,
		db:              db,
	}
	return tk, nil
}

func NewTestKeeper(t *testing.T, dir, clusterName, pgSUUsername, pgSUPassword, pgReplUsername, pgReplPassword string, storeBackend store.Backend, storeEndpoints string, a ...string) (*TestKeeper, error) {
	u := uuid.New()
	uid := fmt.Sprintf("%x", u[:4])

	return NewTestKeeperWithID(t, dir, uid, clusterName, pgSUUsername, pgSUPassword, pgReplUsername, pgReplPassword, storeBackend, storeEndpoints, a...)
}

func (tk *TestKeeper) PGDataVersion() (int, int, error) {
	fh, err := os.Open(filepath.Join(tk.dataDir, "postgres", "PG_VERSION"))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read PG_VERSION: %v", err)
	}
	defer fh.Close()

	scanner := bufio.NewScanner(fh)
	scanner.Split(bufio.ScanLines)

	scanner.Scan()

	version := scanner.Text()
	return pg.ParseVersion(version)
}

func (tk *TestKeeper) Exec(query string, args ...interface{}) (sql.Result, error) {
	res, err := tk.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (tk *TestKeeper) openDB(username, password string) (*sql.DB, error) {
	connParams := pg.ConnParams{
		"user":     username,
		"password": password,
		"host":     tk.pgListenAddress,
		"port":     tk.pgPort,
		"dbname":   "postgres",
		"sslmode":  "disable",
	}
	return sql.Open(pg.SQLDriverName, connParams.ConnString())
}

func (tk *TestKeeper) waitDBUpWithCredentials(username, password string, timeout time.Duration) error {
	db, err := tk.openDB(username, password)
	if err != nil {
		return err
	}
	defer db.Close()

	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		if _, err := db.Exec("select 1"); err == nil {
			return nil
		}
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout")
}

func (tk *TestKeeper) expectConnect(username, password string) error {
	db, err := tk.openDB(username, password)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec("select 1")
	return err
}

func (tk *TestKeeper) roleAttributes(role string) (replication bool, superuser bool, err error) {
	rows, err := tk.Query(
		"select rolreplication, rolsuper from pg_roles where rolname = $1",
		role,
	)
	if err != nil {
		return false, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return false, false, fmt.Errorf("role %q not found", role)
	}
	if err := rows.Scan(&replication, &superuser); err != nil {
		return false, false, err
	}
	return replication, superuser, rows.Err()
}

func (tk *TestKeeper) waitRoleAttributes(role string, wantReplication, wantSuperuser bool, timeout time.Duration) error {
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		replication, superuser, err := tk.roleAttributes(role)
		if err == nil && replication == wantReplication && superuser == wantSuperuser {
			return nil
		}
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf(
		"timeout waiting for role %q attributes replication=%t superuser=%t",
		role,
		wantReplication,
		wantSuperuser,
	)
}

func (tk *TestKeeper) waitRoleSuperuser(role string, timeout time.Duration) error {
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		_, superuser, err := tk.roleAttributes(role)
		if err == nil && superuser {
			return nil
		}
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout waiting for role %q to be a superuser", role)
}

func (tk *TestKeeper) waitRoleReplication(role string, timeout time.Duration) error {
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		replication, _, err := tk.roleAttributes(role)
		if err == nil && replication {
			return nil
		}
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout waiting for role %q to have replication privileges", role)
}

func (tk *TestKeeper) Query(query string, args ...interface{}) (*sql.Rows, error) {
	res, err := tk.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (tk *TestKeeper) ReplConnParams() pg.ConnParams {
	return tk.replConnParams.Copy()
}

func (tk *TestKeeper) SwitchWals(times int) error {
	maj, _, err := tk.PGDataVersion()
	if err != nil {
		return err
	}
	var switchLogFunc string
	if maj < 10 {
		switchLogFunc = "select pg_switch_xlog()"
	} else {
		switchLogFunc = "select pg_switch_wal()"
	}

	_, _ = tk.Exec("DROP TABLE switchwal")
	if _, err := tk.Exec("CREATE TABLE switchwal(ID INT PRIMARY KEY NOT NULL)"); err != nil {
		return err
	}
	// if times > 1 we have to do some transactions or the wal won't switch
	for i := 0; i < times; i++ {
		if _, err := tk.Exec("INSERT INTO switchwal VALUES ($1)", i); err != nil {
			return err
		}
		if _, err := tk.db.Exec(switchLogFunc); err != nil {
			return err
		}
	}
	_, _ = tk.Exec("DROP TABLE switchwal")
	return nil
}

func (tk *TestKeeper) CheckPoint() error {
	_, err := tk.Exec("CHECKPOINT")
	return err
}

func (tk *TestKeeper) WaitDBUp(timeout time.Duration) error {
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		_, err := tk.Exec("select 1")
		if err == nil {
			return nil
		}
		tk.t.Logf("tk: %v, error: %v", tk.uid, err)
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout")
}

func (tk *TestKeeper) WaitDBDown(timeout time.Duration) error {
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		_, err := tk.Exec("select 1")
		if err != nil {
			return nil
		}
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout")
}

func (tk *TestKeeper) GetPGProcess() (*os.Process, error) {
	fh, err := os.Open(filepath.Join(tk.dataDir, "postgres/postmaster.pid"))
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	scanner := bufio.NewScanner(fh)
	scanner.Split(bufio.ScanLines)
	if !scanner.Scan() {
		return nil, fmt.Errorf("not enough lines in pid file")
	}
	pidStr := scanner.Text()
	pid, err := strconv.Atoi(string(pidStr))
	if err != nil {
		return nil, err
	}
	return os.FindProcess(pid)
}

func (tk *TestKeeper) SignalPG(sig os.Signal) error {
	p, err := tk.GetPGProcess()
	if err != nil {
		return err
	}
	return p.Signal(sig)
}

func (tk *TestKeeper) FreezePG() error {
	sig, ok := freezeSignal()
	if !ok {
		tk.t.Skip("PostgreSQL process freezing is not supported on this platform")
	}
	tk.t.Logf("freezing postgres for keeper %s", tk.uid)
	return tk.SignalPG(sig)
}

func (tk *TestKeeper) ResumePG() error {
	sig, ok := resumeSignal()
	if !ok {
		tk.t.Skip("PostgreSQL process resuming is not supported on this platform")
	}
	tk.t.Logf("resuming postgres for keeper %s", tk.uid)
	return tk.SignalPG(sig)
}

func (tk *TestKeeper) isInRecovery() (bool, error) {
	rows, err := tk.Query("SELECT pg_is_in_recovery from pg_is_in_recovery()")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	if rows.Next() {
		var isInRecovery bool
		if err := rows.Scan(&isInRecovery); err != nil {
			return false, err
		}
		if isInRecovery {
			return true, nil
		}
		return false, nil
	}
	return false, fmt.Errorf("no rows returned")
}

func (tk *TestKeeper) primaryConninfo() (pg.ConnParams, error) {
	rows, err := tk.Query("SELECT setting FROM pg_settings WHERE name = 'primary_conninfo'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("primary_conninfo setting not found")
	}

	var setting string
	if err := rows.Scan(&setting); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if setting == "" {
		return nil, nil
	}
	return pg.ParseConnString(setting)
}

func (tk *TestKeeper) WaitDBRole(r common.Role, ptk *TestKeeper, timeout time.Duration) error {
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		time.Sleep(sleepInterval)
		// when the cluster is in standby mode also the master db is a standby
		// so we cannot just check if the keeper is in recovery but have to
		// check if the primary_conninfo points to the primary db or to the
		// cluster master
		if ptk == nil {
			ok, err := tk.isInRecovery()
			if err != nil {
				continue
			}
			if !ok && r == common.RoleMaster {
				return nil
			}
			if ok && r == common.RoleStandby {
				return nil
			}
		} else {
			ok, err := tk.isInRecovery()
			if err != nil {
				continue
			}
			if !ok {
				continue
			}
			conninfo, err := tk.primaryConninfo()
			if err != nil {
				continue
			}
			if conninfo["host"] == ptk.pgListenAddress && conninfo["port"] == ptk.pgPort {
				if r == common.RoleMaster {
					return nil
				}
			} else {
				if r == common.RoleStandby {
					return nil
				}
			}
		}
	}

	return fmt.Errorf("timeout")
}

func (tk *TestKeeper) WaitPGParameter(parameter, value string, timeout time.Duration) error {
	latestValue := ""
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		pgParameters, err := GetPGParameters(tk)
		if err != nil {
			goto end
		}
		latestValue = pgParameters[parameter]
		if latestValue == value {
			return nil
		}
	end:
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout waiting for pgParamater %q (%q) to equal %q", parameter, latestValue, value)
}

func (tk *TestKeeper) GetPGParameters() (common.Parameters, error) {
	return GetPGParameters(tk)
}

func (tk *TestKeeper) waitPostgresConfParam(parameter, value string, timeout time.Duration) error {
	expected := fmt.Sprintf("%s = '%s'", parameter, value)
	path := filepath.Join(tk.dataDir, "postgres", "postgresql.conf")
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		data, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(data), expected) {
			return nil
		}
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout waiting for %s in %s", expected, path)
}

/*
// currently unused, keep for future needs

type CheckFunc func(time.Duration) error

func waitChecks(timeout time.Duration, fns ...CheckFunc) error {
	end := make(chan error)
	fnc := len(fns)
	for _, fn := range fns {
		go func(fn CheckFunc, end chan error) {
			end <- fn(timeout)
		}(fn, end)
	}

	c := 0
	for c < fnc {
		err := <-end
		if err != nil {
			return err
		}
		c++
	}
	return nil
}
*/

type TestSentinel struct {
	t *testing.T
	Process
}

func NewTestSentinel(t *testing.T, dir string, clusterName string, storeBackend store.Backend, storeEndpoints string, a ...string) (*TestSentinel, error) {
	u := uuid.New()
	uid := fmt.Sprintf("%x", u[:4])

	args := []string{}
	args = append(args, fmt.Sprintf("--cluster-name=%s", clusterName))
	args = append(args, fmt.Sprintf("--store-backend=%s", storeBackend))
	args = append(args, fmt.Sprintf("--store-endpoints=%s", storeEndpoints))
	if os.Getenv("DEBUG") != "" {
		args = append(args, "--debug")
	}
	args = append(args, a...)

	bin := os.Getenv("STSENTINEL_BIN")
	if bin == "" {
		return nil, fmt.Errorf("missing STSENTINEL_BIN env")
	}
	ts := &TestSentinel{
		t: t,
		Process: Process{
			t:    t,
			uid:  uid,
			name: "sentinel",
			bin:  bin,
			args: args,
		},
	}
	return ts, nil
}

type TestProxy struct {
	t *testing.T
	Process
	listenAddress  string
	port           string
	replConnParams pg.ConnParams
	db             *sql.DB
}

func NewTestProxy(t *testing.T, dir string, clusterName, pgSUUsername, pgSUPassword, pgReplUsername, pgReplPassword string, storeBackend store.Backend, storeEndpoints string, a ...string) (*TestProxy, error) {
	u := uuid.New()
	uid := fmt.Sprintf("%x", u[:4])

	listenAddress, port, err := getFreePort(true, false)
	if err != nil {
		return nil, err
	}

	args := []string{}
	args = append(args, fmt.Sprintf("--cluster-name=%s", clusterName))
	args = append(args, fmt.Sprintf("--listen-address=%s", listenAddress))
	args = append(args, fmt.Sprintf("--port=%s", port))
	args = append(args, fmt.Sprintf("--store-backend=%s", storeBackend))
	args = append(args, fmt.Sprintf("--store-endpoints=%s", storeEndpoints))
	if os.Getenv("DEBUG") != "" {
		args = append(args, "--debug")
	}
	args = append(args, a...)

	connParams := pg.ConnParams{
		"user":     pgSUUsername,
		"password": pgSUPassword,
		"host":     listenAddress,
		"port":     port,
		"dbname":   "postgres",
		"sslmode":  "disable",
	}

	replConnParams := pg.ConnParams{
		"user":        pgReplUsername,
		"password":    pgReplPassword,
		"host":        listenAddress,
		"port":        port,
		"dbname":      "postgres",
		"sslmode":     "disable",
		"replication": "1",
	}

	connString := connParams.ConnString()
	db, err := sql.Open(pg.SQLDriverName, connString)
	if err != nil {
		return nil, err
	}

	bin := os.Getenv("STPROXY_BIN")
	if bin == "" {
		return nil, fmt.Errorf("missing STPROXY_BIN env")
	}
	tp := &TestProxy{
		t: t,
		Process: Process{
			t:    t,
			uid:  uid,
			name: "proxy",
			bin:  bin,
			args: args,
		},
		listenAddress:  listenAddress,
		port:           port,
		replConnParams: replConnParams,
		db:             db,
	}
	return tp, nil
}

func (tp *TestProxy) WaitListening(timeout time.Duration) error {
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		_, err := net.DialTimeout("tcp", net.JoinHostPort(tp.listenAddress, tp.port), timeout-time.Since(start))
		if err == nil {
			return nil
		}
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout")
}

func (tp *TestProxy) CheckListening() bool {
	_, err := net.Dial("tcp", net.JoinHostPort(tp.listenAddress, tp.port))
	return err == nil
}

func (tp *TestProxy) WaitNotListening(timeout time.Duration) error {
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		_, err := net.DialTimeout("tcp", net.JoinHostPort(tp.listenAddress, tp.port), timeout-time.Since(start))
		if err != nil {
			return nil
		}
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout")
}

func (tp *TestProxy) Exec(query string, args ...interface{}) (sql.Result, error) {
	res, err := tp.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (tp *TestProxy) Query(query string, args ...interface{}) (*sql.Rows, error) {
	res, err := tp.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (tp *TestProxy) ReplConnParams() pg.ConnParams {
	return tp.replConnParams.Copy()
}

func (tp *TestProxy) GetPGParameters() (common.Parameters, error) {
	return GetPGParameters(tp)
}

func (tp *TestProxy) WaitRightMaster(tk *TestKeeper, timeout time.Duration) error {
	return tk.WaitPGParameter("port", tk.pgPort, timeout)
}

func StolonCtl(t *testing.T, clusterName string, storeBackend store.Backend, storeEndpoints string, a ...string) error {
	args := []string{}
	args = append(args, fmt.Sprintf("--cluster-name=%s", clusterName))
	args = append(args, fmt.Sprintf("--store-backend=%s", storeBackend))
	args = append(args, fmt.Sprintf("--store-endpoints=%s", storeEndpoints))
	args = append(args, a...)

	t.Logf("executing stolonctl, args: %s", args)

	bin := os.Getenv("STCTL_BIN")
	if bin == "" {
		return fmt.Errorf("missing STCTL_BIN env")
	}
	cmd := exec.Command(bin, args...)
	pr, pw, err := os.Pipe()
	if err != nil {
		return err
	}
	cmd.Stdout = pw
	cmd.Stderr = pw
	go func() {
		scanner := bufio.NewScanner(pr)
		for scanner.Scan() {
			t.Logf("[%s]: %s", "stolonctl", scanner.Text())
		}
	}()

	return cmd.Run()
}

type TestStore struct {
	t *testing.T
	Process
	listenAddress string
	port          string
	store         store.KVStore
	storeBackend  store.Backend
}

func NewTestStore(t *testing.T, dir string, a ...string) (*TestStore, error) {
	storeBackend := store.Backend(os.Getenv("STOLON_TEST_STORE_BACKEND"))
	switch storeBackend {
	case "etcdv3":
		return NewTestEtcd(t, dir, storeBackend, a...)
	}
	return nil, fmt.Errorf("wrong store backend")
}

func NewTestEtcd(t *testing.T, dir string, backend store.Backend, a ...string) (*TestStore, error) {
	u := uuid.New()
	uid := fmt.Sprintf("%x", u[:4])

	dataDir := filepath.Join(dir, fmt.Sprintf("etcd%s", uid))

	listenAddress, port, err := getFreePort(true, false)
	if err != nil {
		return nil, err
	}
	listenAddress2, port2, err := getFreePort(true, false)
	if err != nil {
		return nil, err
	}

	args := []string{}
	args = append(args, fmt.Sprintf("--name=%s", uid))
	args = append(args, fmt.Sprintf("--data-dir=%s", dataDir))
	args = append(args, fmt.Sprintf("--listen-client-urls=http://%s:%s", listenAddress, port))
	args = append(args, fmt.Sprintf("--advertise-client-urls=http://%s:%s", listenAddress, port))
	args = append(args, fmt.Sprintf("--listen-peer-urls=http://%s:%s", listenAddress2, port2))
	args = append(args, fmt.Sprintf("--initial-advertise-peer-urls=http://%s:%s", listenAddress2, port2))
	args = append(args, fmt.Sprintf("--initial-cluster=%s=http://%s:%s", uid, listenAddress2, port2))
	args = append(args, a...)

	storeEndpoints := fmt.Sprintf("%s:%s", listenAddress, port)

	storeConfig := store.Config{
		Backend:   store.Backend(backend),
		Endpoints: storeEndpoints,
		Timeout:   defaultStoreTimeout,
	}
	kvstore, err := store.NewKVStore(storeConfig)
	if err != nil {
		return nil, fmt.Errorf("cannot create store: %v", err)
	}

	bin := os.Getenv("ETCD_BIN")
	if bin == "" {
		return nil, fmt.Errorf("missing ETCD_BIN env")
	}
	tstore := &TestStore{
		t: t,
		Process: Process{
			t:    t,
			uid:  uid,
			name: "etcd",
			bin:  bin,
			args: args,
		},
		listenAddress: listenAddress,
		port:          port,
		store:         kvstore,
		storeBackend:  backend,
	}
	return tstore, nil
}

func (ts *TestStore) WaitUp(timeout time.Duration) error {
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		ctx, cancel := context.WithTimeout(context.Background(), defaultStoreTimeout)
		_, err := ts.store.Get(ctx, "anykey")
		cancel()
		ts.t.Logf("err: %v", err)
		if err != nil && err == store.ErrKeyNotFound {
			return nil
		}
		if err == nil {
			return nil
		}
		time.Sleep(sleepInterval)
	}

	return fmt.Errorf("timeout")
}

func (ts *TestStore) WaitDown(timeout time.Duration) error {
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		ctx, cancel := context.WithTimeout(context.Background(), defaultStoreTimeout)
		_, err := ts.store.Get(ctx, "anykey")
		cancel()
		if err != nil && err != store.ErrKeyNotFound {
			return nil
		}
		time.Sleep(sleepInterval)
	}

	return fmt.Errorf("timeout")
}

func WaitClusterDataUpdated(e *store.KVBackedStore, timeout time.Duration) error {
	icd, _, err := e.GetClusterData(context.TODO())
	if err != nil {
		return fmt.Errorf("unexpected err: %v", err)
	}
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		cd, _, err := e.GetClusterData(context.TODO())
		if err != nil || cd == nil {
			goto end
		}
		if !reflect.DeepEqual(icd, cd) {
			return nil
		}
	end:
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout")
}

func WaitClusterDataWithMaster(e *store.KVBackedStore, timeout time.Duration) (string, error) {
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		cd, _, err := e.GetClusterData(context.TODO())
		if err != nil || cd == nil {
			goto end
		}
		if cd.Cluster.Status.Phase == cluster.ClusterPhaseNormal && cd.Cluster.Status.Master != "" {
			return cd.DBs[cd.Cluster.Status.Master].Spec.KeeperUID, nil
		}
	end:
		time.Sleep(sleepInterval)
	}
	return "", fmt.Errorf("timeout")
}

func WaitClusterDataMaster(master string, e *store.KVBackedStore, timeout time.Duration) error {
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		cd, _, err := e.GetClusterData(context.TODO())
		if err != nil || cd == nil {
			goto end
		}
		if cd.Cluster.Status.Phase == cluster.ClusterPhaseNormal && cd.Cluster.Status.Master != "" {
			if cd.DBs[cd.Cluster.Status.Master].Spec.KeeperUID == master {
				return nil
			}
		}
	end:
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout")
}

func WaitClusterDataKeeperInitialized(keeperUID string, e *store.KVBackedStore, timeout time.Duration) error {
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		cd, _, err := e.GetClusterData(context.TODO())
		if err != nil || cd == nil {
			goto end
		}
		// Check for db on keeper to be initialized
		for _, db := range cd.DBs {
			if db.Spec.KeeperUID == keeperUID {
				if db.Status.CurrentGeneration >= cluster.InitialGeneration {
					return nil
				}
			}
		}
	end:
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout")
}

// WaitClusterDataSynchronousStandbys waits for:
// * synchronous standby defined in masterdb spec
// * synchronous standby reported from masterdb status
func WaitClusterDataSynchronousStandbys(synchronousStandbys []string, e *store.KVBackedStore, timeout time.Duration) error {
	sort.Strings(synchronousStandbys)
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		cd, _, err := e.GetClusterData(context.TODO())
		if err != nil || cd == nil {
			goto end
		}
		if cd.Cluster.Status.Phase == cluster.ClusterPhaseNormal && cd.Cluster.Status.Master != "" {
			masterDB := cd.DBs[cd.Cluster.Status.Master]
			// get keepers for db spec synchronousStandbys
			keepersUIDs := []string{}
			for _, dbUID := range masterDB.Spec.SynchronousStandbys {
				db, ok := cd.DBs[dbUID]
				if ok {
					keepersUIDs = append(keepersUIDs, db.Spec.KeeperUID)
				}
			}
			sort.Strings(keepersUIDs)
			if !reflect.DeepEqual(synchronousStandbys, keepersUIDs) {
				goto end
			}

			// get keepers for db status synchronousStandbys
			keepersUIDs = []string{}
			for _, dbUID := range masterDB.Status.SynchronousStandbys {
				db, ok := cd.DBs[dbUID]
				if ok {
					keepersUIDs = append(keepersUIDs, db.Spec.KeeperUID)
				}
			}
			sort.Strings(keepersUIDs)
			if !reflect.DeepEqual(synchronousStandbys, keepersUIDs) {
				goto end
			}
			return nil
		}
	end:
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout")
}

func WaitClusterPhase(e *store.KVBackedStore, phase cluster.ClusterPhase, timeout time.Duration) error {
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		cd, _, err := e.GetClusterData(context.TODO())
		if err != nil || cd == nil {
			goto end
		}
		if cd.Cluster.Status.Phase == phase {
			return nil
		}
	end:
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout")
}

func WaitNumDBs(e *store.KVBackedStore, n int, timeout time.Duration) error {
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		cd, _, err := e.GetClusterData(context.TODO())
		if err != nil || cd == nil {
			goto end
		}
		if len(cd.DBs) == n {
			return nil
		}
	end:
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout")
}

func WaitStandbyKeeper(e *store.KVBackedStore, keeperUID string, timeout time.Duration) error {
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		cd, _, err := e.GetClusterData(context.TODO())
		if err != nil || cd == nil {
			goto end
		}

		for _, db := range cd.DBs {
			if db.UID == cd.Cluster.Status.Master {
				continue
			}
			if db.Spec.KeeperUID == keeperUID && db.Spec.Role == common.RoleStandby {
				return nil
			}
		}
	end:
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout")
}

func WaitClusterDataKeepers(keepersUIDs []string, e *store.KVBackedStore, timeout time.Duration) error {
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		cd, _, err := e.GetClusterData(context.TODO())
		if err != nil || cd == nil {
			goto end
		}
		if len(keepersUIDs) != len(cd.Keepers) {
			goto end
		}
		// Check for db on keeper to be initialized
		for _, keeper := range cd.Keepers {
			if !slices.Contains(keepersUIDs, keeper.UID) {
				goto end
			}
		}
		return nil
	end:
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout")
}

// WaitClusterSyncedXLogPos waits for all the specified keepers to have the same
// reported XLogPos and that it's >= than master XLogPos
func WaitClusterSyncedXLogPos(keepers []*TestKeeper, xLogPos uint64, e *store.KVBackedStore, timeout time.Duration) error {
	keepersUIDs := []string{}
	for _, sk := range keepers {
		keepersUIDs = append(keepersUIDs, sk.uid)
	}

	// check that master and all the keepers XLogPos are the same and >=
	// masterXLogPos
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		c := 0
		curXLogPos := uint64(0)
		cd, _, err := e.GetClusterData(context.TODO())
		if err != nil || cd == nil {
			goto end
		}
		// Check for db on keeper to be initialized
		for _, keeper := range cd.Keepers {
			if !slices.Contains(keepersUIDs, keeper.UID) {
				continue
			}
			for _, db := range cd.DBs {
				if db.Spec.KeeperUID == keeper.UID {
					if db.Status.XLogPos < xLogPos {
						goto end
					}
					if c == 0 {
						curXLogPos = db.Status.XLogPos
					} else {
						if db.Status.XLogPos != curXLogPos {
							goto end
						}
					}
				}
			}
			c++
		}
		if c == len(keepersUIDs) {
			return nil
		}
	end:
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout")
}

func WaitClusterDataEnabledProxiesNum(e *store.KVBackedStore, n int, timeout time.Duration) error {
	// TODO(sgotti) find a way to retrieve the proxies internally generated uids
	// and check for them instead of relying only on the number of proxies
	start := time.Now()
	for time.Now().Add(-timeout).Before(start) {
		cd, _, err := e.GetClusterData(context.TODO())
		if err != nil || cd == nil {
			goto end
		}
		if len(cd.Proxy.Spec.EnabledProxies) == n {
			return nil
		}
	end:
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout")
}

func testFreeTCPPort(port int) error {
	ln, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return err
	}
	ln.Close()
	return nil
}

func testFreeUDPPort(port int) error {
	ln, err := net.ListenPacket("udp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return err
	}
	ln.Close()
	return nil
}

// Hack to find a free tcp and udp port
func getFreePort(tcp bool, udp bool) (string, string, error) {
	portMutex.Lock()
	defer portMutex.Unlock()

	if !tcp && !udp {
		return "", "", fmt.Errorf("at least one of tcp or udp port shuld be required")
	}
	localhostIP, err := net.ResolveIPAddr("ip", "localhost")
	if err != nil {
		return "", "", fmt.Errorf("failed to resolve ip addr: %v", err)
	}
	for {
		curPort++
		if curPort > MaxPort {
			return "", "", fmt.Errorf("all available ports to test have been exausted")
		}
		if tcp {
			if err := testFreeTCPPort(curPort); err != nil {
				continue
			}
		}
		if udp {
			if err := testFreeUDPPort(curPort); err != nil {
				continue
			}
		}
		return localhostIP.IP.String(), strconv.Itoa(curPort), nil
	}
}

func writeClusterSpec(dir string, cs *cluster.ClusterSpec) (string, error) {
	csj, err := json.Marshal(cs)
	if err != nil {
		return "", err
	}
	tmpFile, err := os.CreateTemp(dir, "initial-cluster-spec.json")
	if err != nil {
		return "", err
	}
	defer tmpFile.Close()
	if _, err := tmpFile.Write(csj); err != nil {
		return "", err
	}
	return tmpFile.Name(), nil

}
