# Integration Tests

This directory contains process-level HA integration tests for `hysteron`.
The default and recommended execution mode is container-first through
`tests/integration/compose.yml`.

## Goals

* Stable, reproducible integration pipeline.
* Clear separation between image build and test execution.
* Easy local and CI matrix runs across PostgreSQL majors.
* Log artifacts for every run under `artifacts/integration`.

## Pipeline Model

Integration execution is split into two explicit phases:

1. Build phase: build integration image for target PostgreSQL major.
1. Run phase: execute tests in that image.

This avoids hidden rebuilds during retries and makes matrix runs easier to
control and debug.

## Entrypoints

Primary orchestration lives in `tests/Makefile`.
Repository root `Makefile` is intentionally a thin pass-through layer to
`tests/Makefile` for integration targets:

* `make integration` -> local non-container mode
* `make integration-compose` -> container build+run for one `PG_MAJOR`
* `make integration-matrix` -> container matrix build+run (`PG_MATRIX`)
* `make integration-matrix-ci` -> reduced container matrix (`PG_MATRIX_CI`)
* `make integration-profile-<name>` -> forwards to
  `make -C tests integration-profile-run PROFILE=<name>`

Direct tests-scope entrypoints:

* `make -C tests integration-build`
* `make -C tests integration-run`
* `make -C tests integration`
* `make -C tests integration-matrix-build`
* `make -C tests integration-matrix-run`
* `make -C tests integration-matrix`
* `make -C tests integration-matrix-ci-build`
* `make -C tests integration-matrix-ci-run`
* `make -C tests integration-matrix-ci`
* `make -C tests integration-smoke`
* `make -C tests integration-full`
* `make -C tests integration-profile-smoke`
* `make -C tests integration-local`
* `make -C tests integration-check-matrix`
* `make -C tests integration-profile-list PROFILE=<smoke|storage-ha|logical-slots|merge-gate|merge|full|soak>`
* `make -C tests integration-profile-run PROFILE=<smoke|storage-ha|logical-slots|merge-gate|merge|full|soak>`
* `make -C tests integration-profile-merge-gate`
* `make -C tests integration-profile-merge`
* `make -C tests integration-profile-full`
* `make -C tests integration-profile-soak`
* `make -C tests integration-baseline-inventory`
* `make -C tests integration-profile-counts`

## Quick Start (Container, Single PostgreSQL Version)

```bash
make -C tests integration-build PG_MAJOR=18
make -C tests integration-run PG_MAJOR=18
```

Or combined:

```bash
make integration-compose PG_MAJOR=18
```

## Matrix Runs

Full matrix:

```bash
make integration-matrix PG_MATRIX="18 17 16 15 14"
```

Reduced CI matrix:

```bash
make integration-matrix-ci PG_MATRIX_CI="18 14"
```

## Test Selection and Runtime Controls

Useful variables:

* `PG_MAJOR`: PostgreSQL major for single-version container run (`18` default).
* `PG_MATRIX`: majors for full matrix.
* `PG_MATRIX_CI`: majors for reduced matrix.
* `PG_LIST`: explicit majors list for profile matrix runs.
* `MAX_MATRIX_WORKERS`: max parallel profile jobs for matrix runs.
* `INTEGRATION_RUN`: forwarded to `go test -run`.
* `INTEGRATION_TEST_ARGS`: extra `go test` args.
* `INTEGRATION_TIMEOUT`: `go test -timeout` value (`30m` default).
* `INTEGRATION_PARALLEL`: `go test -parallel` value (`4` default).
* `INTEGRATION_MAX_STORES`: max parallel test stores (`8` default).
* `INTEGRATION_DEBUG`: optional debug mode forwarded to container.
* `HYSTERON_INTEGRATION_SOAK`: enable soak behavior where supported.
* `HYSTERON_INTEGRATION_SOAK_FAILOVER_CYCLES`: failover cycles for soak.
* `INTEGRATION_ARTIFACTS_DIR`: log destination (`artifacts/integration`).

Examples:

```bash
make -C tests integration-run \
  PG_MAJOR=18 \
  INTEGRATION_RUN='TestIntegrationClusterDataWalDirAndTablespaces.*' \
  INTEGRATION_TEST_ARGS='-v -count 1'
```

```bash
make -C tests integration-smoke PG_MAJOR=18
```

Profile run via manifest:

```bash
make -C tests integration-check-matrix
make -C tests integration-profile-list PROFILE=smoke
make -C tests integration-profile-counts
make -C tests integration-profile-run PROFILE=smoke PG_MAJOR=18
make -C tests integration-profile-fast-matrix
make -C tests integration-profile-storage-ha-matrix
make -C tests integration-profile-logical-slots-matrix
make -C tests integration-profile-merge-gate-matrix
make -C tests integration-profile-merge-matrix
```

`matrix.yaml` is the coverage parity source of truth. New integration tests
must be matched by at least one selector.

## Logical CDC Scope

Current `logical-slots` profile validates PostgreSQL-only logical CDC behavior
inside hysteron:

* managed logical-slot lifecycle and validation guards;
* failover/rejoin continuity with deterministic logical change assertions;
* gate behavior for standby readiness/advance constraints.

## Slot Assertion Semantics

Replication-slot integration checks use two assertion modes:

* strict set equality (`waitHysteronReplicationSlots`) when full slot set is
  deterministic;
* required-subset checks (`waitHysteronReplicationSlotsContains`) when keeper
  reconciliation can temporarily include extra `hysteron_*` slots during
  transition windows.

Use the subset mode only for transition-sensitive flows. Prefer strict checks
everywhere else.

## Recommended Test Sets

Fast PR sanity (`PG 18`):

```bash
make integration-profile-smoke PG_MAJOR=18
```

Fast dual-major developer gate (`smoke + storage-ha + logical-slots` on
`PG 18 + 14`, sequential):

```bash
make integration-profile-fast-matrix
```

Fast storage/HA iteration (`PG 18`):

```bash
make integration-profile-storage-ha PG_MAJOR=18
```

Dual-major storage/HA gate (`PG 18 + 14`, sequential):

```bash
make integration-profile-storage-ha-matrix
```

Fast logical-slot iteration (`PG 18`):

```bash
make integration-profile-logical-slots PG_MAJOR=18
```

Dual-major logical-slot gate (`PG 18 + 14`, sequential):

```bash
make integration-profile-logical-slots-matrix
```

Dual-major merge gate (`PG 18 + 14`, sequential):

```bash
make integration-profile-merge-gate-matrix
```

Dual-major merge profile (`PG 18 + 14`, sequential):

```bash
make integration-profile-merge-matrix
```

Deeper merge profile on one major:

```bash
make integration-profile-merge PG_MAJOR=18
```

Full profile on one major:

```bash
make integration-profile-full PG_MAJOR=18
```

## Parallel Matrix Runs

You can run one profile across multiple PostgreSQL majors in parallel from
bash.

Example (`xargs`): run `full` on `14 15 16 17 18` with two workers:

```bash
printf '%s\n' 14 15 16 17 18 | xargs -n1 -P2 -I{} \
  make -C tests integration-profile-run PROFILE=full PG_MAJOR={}
```

Example (`xargs`): run `merge-gate` on `14 15 16 17 18` with three workers:

```bash
printf '%s\n' 14 15 16 17 18 | xargs -n1 -P3 -I{} \
  make -C tests integration-profile-run PROFILE=merge-gate PG_MAJOR={}
```

Example (`GNU parallel`):

```bash
parallel -j 3 "make -C tests integration-profile-run PROFILE=merge-gate PG_MAJOR={}" ::: 14 15 16 17 18
```

Notes:

* Parallel runs increase CPU, RAM, disk IO, and container contention.
* Start with `MAX_MATRIX_WORKERS=2` and increase after observing stability.
* Keep `full` and `soak` profile parallelism conservative on shared CI hosts.

## Manual Docker Compose Execution

If you need raw compose commands instead of Make:

```bash
PG_MAJOR=18 docker compose -f tests/integration/compose.yml build integration
PG_MAJOR=18 INTEGRATION_RUN='TestIntegration.*' docker compose \
  -f tests/integration/compose.yml run --rm integration
```

## Local Non-Container Fallback

Local mode is for low-level debugging and parity checks. Container mode is the
default for CI and reproducible runs.

Requirements for local mode:

* `HYSTERON_BIN` points to local `hysteron` binary.
* `HYSTERON_TEST_STORE_BACKEND` is `etcd` or `etcdv3`.
* `ETCD_BIN` points to local `etcd` binary.
* PostgreSQL binaries are available in `PATH`.

Run:

```bash
make integration
```

Or directly:

```bash
HYSTERON_BIN=./build/hysteron \
HYSTERON_TEST_STORE_BACKEND=etcdv3 \
HYSTERON_INTEGRATION_MAX_STORES=8 \
ETCD_BIN="$(command -v etcd)" \
go test -tags integration -v -count 1 ./tests/integration
```

## Artifacts and Failure Triage

Each stage writes logs to `artifacts/integration` with timestamped names:

* `integration-build-pg<major>-<timestamp>.log`
* `integration-run-pg<major>-<timestamp>.log`
* `integration-run-<profile>-pg<major>-<timestamp>.jsonl` (`go test -json`)
* `integration-run-<profile>-pg<major>-<timestamp>.md` (markdown summary with
  failed-test output snippets)
* `INDEX.md` (latest report per profile/PG major)
* `integration-local-<timestamp>.log`

Generate/refresh report index:

```bash
make integration-report-index
```

Suggested triage order:

1. Check build log for image/toolchain failures.
1. Check run log for deterministic test failures.
1. Re-run single test with `INTEGRATION_RUN` and `-count 1`.
1. Re-run with lower `INTEGRATION_MAX_STORES`
   if resource contention is suspected.
