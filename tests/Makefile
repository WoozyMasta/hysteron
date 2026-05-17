GO ?= go
CRI ?= docker
ROOT_DIR ?= ..

PG_MAJOR ?= 18
PG_MATRIX ?= 18 17 16 15 14
PG_MATRIX_CI ?= 18 14
PG_LIST ?= $(PG_MATRIX)
MAX_MATRIX_WORKERS ?= 2

INTEGRATION_TAGS ?= integration
INTEGRATION_TIMEOUT ?= 60m
INTEGRATION_PARALLEL ?= 4
INTEGRATION_MAX_STORES ?= 8
INTEGRATION_RUN ?=
INTEGRATION_TEST_ARGS ?=
INTEGRATION_STORE_BACKEND ?= etcdv3
INTEGRATION_DEBUG ?=
HYSTERON_INTEGRATION_SOAK ?=
HYSTERON_INTEGRATION_SOAK_FAILOVER_CYCLES ?=
INTEGRATION_ARTIFACTS_DIR ?= ../artifacts/integration
INTEGRATION_TIMESTAMP ?= $(shell date -u +%Y%m%dT%H%M%SZ)

COMPOSE_FILE := integration/compose.yml
COMPOSE_CMD := PG_MAJOR="$(PG_MAJOR)" $(CRI) compose -f "$(COMPOSE_FILE)"
INTEGRATION_RUN_ARG := $(if $(INTEGRATION_RUN),-run '$(INTEGRATION_RUN)',)

.PHONY: integration-build integration-run integration \
	integration-matrix-build integration-matrix-run integration-matrix \
	integration-matrix-ci-build integration-matrix-ci-run integration-matrix-ci \
	integration-container \
	integration-local integration-smoke integration-full \
	integration-profile-smoke integration-profile-storage-ha integration-profile-logical-slots integration-profile-merge-gate integration-profile-merge integration-profile-full integration-profile-soak \
	integration-check-matrix integration-profile-regex integration-profile-list integration-profile-run \
	integration-baseline-inventory integration-profile-counts integration-profile-fast-matrix integration-profile-storage-ha-matrix integration-profile-logical-slots-matrix integration-profile-merge-gate-matrix integration-profile-merge-matrix \
	integration-runtime-summary integration-report-index integration-clean-artifacts

integration-build:
	@mkdir -p "$(INTEGRATION_ARTIFACTS_DIR)"
	@log_file="$(INTEGRATION_ARTIFACTS_DIR)/integration-build-pg$(PG_MAJOR)-$(INTEGRATION_TIMESTAMP).log"; \
	echo ">> building integration image for PostgreSQL $(PG_MAJOR)"; \
	echo ">> writing integration build log to $$log_file"; \
	$(COMPOSE_CMD) build integration > "$$log_file" 2>&1; \
	status=$$?; cat "$$log_file"; exit $$status

integration-run:
	@mkdir -p "$(INTEGRATION_ARTIFACTS_DIR)"
	@log_file="$(INTEGRATION_ARTIFACTS_DIR)/integration-run-pg$(PG_MAJOR)-$(INTEGRATION_TIMESTAMP).log"; \
	echo ">> running integration tests for PostgreSQL $(PG_MAJOR)"; \
	echo ">> writing integration run log to $$log_file"; \
	INTEGRATION_RUN="$(INTEGRATION_RUN)" \
	INTEGRATION_TEST_ARGS="$(INTEGRATION_TEST_ARGS)" \
	INTEGRATION_TIMEOUT="$(INTEGRATION_TIMEOUT)" \
	INTEGRATION_PARALLEL="$(INTEGRATION_PARALLEL)" \
	INTEGRATION_MAX_STORES="$(INTEGRATION_MAX_STORES)" \
	INTEGRATION_DEBUG="$(INTEGRATION_DEBUG)" \
	HYSTERON_INTEGRATION_SOAK="$(HYSTERON_INTEGRATION_SOAK)" \
	HYSTERON_INTEGRATION_SOAK_FAILOVER_CYCLES="$(HYSTERON_INTEGRATION_SOAK_FAILOVER_CYCLES)" \
	$(COMPOSE_CMD) run --rm integration > "$$log_file" 2>&1; \
	status=$$?; cat "$$log_file"; exit $$status

integration: integration-build integration-run

integration-container:
	@rm -rf /tmp/hysteron-build
	@$(MAKE) -C "$(ROOT_DIR)" build DOC_BUILD=0 OUTPUT_DIR=/tmp/hysteron-build
	@mkdir -p "$(INTEGRATION_ARTIFACTS_DIR)"
	@log_file="$(INTEGRATION_ARTIFACTS_DIR)/integration-local-$(INTEGRATION_TIMESTAMP).log"; \
	json_file="$(INTEGRATION_ARTIFACTS_DIR)/integration-run-$${INTEGRATION_PROFILE:-adhoc}-pg$(PG_MAJOR)-$(INTEGRATION_TIMESTAMP).jsonl"; \
	md_report_file="$(INTEGRATION_ARTIFACTS_DIR)/integration-run-$${INTEGRATION_PROFILE:-adhoc}-pg$(PG_MAJOR)-$(INTEGRATION_TIMESTAMP).md"; \
	etcd_bin="$${ETCD_BIN:-$$(command -v etcd)}"; \
	if [ -z "$$etcd_bin" ]; then echo "etcd binary not found in PATH"; exit 1; fi; \
	echo ">> running container integration tests"; \
	echo ">> writing integration log to $$log_file"; \
	echo ">> writing integration json report source to $$json_file"; \
	run_arg=""; \
	if [ -n "$$INTEGRATION_RUN" ]; then run_arg="-run $$INTEGRATION_RUN"; fi; \
	HYSTERON_TEST_STORE_BACKEND="$(INTEGRATION_STORE_BACKEND)" \
	HYSTERON_INTEGRATION_MAX_STORES="$(INTEGRATION_MAX_STORES)" \
	ETCD_BIN="$$etcd_bin" \
	HYSTERON_BIN="/tmp/hysteron-build/hysteron" \
	$(GO) test -tags "$(INTEGRATION_TAGS)" -timeout "$(INTEGRATION_TIMEOUT)" \
		-parallel "$(INTEGRATION_PARALLEL)" $$run_arg \
		$(INTEGRATION_TEST_ARGS) \
		-json -count 1 ./integration > "$$json_file" 2>> "$$log_file"; \
	status=$$?; \
	$(GO) run ./integration/cmd/reportctl markdown \
		--input "$$json_file" \
		--output "$$md_report_file" \
		--profile "$${INTEGRATION_PROFILE:-adhoc}" \
		--pg-major "$(PG_MAJOR)" >> "$$log_file" 2>&1 || true; \
	cat "$$log_file"; \
	if [ -f "$$md_report_file" ]; then cat "$$md_report_file"; fi; \
	exit $$status

integration-matrix-build:
	@mkdir -p "$(INTEGRATION_ARTIFACTS_DIR)"
	@for pg in $(PG_MATRIX); do \
		log_file="$(INTEGRATION_ARTIFACTS_DIR)/integration-build-pg$${pg}-$(INTEGRATION_TIMESTAMP).log"; \
		echo ">> building integration image for PostgreSQL $$pg"; \
		echo ">> writing integration build log to $$log_file"; \
		PG_MAJOR="$$pg" $(CRI) compose -f "$(COMPOSE_FILE)" build integration > "$$log_file" 2>&1; \
		status=$$?; cat "$$log_file"; if [ $$status -ne 0 ]; then exit $$status; fi; \
	done

integration-matrix-run:
	@mkdir -p "$(INTEGRATION_ARTIFACTS_DIR)"
	@for pg in $(PG_MATRIX); do \
		log_file="$(INTEGRATION_ARTIFACTS_DIR)/integration-run-pg$${pg}-$(INTEGRATION_TIMESTAMP).log"; \
		echo ">> running integration tests for PostgreSQL $$pg"; \
		echo ">> writing integration run log to $$log_file"; \
		INTEGRATION_RUN="$(INTEGRATION_RUN)" \
		INTEGRATION_TEST_ARGS="$(INTEGRATION_TEST_ARGS)" \
		INTEGRATION_TIMEOUT="$(INTEGRATION_TIMEOUT)" \
		INTEGRATION_PARALLEL="$(INTEGRATION_PARALLEL)" \
		INTEGRATION_MAX_STORES="$(INTEGRATION_MAX_STORES)" \
		INTEGRATION_DEBUG="$(INTEGRATION_DEBUG)" \
		HYSTERON_INTEGRATION_SOAK="$(HYSTERON_INTEGRATION_SOAK)" \
		HYSTERON_INTEGRATION_SOAK_FAILOVER_CYCLES="$(HYSTERON_INTEGRATION_SOAK_FAILOVER_CYCLES)" \
		PG_MAJOR="$$pg" $(CRI) compose -f "$(COMPOSE_FILE)" run --rm integration > "$$log_file" 2>&1; \
		status=$$?; cat "$$log_file"; if [ $$status -ne 0 ]; then exit $$status; fi; \
	done

integration-matrix: integration-matrix-build integration-matrix-run

integration-matrix-ci-build:
	@mkdir -p "$(INTEGRATION_ARTIFACTS_DIR)"
	@for pg in $(PG_MATRIX_CI); do \
		log_file="$(INTEGRATION_ARTIFACTS_DIR)/integration-build-pg$${pg}-$(INTEGRATION_TIMESTAMP).log"; \
		echo ">> building integration image for PostgreSQL $$pg (ci matrix)"; \
		echo ">> writing integration build log to $$log_file"; \
		PG_MAJOR="$$pg" $(CRI) compose -f "$(COMPOSE_FILE)" build integration > "$$log_file" 2>&1; \
		status=$$?; cat "$$log_file"; if [ $$status -ne 0 ]; then exit $$status; fi; \
	done

integration-matrix-ci-run:
	@mkdir -p "$(INTEGRATION_ARTIFACTS_DIR)"
	@for pg in $(PG_MATRIX_CI); do \
		log_file="$(INTEGRATION_ARTIFACTS_DIR)/integration-run-pg$${pg}-$(INTEGRATION_TIMESTAMP).log"; \
		echo ">> running integration tests for PostgreSQL $$pg (ci matrix)"; \
		echo ">> writing integration run log to $$log_file"; \
		INTEGRATION_RUN="$(INTEGRATION_RUN)" \
		INTEGRATION_TEST_ARGS="$(INTEGRATION_TEST_ARGS)" \
		INTEGRATION_TIMEOUT="$(INTEGRATION_TIMEOUT)" \
		INTEGRATION_PARALLEL="$(INTEGRATION_PARALLEL)" \
		INTEGRATION_MAX_STORES="$(INTEGRATION_MAX_STORES)" \
		INTEGRATION_DEBUG="$(INTEGRATION_DEBUG)" \
		HYSTERON_INTEGRATION_SOAK="$(HYSTERON_INTEGRATION_SOAK)" \
		HYSTERON_INTEGRATION_SOAK_FAILOVER_CYCLES="$(HYSTERON_INTEGRATION_SOAK_FAILOVER_CYCLES)" \
		PG_MAJOR="$$pg" $(CRI) compose -f "$(COMPOSE_FILE)" run --rm integration > "$$log_file" 2>&1; \
		status=$$?; cat "$$log_file"; if [ $$status -ne 0 ]; then exit $$status; fi; \
	done

integration-matrix-ci: integration-matrix-ci-build integration-matrix-ci-run

integration-smoke:
	@$(MAKE) integration-profile-run PROFILE=smoke

integration-full:
	@$(MAKE) integration-matrix

integration-profile-smoke:
	@$(MAKE) integration-profile-run PROFILE=smoke

integration-profile-storage-ha:
	@$(MAKE) integration-profile-run PROFILE=storage-ha

integration-profile-logical-slots:
	@$(MAKE) integration-profile-run PROFILE=logical-slots

integration-profile-merge-gate:
	@$(MAKE) integration-profile-run PROFILE=merge-gate

integration-profile-merge:
	@$(MAKE) integration-profile-run PROFILE=merge

integration-profile-full:
	@$(MAKE) integration-profile-run PROFILE=full

integration-profile-soak:
	@$(MAKE) integration-profile-run PROFILE=soak

integration-check-matrix:
	@$(GO) run ./integration/cmd/matrixctl check

integration-profile-regex:
	@if [ -z "$(PROFILE)" ]; then echo "PROFILE is required"; exit 1; fi
	@$(GO) run ./integration/cmd/matrixctl regex --profile "$(PROFILE)"

integration-profile-list:
	@if [ -z "$(PROFILE)" ]; then echo "PROFILE is required"; exit 1; fi
	@$(GO) run ./integration/cmd/matrixctl list --profile "$(PROFILE)"

integration-profile-run:
	@if [ -z "$(PROFILE)" ]; then echo "PROFILE is required"; exit 1; fi
	@profile_regex="$$($(GO) run ./integration/cmd/matrixctl regex --profile "$(PROFILE)")"; \
	mkdir -p "$(INTEGRATION_ARTIFACTS_DIR)"; \
	log_file="$(INTEGRATION_ARTIFACTS_DIR)/integration-run-$(PROFILE)-pg$(PG_MAJOR)-$(INTEGRATION_TIMESTAMP).log"; \
	echo ">> running integration profile $(PROFILE) for PostgreSQL $(PG_MAJOR)"; \
	echo ">> writing integration run log to $$log_file"; \
	INTEGRATION_RUN="$$profile_regex" \
	INTEGRATION_PROFILE="$(PROFILE)" \
	INTEGRATION_REPORT="1" \
	INTEGRATION_TEST_ARGS="$(INTEGRATION_TEST_ARGS)" \
	INTEGRATION_TIMEOUT="$(INTEGRATION_TIMEOUT)" \
	INTEGRATION_PARALLEL="$(INTEGRATION_PARALLEL)" \
	INTEGRATION_MAX_STORES="$(INTEGRATION_MAX_STORES)" \
	INTEGRATION_DEBUG="$(INTEGRATION_DEBUG)" \
	HYSTERON_INTEGRATION_SOAK="$(HYSTERON_INTEGRATION_SOAK)" \
	HYSTERON_INTEGRATION_SOAK_FAILOVER_CYCLES="$(HYSTERON_INTEGRATION_SOAK_FAILOVER_CYCLES)" \
	$(COMPOSE_CMD) run --rm integration > "$$log_file" 2>&1; \
	status=$$?; cat "$$log_file"; exit $$status

integration-baseline-inventory:
	@mkdir -p "$(INTEGRATION_ARTIFACTS_DIR)"
	@out="$(INTEGRATION_ARTIFACTS_DIR)/integration-profile-inventory-$(INTEGRATION_TIMESTAMP).txt"; \
	echo ">> writing profile inventory to $$out"; \
	{ \
		echo "profile=smoke"; \
		$(GO) run ./integration/cmd/matrixctl list --profile smoke; \
		echo ""; \
		echo "profile=storage-ha"; \
		$(GO) run ./integration/cmd/matrixctl list --profile storage-ha; \
		echo ""; \
		echo "profile=logical-slots"; \
		$(GO) run ./integration/cmd/matrixctl list --profile logical-slots; \
		echo ""; \
		echo "profile=merge"; \
		$(GO) run ./integration/cmd/matrixctl list --profile merge; \
		echo ""; \
		echo "profile=full"; \
		$(GO) run ./integration/cmd/matrixctl list --profile full; \
		echo ""; \
		echo "profile=soak"; \
		$(GO) run ./integration/cmd/matrixctl list --profile soak; \
	} > "$$out" 2>&1; \
	status=$$?; cat "$$out"; exit $$status

integration-profile-counts:
	@for profile in smoke storage-ha logical-slots merge-gate merge full soak; do \
		count="$$( $(GO) run ./integration/cmd/matrixctl list --profile $$profile 2>/dev/null | wc -l )"; \
		echo "$$profile=$$count"; \
	done

integration-profile-storage-ha-matrix:
	@for pg in 18 14; do \
		echo ">> running storage-ha profile for PG $$pg"; \
		$(MAKE) integration-profile-run PROFILE=storage-ha PG_MAJOR=$$pg || exit $$?; \
	done

integration-profile-logical-slots-matrix:
	@for pg in 18 14; do \
		echo ">> running logical-slots profile for PG $$pg"; \
		$(MAKE) integration-profile-run PROFILE=logical-slots PG_MAJOR=$$pg || exit $$?; \
	done

integration-profile-fast-matrix:
	@for profile in smoke storage-ha logical-slots; do \
		echo ">> running fast matrix profile $$profile on PG 18 + 14"; \
		for pg in 18 14; do \
			echo ">> running $$profile profile for PG $$pg"; \
			$(MAKE) integration-profile-run PROFILE=$$profile PG_MAJOR=$$pg || exit $$?; \
		done; \
	done

integration-profile-merge-gate-matrix:
	@for pg in 18 14; do \
		echo ">> running merge-gate profile for PG $$pg"; \
		$(MAKE) integration-profile-run PROFILE=merge-gate PG_MAJOR=$$pg || exit $$?; \
	done

integration-profile-merge-matrix:
	@for pg in 18 14; do \
		echo ">> running merge profile for PG $$pg"; \
		$(MAKE) integration-profile-run PROFILE=merge PG_MAJOR=$$pg || exit $$?; \
	done


integration-runtime-summary:
	@mkdir -p "$(INTEGRATION_ARTIFACTS_DIR)"
	@out="$(INTEGRATION_ARTIFACTS_DIR)/integration-runtime-summary-$(INTEGRATION_TIMESTAMP).txt"; \
	echo ">> writing runtime summary to $$out"; \
	{ \
		echo "profile runtime summary (latest logs)"; \
		echo "generated_at=$(INTEGRATION_TIMESTAMP)"; \
		echo ""; \
		for profile in smoke storage-ha logical-slots merge-gate merge full soak; do \
			for pg in 18 14; do \
				log_file="$$(ls -1t "$(INTEGRATION_ARTIFACTS_DIR)/integration-run-$${profile}-pg$${pg}-"*.log 2>/dev/null | head -n 1)"; \
				if [ -z "$$log_file" ]; then \
					echo "$$profile pg$$pg: no log"; \
					continue; \
				fi; \
				runtime_line="$$(grep -E '^ok[[:space:]]+github.com/woozymasta/hysteron/tests/integration[[:space:]]+[0-9.]+s$$' "$$log_file" | tail -n 1)"; \
				if [ -z "$$runtime_line" ]; then \
					echo "$$profile pg$$pg: no runtime line in $$(basename "$$log_file")"; \
					continue; \
				fi; \
				runtime_val="$$(echo "$$runtime_line" | awk '{print $$3}')"; \
				echo "$$profile pg$$pg: $$runtime_val ($$(basename "$$log_file"))"; \
			done; \
		done; \
	} > "$$out"; \
	cat "$$out"

integration-report-index:
	@mkdir -p "$(INTEGRATION_ARTIFACTS_DIR)"
	@out="$(INTEGRATION_ARTIFACTS_DIR)/INDEX.md"; \
	echo ">> writing report index to $$out"; \
	$(GO) run ./integration/cmd/reportctl index \
		--artifacts-dir "$(INTEGRATION_ARTIFACTS_DIR)" \
		--output "$$out"

integration-local:
	@mkdir -p "$(INTEGRATION_ARTIFACTS_DIR)"
	@log_file="$(INTEGRATION_ARTIFACTS_DIR)/integration-local-$(INTEGRATION_TIMESTAMP).log"; \
	echo ">> running local integration tests (non-container mode)"; \
	echo ">> writing local integration log to $$log_file"; \
	HYSTERON_TEST_STORE_BACKEND="$(INTEGRATION_STORE_BACKEND)" \
	HYSTERON_INTEGRATION_MAX_STORES="$(INTEGRATION_MAX_STORES)" \
	$(GO) test -tags "$(INTEGRATION_TAGS)" -timeout "$(INTEGRATION_TIMEOUT)" \
		-parallel "$(INTEGRATION_PARALLEL)" $(INTEGRATION_RUN_ARG) \
		$(INTEGRATION_TEST_ARGS) \
		-v -count 1 ./integration > "$$log_file" 2>&1; \
	status=$$?; cat "$$log_file"; exit $$status

integration-clean-artifacts:
	@echo ">> cleaning integration artifacts in $(INTEGRATION_ARTIFACTS_DIR)"
	@rm -rf "$(INTEGRATION_ARTIFACTS_DIR)"
	@mkdir -p "$(INTEGRATION_ARTIFACTS_DIR)"
