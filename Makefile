RELEASE_MATRIX := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64

GO          ?= go
LINTER      ?= golangci-lint
ALIGNER     ?= betteralign
BENCHSTAT   ?= benchstat
VULNCHECK   ?= govulncheck
MOCKGEN     ?= mockgen
CRI         ?= docker
BENCH_COUNT ?= 6
BENCH_REF   ?= bench_baseline.txt
PG_MAJOR    ?= 18
PG_MATRIX   ?= 18 17 16 15 14
PG_MATRIX_CI ?= 18 14

CGO_ENABLED ?= 0
GOFLAGS     ?= -buildvcs=auto -trimpath
LDFLAGS     ?= -s -w
GOWORK      ?= off
GOFTAGS     ?= forceposix

DOC_BUILD        ?= 1
DOC_RENDER_STYLE ?= posix
DOC_COMMANDS_DIR ?= doc/commands

INTEGRATION_TAGS          ?= integration
INTEGRATION_TIMEOUT       ?= 30m
INTEGRATION_PARALLEL      ?= 4
INTEGRATION_MAX_STORES    ?= 8
INTEGRATION_RUN           ?=
INTEGRATION_TEST_ARGS     ?=
INTEGRATION_STORE_BACKEND ?= etcdv3
ETCD_BIN                  ?= etcd
INTEGRATION_ARTIFACTS_DIR ?= artifacts/integration
INTEGRATION_TIMESTAMP     ?= $(shell date -u +%Y%m%dT%H%M%SZ)
INTEGRATION_RUN_ARG       := $(if $(INTEGRATION_RUN),-run '$(INTEGRATION_RUN)',)

BINARY     ?= hysteron
PKG        ?= ./cmd/hysteron
OUTPUT_DIR ?= build
OUTPUT_ABS_DIR := $(abspath $(OUTPUT_DIR))

TEST_PACKAGES ?= $(shell GOWORK=off $(GO) list ./... | grep -v '/tests/integration$$')

NATIVE_GOOS      := $(shell GOWORK=off $(GO) env GOOS)
NATIVE_GOARCH    := $(shell GOWORK=off $(GO) env GOARCH)
NATIVE_EXTENSION := $(if $(filter $(NATIVE_GOOS),windows),.exe,)

RACE ?= 0
ifeq ($(RACE),1)
	EXTRA_BUILD_FLAGS := -race
endif

MODULE_PATH := $(shell GOWORK=$(GOWORK) $(GO) list -m 2>/dev/null)
VERSION := $(shell git describe --tags --abbrev=0 2>/dev/null || echo v0.0.0)
COMMIT  := $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
DATE    := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
URL     ?= https://$(MODULE_PATH)

LDFLAGS_PKG := $(MODULE_PATH)/internal/buildflags
LDFLAGS_X := \
	-X '$(LDFLAGS_PKG).Version=$(VERSION)' \
	-X '$(LDFLAGS_PKG).Commit=$(COMMIT)' \
	-X '$(LDFLAGS_PKG).Date=$(DATE)' \
	-X '$(LDFLAGS_PKG).URL=$(URL)'

.PHONY: \
	all clean \
	build generate cli-docs \
	release release-notes container-build

.PHONY: \
	check ci verify tidy tidy-check download \
	fmt fmt-check vet lint lint-fix align align-fix	vulncheck

.PHONY: \
	test test-race test-short \
	bench bench-fast bench-reset

.PHONY: \
	integration integration-compose integration-matrix \
	integration-matrix-ci integration-container \
	integration-check-matrix \
	integration-profile-run integration-profile-list \
	integration-baseline-inventory integration-profile-counts \
	integration-profile-fast-matrix integration-profile-storage-ha-matrix integration-profile-logical-slots-matrix integration-profile-merge-gate-matrix integration-profile-merge-matrix integration-runtime-summary \
	integration-report-index integration-clean-artifacts

.PHONY: \
	tools tools-ci \
	tool-golangci-lint tool-betteralign tool-benchstat tool-govulncheck tool-mockgen

all: build

check: verify vulncheck tidy fmt vet lint-fix align-fix integration-check-matrix test
ci: download tools-ci verify vulncheck tidy-check fmt-check vet lint align integration-check-matrix test

clean:
	rm -rf $(OUTPUT_DIR)

build: clean generate
	@mkdir -p $(OUTPUT_DIR)
	@out="$(OUTPUT_DIR)/$(BINARY)$(NATIVE_EXTENSION)"; \
	echo ">> building $$out"; \
	GOOS=$(NATIVE_GOOS) GOARCH=$(NATIVE_GOARCH) \
	GOWORK=$(GOWORK) CGO_ENABLED=$(CGO_ENABLED) \
	$(GO) build $(GOFLAGS) -ldflags="$(LDFLAGS) $(LDFLAGS_X)" \
		-tags "$(GOFTAGS)" $(EXTRA_BUILD_FLAGS) -o "$$out" "$(PKG)"
	@if [ "$(DOC_BUILD)" = "1" ]; then \
		$(MAKE) cli-docs; \
	fi

cli-docs:
	@mkdir -p $(DOC_COMMANDS_DIR)
	@bin="$(OUTPUT_DIR)/$(BINARY)$(NATIVE_EXTENSION)"; \
	out="$(DOC_COMMANDS_DIR)/hysteron.md"; \
	echo ">> generating $$out"; \
	"$$bin" docs md --style "$(DOC_RENDER_STYLE)" --template=list --toc --trim-descriptions "$$out"

release: clean
	@mkdir -p $(OUTPUT_DIR)
	@for target in $(RELEASE_MATRIX); do \
		goos=$${target%%/*}; \
		goarch=$${target##*/}; \
		ext=$$( [ "$$goos" = "windows" ] && echo ".exe" || echo "" ); \
		out="$(OUTPUT_DIR)/$(BINARY)-$${goos}-$${goarch}$$ext"; \
		echo ">> building $$out"; \
		GOOS=$$goos GOARCH=$$goarch \
		GOWORK=$(GOWORK) CGO_ENABLED=$(CGO_ENABLED) \
		$(GO) build $(GOFLAGS) -ldflags="$(LDFLAGS) $(LDFLAGS_X)" \
			-tags "$(GOFTAGS)" -o "$$out" "$(PKG)"; \
	done

generate:
	$(GO) generate ./...

verify:
	$(GO) mod verify

tidy:
	$(GO) mod tidy

tidy-check:
	@$(GO) mod tidy
	@git diff --stat --exit-code -- go.mod go.sum || ( \
		echo "go mod tidy: repository is not tidy"; \
		exit 1; \
	)

download:
	$(GO) mod download

fmt:
	gofmt -w .

fmt-check:
	@files=$$(gofmt -l .); \
	if [ -n "$$files" ]; then \
		echo "$$files" 1>&2; \
		echo "gofmt: files need formatting" 1>&2; \
		exit 1; \
	fi

vet:
	$(GO) vet ./...

lint:
	$(LINTER) run ./...

lint-fix:
	$(LINTER) run --fix ./...

align:
	$(ALIGNER) ./...

align-fix:
	-$(ALIGNER) -apply ./...
	$(ALIGNER) ./...

vulncheck:
	$(VULNCHECK) ./...

test:
	$(GO) test $(TEST_PACKAGES)

test-race:
	$(GO) test -race $(TEST_PACKAGES)

test-short:
	$(GO) test -short $(TEST_PACKAGES)

bench:
	@tmp=$$(mktemp); \
	$(GO) test $(TEST_PACKAGES) -run=^$$ -bench 'Benchmark' -benchmem -count=$(BENCH_COUNT) | tee "$$tmp"; \
	if [ -f "$(BENCH_REF)" ]; then \
		$(BENCHSTAT) "$(BENCH_REF)" "$$tmp"; \
	else \
		cp "$$tmp" "$(BENCH_REF)" && echo "Baseline saved to $(BENCH_REF)"; \
	fi; \
	rm -f "$$tmp"

bench-fast:
	$(GO) test $(TEST_PACKAGES) -run=^$$ -bench 'Benchmark' -benchmem

bench-reset:
	rm -f "$(BENCH_REF)"

integration: build
	@$(MAKE) -C tests integration-local

integration-compose:
	@$(MAKE) -C tests integration CRI="$(CRI)" PG_MAJOR="$(PG_MAJOR)"

integration-matrix:
	@$(MAKE) -C tests integration-matrix CRI="$(CRI)" PG_MATRIX="$(PG_MATRIX)"

integration-matrix-ci:
	@$(MAKE) -C tests integration-matrix-ci CRI="$(CRI)" PG_MATRIX_CI="$(PG_MATRIX_CI)"

integration-container:
	@$(MAKE) -C tests integration-container

integration-check-matrix:
	@$(MAKE) -C tests integration-check-matrix

integration-profile-run:
	@$(MAKE) -C tests integration-profile-run PROFILE="$(PROFILE)" PG_MAJOR="$(PG_MAJOR)"

integration-profile-list:
	@$(MAKE) -C tests integration-profile-list PROFILE="$(PROFILE)"

integration-profile-%:
	@$(MAKE) -C tests integration-profile-run PROFILE="$*" PG_MAJOR="$(PG_MAJOR)"

integration-baseline-inventory:
	@$(MAKE) -C tests integration-baseline-inventory

integration-profile-counts:
	@$(MAKE) -C tests integration-profile-counts

integration-profile-fast-matrix:
	@$(MAKE) -C tests integration-profile-fast-matrix

integration-profile-storage-ha-matrix:
	@$(MAKE) -C tests integration-profile-storage-ha-matrix

integration-profile-logical-slots-matrix:
	@$(MAKE) -C tests integration-profile-logical-slots-matrix

integration-profile-merge-gate-matrix:
	@$(MAKE) -C tests integration-profile-merge-gate-matrix

integration-profile-merge-matrix:
	@$(MAKE) -C tests integration-profile-merge-matrix

integration-runtime-summary:
	@$(MAKE) -C tests integration-runtime-summary

integration-report-index:
	@$(MAKE) -C tests integration-report-index INTEGRATION_ARTIFACTS_DIR="../$(INTEGRATION_ARTIFACTS_DIR)"

integration-clean-artifacts:
	@$(MAKE) -C tests integration-clean-artifacts INTEGRATION_ARTIFACTS_DIR="../$(INTEGRATION_ARTIFACTS_DIR)"

tools: tool-golangci-lint tool-betteralign tool-benchstat tool-govulncheck tool-mockgen
tools-ci: tool-golangci-lint tool-betteralign tool-govulncheck tool-mockgen

tool-golangci-lint:
	$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest

tool-betteralign:
	$(GO) install github.com/dkorunic/betteralign/cmd/betteralign@latest

tool-benchstat:
	$(GO) install golang.org/x/perf/cmd/benchstat@latest

tool-govulncheck:
	$(GO) install golang.org/x/vuln/cmd/govulncheck@latest

tool-mockgen:
	$(GO) install go.uber.org/mock/mockgen@latest

tool-cyclonedx:
	$(GO) install github.com/CycloneDX/cyclonedx-gomod/cmd/cyclonedx-gomod@latest

release-notes:
	@awk '\
	/^<!--/,/^-->/ { next } \
	/^## \[[0-9]+\.[0-9]+\.[0-9]+\]/ { if (found) exit; found=1; next } \
	found { \
		if (/^## \[/) { exit } \
		if (/^$$/) { flush(); print; next } \
		if (/^\* / || /^- /) { flush(); buf=$$0; next } \
		if (/^###/ || /^\[/) { flush(); print; next } \
		sub(/^[ \t]+/, ""); sub(/[ \t]+$$/, ""); \
		if (buf != "") { buf = buf " " $$0 } else { buf = $$0 } \
		next \
	} \
	function flush() { if (buf != "") { print buf; buf = "" } } \
	END { flush() } \
	' CHANGELOG.md

container-build:
	if [ -z "$${PGVERSION}" ]; then echo 'PGVERSION is undefined'; exit 1; fi; \
	if [ -z "$${TAG}" ]; then echo 'TAG is undefined'; exit 1; fi; \
	$(CRI) build --build-arg PGVERSION=$${PGVERSION} -t $${TAG} \
		-f examples/kubernetes/image/docker/Dockerfile .
