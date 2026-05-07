RELEASE_MATRIX := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64

GO          ?= go
LINTER      ?= golangci-lint
ALIGNER     ?= betteralign
BENCHSTAT   ?= benchstat
VULNCHECK   ?= govulncheck
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
INTEGRATION_TIMEOUT       ?= 20m
INTEGRATION_PARALLEL      ?= 16
INTEGRATION_MAX_STORES    ?= 8
INTEGRATION_RUN           ?=
INTEGRATION_TEST_ARGS     ?=
INTEGRATION_STORE_BACKEND ?= etcdv3
ETCD_BIN                  ?= etcd
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

.PHONY: all build cli-docs release clean check ci verify tidy tidy-check download fmt \
	fmt-check vet lint lint-fix align align-fix test test-race test-short bench \
	bench-fast bench-reset integration integration-compose integration-matrix integration-matrix-ci vulncheck tools tools-ci tool-golangci-lint \
	tool-betteralign tool-benchstat tool-vulncheck container-build

all: build

check: verify vulncheck tidy fmt vet lint-fix align-fix test
ci: download tools-ci verify vulncheck tidy-check fmt-check vet lint align test

clean:
	rm -rf $(OUTPUT_DIR)

build: clean
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
	"$$bin" docs md --style "$(DOC_RENDER_STYLE)" "$$out"

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
	HYSTERON_TEST_STORE_BACKEND="$(INTEGRATION_STORE_BACKEND)" \
	HYSTERON_INTEGRATION_MAX_STORES="$(INTEGRATION_MAX_STORES)" \
	ETCD_BIN="$(ETCD_BIN)" \
	HYSTERON_BIN="$(OUTPUT_ABS_DIR)/$(BINARY)$(NATIVE_EXTENSION)" \
	$(GO) test -tags "$(INTEGRATION_TAGS)" -timeout "$(INTEGRATION_TIMEOUT)" \
		-parallel "$(INTEGRATION_PARALLEL)" $(INTEGRATION_RUN_ARG) \
		$(INTEGRATION_TEST_ARGS) \
		-v -count 1 ./tests/integration

integration-compose:
	PG_MAJOR="$(PG_MAJOR)" $(CRI) compose -f tests/integration/compose.yml \
		run --rm integration

integration-matrix:
	@for pg in $(PG_MATRIX); do \
		echo ">> running integration compose matrix for PostgreSQL $$pg"; \
		PG_MAJOR="$$pg" $(CRI) compose -f tests/integration/compose.yml \
			run --rm integration || exit $$?; \
	done

integration-matrix-ci:
	@for pg in $(PG_MATRIX_CI); do \
		echo ">> running reduced integration compose matrix for PostgreSQL $$pg"; \
		PG_MAJOR="$$pg" $(CRI) compose -f tests/integration/compose.yml \
			run --rm integration || exit $$?; \
	done

tools: tool-golangci-lint tool-betteralign tool-benchstat tool-govulncheck
tools-ci: tool-golangci-lint tool-betteralign tool-govulncheck

tool-golangci-lint:
	$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest

tool-betteralign:
	$(GO) install github.com/dkorunic/betteralign/cmd/betteralign@latest

tool-benchstat:
	$(GO) install golang.org/x/perf/cmd/benchstat@latest

tool-govulncheck:
	$(GO) install golang.org/x/vuln/cmd/govulncheck@latest

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

