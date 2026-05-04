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

CGO_ENABLED ?= 0
GOFLAGS     ?= -buildvcs=auto -trimpath
LDFLAGS     ?= -s -w
GOWORK      ?= off
GOFTAGS     ?= forceposix
INTEGRATION_TAGS          ?= integration
INTEGRATION_TIMEOUT       ?= 20m
INTEGRATION_STORE_BACKEND ?= etcdv3
ETCD_BIN                  ?= etcd

BINARIES   := stolon-keeper stolon-sentinel stolon-proxy stolonctl
OUTPUT_DIR := build

TEST_PACKAGES ?= $(shell GOWORK=off $(GO) list ./... | grep -v '/tests/integration$$')

NATIVE_GOOS      := $(shell GOWORK=off $(GO) env GOOS)
NATIVE_GOARCH    := $(shell GOWORK=off $(GO) env GOARCH)
NATIVE_EXTENSION := $(if $(filter $(NATIVE_GOOS),windows),.exe,)

RACE ?= 0
ifeq ($(RACE),1)
	EXTRA_BUILD_FLAGS := -race
endif

VERSION := $(shell git describe --tags --abbrev=0 2>/dev/null || echo v0.0.0)
COMMIT  := $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
DATE    := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
URL     ?= https://$(MODULE_PATH)

LDFLAGS_X := \
	-X 'main.Version=$(VERSION)' \
	-X 'main.Commit=$(COMMIT)' \
	-X 'main._buildTime=$(DATE)' \
	-X 'main.URL=$(URL)'

.PHONY: all build release clean check ci verify tidy tidy-check download fmt \
	fmt-check vet lint lint-fix align align-fix test test-race test-short bench \
	bench-fast bench-reset integration integration-compose vulncheck tools tools-ci tool-golangci-lint \
	tool-betteralign tool-benchstat tool-vulncheck container-build

all: build

check: verify vulncheck tidy fmt vet lint-fix align-fix test
ci: download tools-ci verify vulncheck tidy-check fmt-check vet lint align test

clean:
	rm -rf $(OUTPUT_DIR)

build: clean
	@mkdir -p $(OUTPUT_DIR)
	@for binary in $(BINARIES); do \
		case "$$binary" in \
			stolon-keeper) pkg="./cmd/keeper" ;; \
			stolon-sentinel) pkg="./cmd/sentinel" ;; \
			stolon-proxy) pkg="./cmd/proxy" ;; \
			stolonctl) pkg="./cmd/stolonctl" ;; \
			*) echo "unknown binary $$binary" >&2; exit 1 ;; \
		esac; \
		out="$(OUTPUT_DIR)/$$binary$(NATIVE_EXTENSION)"; \
		echo ">> building $$out"; \
		GOOS=$(NATIVE_GOOS) GOARCH=$(NATIVE_GOARCH) \
		GOWORK=$(GOWORK) CGO_ENABLED=$(CGO_ENABLED) \
		$(GO) build $(GOFLAGS) -ldflags="$(LDFLAGS) $(LDFLAGS_X)" \
			-tags "$(GOFTAGS)" $(EXTRA_BUILD_FLAGS) -o "$$out" "$$pkg"; \
	done

release: clean
	@mkdir -p $(OUTPUT_DIR)
	@for target in $(RELEASE_MATRIX); do \
		goos=$${target%%/*}; \
		goarch=$${target##*/}; \
		ext=$$( [ "$$goos" = "windows" ] && echo ".exe" || echo "" ); \
		for binary in $(BINARIES); do \
			case "$$binary" in \
				stolon-keeper) pkg="./cmd/keeper" ;; \
				stolon-sentinel) pkg="./cmd/sentinel" ;; \
				stolon-proxy) pkg="./cmd/proxy" ;; \
				stolonctl) pkg="./cmd/stolonctl" ;; \
				*) echo "unknown binary $$binary" >&2; exit 1 ;; \
			esac; \
			out="$(OUTPUT_DIR)/$$binary-$${goos}-$${goarch}$$ext"; \
			echo ">> building $$out"; \
			GOOS=$$goos GOARCH=$$goarch \
			GOWORK=$(GOWORK) CGO_ENABLED=$(CGO_ENABLED) \
			$(GO) build $(GOFLAGS) -ldflags="$(LDFLAGS) $(LDFLAGS_X)" \
				-tags "$(GOFTAGS)" -o "$$out" "$$pkg"; \
		done; \
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
	STOLON_TEST_STORE_BACKEND="$(INTEGRATION_STORE_BACKEND)" \
	ETCD_BIN="$(ETCD_BIN)" \
	STKEEPER_BIN="$(OUTPUT_DIR)/stolon-keeper$(NATIVE_EXTENSION)" \
	STSENTINEL_BIN="$(OUTPUT_DIR)/stolon-sentinel$(NATIVE_EXTENSION)" \
	STPROXY_BIN="$(OUTPUT_DIR)/stolon-proxy$(NATIVE_EXTENSION)" \
	STCTL_BIN="$(OUTPUT_DIR)/stolonctl$(NATIVE_EXTENSION)" \
	$(GO) test -tags "$(INTEGRATION_TAGS)" -timeout "$(INTEGRATION_TIMEOUT)" \
		-v -count 1 ./tests/integration

integration-compose:
	PG_MAJOR="$(PG_MAJOR)" $(CRI) compose -f tests/integration/compose.yml \
		run --rm integration

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
