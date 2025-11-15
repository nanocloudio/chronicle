.PHONY: help fmt fmt-check clippy lint build build-release test bench smoke run-sample verify ci clean harness-up harness-down integration-tests schema-validate

CARGO ?= cargo
CLIPPY_ARGS ?= -D warnings
BENCH_EXTRA ?= -- --warm-up-time 1 --measurement-time 3
CI_SCRIPT ?= tools/ci_checks.sh
CONFIG ?= tests/fixtures/chronicle-integration.yaml
HARNESS_CMD ?= e2e/harness.sh
HARNESS_FILE ?= e2e/docker-compose.yaml
HARNESS_PROJECT ?= chronicle-harness
INTEGRATION_TEST_ARGS ?=

help:
	@echo "chronicle make targets"
	@echo "  make build          # debug build with all optional features"
	@echo "  make build-release  # optimized build with all optional features"
	@echo "  make test           # full test suite (all features)"
	@echo "  make verify        # run fmt/clippy/tests/checks/audit scripts"
	@echo "  make bench          # run criterion bench for record_flow"
	@echo "  make smoke          # run the smoke test harness"
	@echo "  make fmt/fmt-check  # rustfmt (check mode available)"
	@echo "  make clippy|lint    # clippy with warnings as errors"
	@echo "  make run-sample     # run chronicle with the fixture YAML"
	@echo "  make ci             # run tools/ci_checks.sh"
	@echo "  make schema-validate # validate config YAML against docs/reference/chronicle.schema.json"
	@echo "  make harness-up     # start integration service harness"
	@echo "  make harness-down   # stop integration service harness"
	@echo "  make integration-tests [INTEGRATION_TEST_ARGS=...]"

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

clippy:
	$(CARGO) clippy --all-targets --all-features -- $(CLIPPY_ARGS)

lint: fmt-check clippy

build:
	$(CARGO) build --all-targets --features full

build-release:
	$(CARGO) build --all-targets --features full --release

test:
	$(CARGO) test --all --all-features

bench:
	$(CARGO) bench --bench record_flow $(BENCH_EXTRA)

smoke:
	$(CARGO) test --test smoke -- --nocapture

run-sample:
	$(CARGO) run -- --config $(CONFIG)

schema-validate:
	$(CARGO) run --quiet -- validate $(CONFIG)

verify:
	./tools/verify.sh

ci:
	$(CI_SCRIPT)

clean:
	$(CARGO) clean

harness-up:
	HARNESS_FILE=$(HARNESS_FILE) HARNESS_PROJECT=$(HARNESS_PROJECT) $(HARNESS_CMD) up

harness-down:
	HARNESS_FILE=$(HARNESS_FILE) HARNESS_PROJECT=$(HARNESS_PROJECT) $(HARNESS_CMD) down

integration-tests:
	HARNESS_FILE=$(HARNESS_FILE) HARNESS_PROJECT=$(HARNESS_PROJECT) $(HARNESS_CMD) test $(INTEGRATION_TEST_ARGS)
