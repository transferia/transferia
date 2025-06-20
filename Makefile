
# removes static build artifacts
.PHONY: clean
clean:
	@echo "--------------> Running 'make clean'"
	@rm -rf binaries tmp
	rm -f *.tgz

# Define the `build` target
API ?= trcli
BUILD_DATE ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

.PHONY: build
build:
	go build -buildvcs=true -ldflags="-X 'main.date=$(BUILD_DATE)'" -o binaries/$(API) ./cmd/trcli

docker: build
	cp binaries/$(API) . && docker build -t transfer

.PHONY: test
test:
	USE_TESTCONTAINERS=1 gotestsum --rerun-fails --format github-actions --packages="./cmd/..." -- -timeout=30m


# Define variables for the suite group, path, and name with defaults
SUITE_GROUP ?= 'tests/e2e'
SUITE_PATH ?= 'pg2pg'
SUITE_NAME ?= 'e2e-pg2pg'
SHELL := /bin/bash

# Define the `run-tests` target
.PHONY: run-tests
run-tests:
	@echo "Running $(SUITE_GROUP) suite $(SUITE_NAME)"
	@export RECIPE_CLICKHOUSE_BIN=clickhouse; \
	export USE_TESTCONTAINERS=1; \
	export YA_TEST_RUNNER=1; \
	export YT_PROXY=localhost:8180; \
	export TEST_DEPS_BINARY_PATH=binaries; \
	for dir in $$(find ./$(SUITE_GROUP)/$(SUITE_PATH) -type d); do \
	  if ls "$$dir"/*_test.go >/dev/null 2>&1; then \
	    echo "::group::$$dir"; \
	    echo "Running tests for directory: $$dir"; \
	    sanitized_dir=$$(echo "$$dir" | sed 's|/|_|g'); \
	    gotestsum \
	      --junitfile="reports/$(SUITE_NAME)_$$sanitized_dir.xml" \
	      --junitfile-project-name="$(SUITE_GROUP)" \
	      --junitfile-testsuite-name="short" \
	      --rerun-fails \
	      --format github-actions \
	      --packages="$$dir" \
	      -- -timeout=15m; \
	    echo "::endgroup::"; \
	  else \
	    echo "No Go test files found in $$dir, skipping tests."; \
	  fi \
	done

# Define variables
HELM_CHART_PATH := ./helm/transfer
IMAGE_NAME := ghcr.io/transferia/transferia-helm
VERSION := $(shell grep '^version:' $(HELM_CHART_PATH)/Chart.yaml | awk '{print $$2}')

# Login to GitHub Container Registry
.PHONY: login-ghcr
login-ghcr:
	echo "${GHCR_TOKEN}" | docker login ghcr.io -u ${GITHUB_USERNAME} --password-stdin

# Package the Helm chart
.PHONY: helm-package
helm-package:
	helm package $(HELM_CHART_PATH) --destination .

# Push the Helm chart as OCI artifact
.PHONY: helm-push
helm-push: helm-package login-ghcr
	helm push ./transfer-$(VERSION).tgz oci://$(IMAGE_NAME)
