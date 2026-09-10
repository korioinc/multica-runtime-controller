.PHONY: build image image-push repository-validate runtime-version-test workflow-validate test test-race vet verify-core verify-local verify

RUNTIME_VERSIONS_FILE := build/runtime-versions.env
include $(RUNTIME_VERSIONS_FILE)

GO_MODULE_DIR := src

RUNTIME_VERSION_ARGS := GO_VERSION
RUNTIME_BUILD_ARGS := $(foreach name,$(RUNTIME_VERSION_ARGS),--build-arg $(name)=$($(name)))

HOST_ARCH := $(shell uname -m | sed -e 's/^x86_64$$/amd64/' -e 's/^aarch64$$/arm64/')
IMAGE ?= multica-runtime-controller:dev
PLATFORM ?= linux/$(HOST_ARCH)
PLATFORMS ?= linux/amd64,linux/arm64
VERSION ?= $(shell cat VERSION)
COMMIT ?= $(shell git rev-parse HEAD)

ACTIONLINT_VERSION := v1.7.12
# GitHub supports concurrency.queue; actionlint does not yet (rhysd/actionlint#657).
ACTIONLINT_FLAGS := -ignore '^unexpected key "queue" for "concurrency" section\.'
ACTIONLINT_WORKFLOWS := \
	../.github/workflows/create-develop-to-main-pr.yml \
	../.github/workflows/tag-version.yml \
	../.github/workflows/release.yml

build:
	mkdir -p bin
	go -C $(GO_MODULE_DIR) build -o ../bin/runtime ./cmd/runtime

image:
	docker buildx build --load \
		--platform $(PLATFORM) \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		$(RUNTIME_BUILD_ARGS) \
		--tag $(IMAGE) \
		.

image-push:
	docker buildx build --push \
		--platform $(PLATFORMS) \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		$(RUNTIME_BUILD_ARGS) \
		--tag $(IMAGE) \
		.

runtime-version-test:
	for script in scripts/*.sh scripts/lib/*.sh .github/scripts/*.sh; do bash -n "$$script" || exit 1; done
	shellcheck -x scripts/*.sh scripts/lib/*.sh .github/scripts/*.sh
	.github/scripts/verify-release.sh

repository-validate:
	./scripts/runtime-versions.sh --root "$(CURDIR)" validate

workflow-validate:
	go -C $(GO_MODULE_DIR) run github.com/rhysd/actionlint/cmd/actionlint@$(ACTIONLINT_VERSION) $(ACTIONLINT_FLAGS) $(ACTIONLINT_WORKFLOWS)

test:
	go -C $(GO_MODULE_DIR) test ./...

test-race:
	go -C $(GO_MODULE_DIR) test -race ./...

vet:
	go -C $(GO_MODULE_DIR) vet ./...

verify-core:
	./scripts/verify-local.sh --core-only

verify-local: verify-core

verify: runtime-version-test repository-validate workflow-validate test test-race vet verify-core
