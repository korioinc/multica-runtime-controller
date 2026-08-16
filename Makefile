.PHONY: build image image-push runtime-version-test workflow-validate test test-race vet helm-lint helm-template helm-validate verify

RUNTIME_VERSIONS_FILE := build/runtime-versions.env
include $(RUNTIME_VERSIONS_FILE)

GO_MODULE_DIR := src

RUNTIME_VERSION_ARGS := \
	NODE_VERSION PHP_VERSION GO_VERSION COMPOSER_VERSION \
	MONGODB_PHP_EXTENSION_VERSION PHPREDIS_VERSION ZSTD_PHP_EXTENSION_VERSION \
	GH_VERSION K9S_VERSION KUBECTX_VERSION KUBECTL_VERSION \
	AWS_CLI_VERSION OCI_CLI_VERSION GCLOUD_CLI_VERSION \
	UV_VERSION YQ_VERSION SHFMT_VERSION COREPACK_VERSION \
	MULTICA_CLI_VERSION \
	CODEX_VERSION COPILOT_VERSION PI_VERSION \
	ANTIGRAVITY_VERSION
RUNTIME_BUILD_ARGS := $(foreach name,$(RUNTIME_VERSION_ARGS),--build-arg $(name)=$($(name)))

HOST_ARCH := $(shell uname -m | sed -e 's/^x86_64$$/amd64/' -e 's/^aarch64$$/arm64/')
IMAGE ?= multica-runtime-controller:dev
PLATFORM ?= linux/$(HOST_ARCH)
PLATFORMS ?= linux/amd64,linux/arm64
VERSION ?= dev
COMMIT ?= $(shell git rev-parse --short=12 HEAD)

CHART := deploy/helm/multica-runtime-controller
RENDERED := /tmp/multica-runtime-controller-rendered.yaml
ACTIONLINT_VERSION := v1.7.12
ACTIONLINT_WORKFLOWS := \
	../.github/workflows/ci.yml \
	../.github/workflows/create-develop-to-main-pr.yml \
	../.github/workflows/develop-image.yml \
	../.github/workflows/release.yml \
	../.github/workflows/release-repair.yml \
	../.github/workflows/release-repair-guard.yml \
	../.github/workflows/runtime-version-update.yml \
	../.github/workflows/runtime-version-auto-merge.yml

build:
	go -C $(GO_MODULE_DIR) build ./cmd/runtime ./cmd/provider-shim

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
	python3 -m unittest -v scripts.tests.test_runtime_versions

workflow-validate:
	go -C $(GO_MODULE_DIR) run github.com/rhysd/actionlint/cmd/actionlint@$(ACTIONLINT_VERSION) $(ACTIONLINT_WORKFLOWS)
	python3 scripts/runtime_versions.py validate-actions .github/workflows

test:
	go -C $(GO_MODULE_DIR) test ./...

test-race:
	go -C $(GO_MODULE_DIR) test -race ./...

vet:
	go -C $(GO_MODULE_DIR) vet ./...

helm-lint:
	helm lint $(CHART) -f $(CHART)/ci/values-single-replica.yaml

helm-template:
	helm template multica-runtime-controller $(CHART) -f $(CHART)/ci/values-single-replica.yaml > $(RENDERED)

helm-validate: helm-template
	go -C $(GO_MODULE_DIR) run github.com/yannh/kubeconform/cmd/kubeconform@v0.7.0 -strict -summary -kubernetes-version 1.36.0 $(RENDERED)

verify: runtime-version-test workflow-validate test test-race vet helm-lint helm-validate
