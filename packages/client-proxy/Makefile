ENV := $(shell cat ../../.last_used_env || echo "not-set")
-include ../../.env.${ENV}

LOCAL_BUILD_TARGET ?= build-debug
PREFIX := $(strip $(subst ",,$(PREFIX)))
HOSTNAME := $(shell hostname 2> /dev/null || hostnamectl hostname 2> /dev/null)
$(if $(HOSTNAME),,$(error Failed to determine hostname: both 'hostname' and 'hostnamectl' failed))

# Volcengine configuration
VOLC_REGISTRY ?= bp-dockerhub-relay-cn-hongkong.cr.volces.com
VOLC_NAMESPACE ?= e2b
VOLC_IMAGE_PREFIX := $(VOLC_REGISTRY)/$(VOLC_NAMESPACE)
IMAGE_TAG ?= 2026.30.hugepage

# E2B-hosted registry that holds released, prebuilt client-proxy images.
# Published to by the release-please workflow on tagging a release.
E2B_ARTIFACTS_REGISTRY ?= us-docker.pkg.dev/e2b-artifacts/client-proxy/client-proxy

# Optional version "hook" for the released-image flow.
# When empty (default), build-and-upload builds from source and pushes to the
# client's own core repo (the existing flow, unchanged).
# When set (e.g. CLIENT_PROXY_VERSION=v0.1.0), skip building: pull the released
# image from $(E2B_ARTIFACTS_REGISTRY) and retag/push it into the client's core
# repo, so the Terraform `client-proxy:latest` lookup keeps working.
#
# Set it either on the command line (make ... CLIENT_PROXY_VERSION=v0.1.0) or in
# the active .env.${ENV} file (CLIENT_PROXY_VERSION=v0.1.0), which is included
# above. Quotes are stripped so both `=v0.1.0` and `="v0.1.0"` work; a
# command-line value always wins over the env file.
CLIENT_PROXY_VERSION := $(strip $(subst ",,$(CLIENT_PROXY_VERSION)))

VERSION_LDFLAGS :=
ifneq ($(strip $(VERSION)),)
VERSION_LDFLAGS := -X=main.version=$(VERSION)
endif

.PHONY: build
build:
	# Allow for passing commit sha directly for docker builds
	$(eval COMMIT_SHA ?= $(shell git rev-parse --short HEAD))
	CGO_ENABLED=0 GOOS=linux GOARCH=$(BUILD_ARCH) go build -o bin/client-proxy -ldflags "-X=main.commitSHA=$(COMMIT_SHA) $(VERSION_LDFLAGS)" .

# Compiler flags for debug builds. The default disables optimizations and
# inlining so binaries are debugger-friendly. CI overrides this with an empty
# value: race-instrumented but optimized binaries compile faster and speed up
# every integration test at runtime.
DEBUG_GCFLAGS ?= -gcflags=all="-N -l"

# Race instrumentation of the debug build, overridable (RACE_FLAG="") so CI
# can run some suite configurations against uninstrumented binaries.
RACE_FLAG ?= -race

.PHONY: build-debug
build-debug:
	$(eval COMMIT_SHA ?= $(shell git rev-parse --short HEAD))
	CGO_ENABLED=1 go build $(RACE_FLAG) $(DEBUG_GCFLAGS) -o bin/client-proxy -ldflags "-X=main.commitSHA=$(COMMIT_SHA) $(VERSION_LDFLAGS)" .

.PHONY: build-and-upload
build-and-upload:
	# Build from source and push to the client's own core repo (Volcengine).
	$(eval COMMIT_SHA := $(shell git rev-parse --short HEAD))
	@echo "Building and pushing to Volcengine: $(VOLC_IMAGE_PREFIX)/client-proxy:$(IMAGE_TAG)"
	@docker build \
		--tag "$(VOLC_IMAGE_PREFIX)/client-proxy:$(IMAGE_TAG)" \
		--build-arg COMMIT_SHA="$(COMMIT_SHA)" \
		-f ./Dockerfile ..
	@echo "Pushing image to Volcengine..."
	@docker push "$(VOLC_IMAGE_PREFIX)/client-proxy:$(IMAGE_TAG)"

.PHONY: run
run:
	make build-debug
	NODE_ID=integration-tests \
	NODE_IP=127.0.0.1 \
	./bin/client-proxy

define setup_local_env
	$(eval include .env.local)
	$(eval export $(shell sed 's/=.*//' .env.local))
endef

.PHONY: run-local
run-local: $(LOCAL_BUILD_TARGET)
	$(call setup_local_env)
	NODE_ID=$(HOSTNAME) ./bin/client-proxy

.PHONY: generate
generate:
	go generate ./...

.PHONY: test
test:
	go test -race -v ./...
