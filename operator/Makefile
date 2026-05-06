# AI-RAN Orchestrator Makefile

# Image URL to use all building/pushing image targets
IMG ?= ai-ran-orchestrator:latest
# ENVTEST_K8S_VERSION refers to the version of kubebuilder assets to be downloaded by envtest binary.
ENVTEST_K8S_VERSION = 1.29.0

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: lint
lint: ## Run golangci-lint against code.
	golangci-lint run ./...

.PHONY: test
test: fmt vet ## Run tests.
	go test ./... -coverprofile cover.out

.PHONY: test-e2e
test-e2e: ## Run end-to-end tests.
	go test ./test/e2e/... -v -timeout 30m

##@ Build

.PHONY: build
build: fmt vet ## Build orchestrator binary.
	go build -o bin/orchestrator ./cmd/orchestrator/main.go

.PHONY: run
run: fmt vet ## Run orchestrator from your host.
	go run ./cmd/orchestrator/main.go

.PHONY: docker-build
docker-build: ## Build docker image.
	docker build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image.
	docker push ${IMG}

##@ Deployment

.PHONY: install-crds
install-crds: ## Install CRDs into the K8s cluster.
	kubectl apply -f config/crd/bases/

.PHONY: uninstall-crds
uninstall-crds: ## Uninstall CRDs from the K8s cluster.
	kubectl delete -f config/crd/bases/

.PHONY: deploy
deploy: install-crds ## Deploy orchestrator to the K8s cluster.
	kubectl apply -f config/rbac/
	kubectl apply -f config/manager/

.PHONY: undeploy
undeploy: ## Undeploy orchestrator from the K8s cluster.
	kubectl delete -f config/manager/
	kubectl delete -f config/rbac/

.PHONY: deploy-samples
deploy-samples: ## Deploy sample workloads.
	kubectl apply -f config/samples/

##@ Proto

.PHONY: proto
proto: ## Generate protobuf code.
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/digitaltwin.proto
	protoc --proto_path=proto \
		--go_out=pkg/digitaltwin/ndtpb --go_opt=paths=source_relative \
		--go-grpc_out=pkg/digitaltwin/ndtpb --go-grpc_opt=paths=source_relative \
		proto/inference.proto

##@ Node Setup

.PHONY: label-ran-nodes
label-ran-nodes: ## Label nodes for RAN workloads (edit node names first).
	@echo "Label your RAN nodes with: kubectl label node <node-name> ran=true"
	@echo "Example: kubectl label node node-1 ran=true"

.PHONY: label-gpu-nodes
label-gpu-nodes: ## Label nodes with GPU.
	@echo "Label your GPU nodes with: kubectl label node <node-name> gpu=true computing=true"
	@echo "Example: kubectl label node node-3 gpu=true computing=true"

.PHONY: label-computing-nodes
label-computing-nodes: ## Label computing nodes.
	@echo "Label your computing nodes with: kubectl label node <node-name> computing=true"
	@echo "Example: kubectl label node node-4 computing=true"

##@ Local Development

.PHONY: local-run
local-run: ## Run orchestrator locally with simulated components.
	go run ./cmd/orchestrator/main.go \
		--digital-twin-address=localhost:50051 \
		--reconciliation-period=10s \
		-v=3

.PHONY: kind-create
kind-create: ## Create a kind cluster for local development.
	kind create cluster --name ai-ran-dev --config hack/kind-config.yaml

.PHONY: kind-delete
kind-delete: ## Delete the kind cluster.
	kind delete cluster --name ai-ran-dev

.PHONY: kind-load
kind-load: docker-build ## Load docker image into kind cluster.
	kind load docker-image ${IMG} --name ai-ran-dev
