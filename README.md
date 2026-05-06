# AI-RAN QoS-Aware Orchestrator

Multi-layer resource orchestrator for AI-RAN workloads with QoS-aware scaling and Digital Twin integration.

## Cluster Setup

```
Nodes:
  master       → control-plane (no workloads)
  edge         → ran=true, computing=true (RAN workloads)
  gpu-worker   → gpu=true, computing=true (AI workloads)
  ue-worker    → ran=false, computing=false, gpu=false (UE emulation only)

Workloads:
  srsran (HIGH QoS)     → srsran-gnb, srsue-1
  ricplt (MEDIUM QoS)   → e2mgr, e2term, a1mediator, submgr, rtmgr, appmgr
  ricxapp (MEDIUM QoS)  → anomaly-detector, resource-allocator, traffic-predictor
  ai-workloads (LOW)    → inference-standard-gpu, training-large-gpu
```

## Quick Start

```bash
# 1. Run the setup script 
chmod +x hack/setup-your-cluster.sh
./hack/setup-your-cluster.sh

# Select option 1 for full setup
```

## Manual Setup

```bash
# 1. Label nodes
kubectl label node edge ran=true computing=true
kubectl label node gpu-worker gpu=true computing=true

# 2. Install CRDs
kubectl apply -f config/crd/bases/

# 3. Deploy RBAC
kubectl apply -f config/rbac/role.yaml

# 4. Register your workloads
kubectl apply -f config/samples/your_cluster_workloads.yaml

# 5. Build
go mod tidy
go build -o bin/orchestrator ./cmd/orchestrator/main.go

# 6. Run
./bin/orchestrator --reconciliation-period=30s -v=2
```

## Scaling Rules

| Namespace | QoS | Scaling | Node |
|-----------|-----|---------|------|
| srsran | HIGH | Vertical only | edge (ran=true) |
| ricplt | MEDIUM | Vertical only | edge (ran=true) |
| ricxapp | MEDIUM | Vertical only | edge (ran=true) |
| ai-workloads | LOW | Horizontal only | gpu-worker |

## Behavior

### Reactive (RAN Degradation)
When srsran latency > 10ms or CPU > 85%:
- ai-workloads scaled DOWN automatically
- Resources freed for RAN

### Proactive (Traffic Prediction)
When Digital Twin predicts traffic peak:
- ai-workloads scaling DEFERRED
- Resumed after peak ends

## Directory Structure
```
ai-ran-orchestrator/
├── operator/
│   ├── cmd/orchestrator/main.go          # Entry point
│   ├── api/v1alpha1/                     # Kubernetes API types
│   ├── pkg/
│   │   ├── allocation/                   # Core algorithm
│   │   ├── controller/                   # K8s controller
│   │   ├── digitaltwin/                  # gRPC client
│   │   ├── executor/                     # Scaling executor
│   │   ├── metrics/                      # Prometheus client
│   │   └── mig/                          # GPU partitioning
│   ├── config/
│   │   ├── crd/bases/                    # CRD definitions
│   │   ├── rbac/                         # RBAC rules
│   │   ├── manager/                      # Deployment
│   │   └── samples/                      # Your workloads
│   ├── hack/
│   │   └── setup-your-cluster.sh         # Setup script
│   ├── Dockerfile
│   ├── Makefile
│   └── go.mod
├── network-digital-twin/
│   ├── model-training          # Entry point
│   ├── docker
│   │   ├── cell/                         # mock cell data-generator
│   │   ├── ndt-api/                      # NDT API-gateway
│   │   ├── ndt-model/                    # ndt 
│   │   └── docker-compose                # Scaling executor
│   ├── kubernetes
│   │   ├── allocation/                   # Core algorithm
│   │   ├── controller/                   # K8s controller
│   │   ├── digitaltwin/                  # gRPC client
│   │   ├── executor/                     # Scaling executor
│   │   ├── metrics/                      # Prometheus client
│   │   └── mig/                          # GPU partitioning
│   ├── config/
│   │   ├── crd/bases/                    # CRD definitions
│   │   ├── rbac/                         # RBAC rules
│   │   ├── manager/                      # Deployment
│   │   └── samples/                      # Your workloads
│   ├── hack/
│   │   └── setup-your-cluster.sh         # Setup script
│   ├── Dockerfile
│   ├── Makefile
│   └── go.mod

```

## Verification

```bash
# Check node labels
kubectl get nodes -L ran,gpu,computing

# Check registered workloads
kubectl get qosawareworkloads -A

# Check orchestrator logs
kubectl logs -f deployment/ai-ran-orchestrator -n airan-system
```
