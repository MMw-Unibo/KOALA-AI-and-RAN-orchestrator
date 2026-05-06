#!/bin/bash
# AI-RAN Orchestrator Setup for YOUR RKE2 Cluster
# Your existing workloads will be managed by the orchestrator

set -e

echo "========================================="
echo "AI-RAN Orchestrator - Your Cluster Setup"
echo "========================================="

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Your cluster nodes, CHANGE WITH YOUR CLUSTER NODES
MASTER_NODE="master"       
RAN_NODE="edge"         # For srsran, ricplt, ricxapp
GPU_NODE="gpu-worker"   # For ai-workloads
UE_NODE="ue-worker


echo -e "\n${BLUE}Your Cluster:${NC}"
echo "  Master:  $MASTER_NODE (control-plane, no workloads)"
echo "  RAN:     $RAN_NODE (srsran, ricplt, ricxapp)"
echo "  GPU:     $GPU_NODE (ai-workloads)"
echo "  UE:      $UE_NODE (ue emulation)"

echo -e "\n${BLUE}Existing Workloads Detected:${NC}"
echo "  srsran:       srsran-gnb, srsue-1"
echo "  ricplt:       e2mgr, e2term, a1mediator, submgr, rtmgr, appmgr"
echo "  ricxapp:      anomaly-detector, resource-allocator, traffic-predictor"
echo "  ai-workloads: inference-standard-gpu, training-large-gpu"

# Step 1: Label nodes
label_nodes() {
    echo -e "\n${YELLOW}Step 1: Labeling nodes for orchestrator...${NC}"
    
    # RAN node - for HIGH and MEDIUM QoS workloads
    echo "  Labeling $RAN_NODE as RAN + Computing node..."
    kubectl label node $RAN_NODE ran=true --overwrite 2>/dev/null || true
    kubectl label node $RAN_NODE computing=true --overwrite 2>/dev/null || true
    
    # GPU node - for LOW QoS workloads
    echo "  Labeling $GPU_NODE as GPU + Computing node..."
    kubectl label node $GPU_NODE gpu=true --overwrite 2>/dev/null || true
    kubectl label node $GPU_NODE computing=true --overwrite 2>/dev/null || true
    
    echo -e "${GREEN}✓ Nodes labeled${NC}"
    
    # Verify
    echo -e "\n${YELLOW}Node labels:${NC}"
    kubectl get nodes -L ran,gpu,computing --no-headers | while read line; do
        echo "  $line"
    done
}

# Step 2: Create orchestrator namespace
create_namespace() {
    echo -e "\n${YELLOW}Step 2: Creating airan-system namespace...${NC}"
    kubectl create namespace airan-system --dry-run=client -o yaml | kubectl apply -f -
    echo -e "${GREEN}✓ Namespace ready${NC}"
}

# Step 3: Install CRDs
install_crds() {
    echo -e "\n${YELLOW}Step 3: Installing CRDs...${NC}"
    kubectl apply -f config/crd/bases/
    
    echo -e "${GREEN}✓ CRDs installed${NC}"
    kubectl get crd | grep airan
}

# Step 4: Deploy RBAC
deploy_rbac() {
    echo -e "\n${YELLOW}Step 4: Deploying RBAC...${NC}"
    kubectl apply -f config/rbac/role.yaml
    echo -e "${GREEN}✓ RBAC deployed${NC}"
}

# Step 5: Register existing workloads
register_workloads() {
    echo -e "\n${YELLOW}Step 5: Registering your existing workloads...${NC}"
    kubectl apply -f config/samples/your_cluster_workloads.yaml
    
    echo -e "${GREEN}✓ Workloads registered${NC}"
    
    echo -e "\n${YELLOW}Registered QoSAwareWorkloads:${NC}"
    kubectl get qosawareworkloads -A --no-headers | while read ns name rest; do
        echo "  $ns/$name"
    done
}

# Step 6: Build orchestrator
build_orchestrator() {
    echo -e "\n${YELLOW}Step 6: Building orchestrator...${NC}"
    
    if ! command -v go &> /dev/null; then
        echo -e "${RED}Go not installed!${NC}"
        echo "Install Go 1.21+: https://go.dev/doc/install"
        exit 1
    fi
    
    go mod tidy 2>/dev/null || true
    go build -o bin/orchestrator ./cmd/orchestrator/main.go
    
    echo -e "${GREEN}✓ Build complete: bin/orchestrator${NC}"
}

# Step 7: Run orchestrator
run_orchestrator() {
    echo -e "\n${YELLOW}Step 7: Starting orchestrator...${NC}"
    echo -e "${BLUE}Connecting to Prometheus at: http://prometheus-k8s.monitoring:9090${NC}"
    echo -e "${BLUE}Press Ctrl+C to stop${NC}\n"
    
    ./bin/orchestrator \
        --digital-twin-address=localhost:50051 \
        --reconciliation-period=30s \
        -v=2
}

# Verify setup
verify_setup() {
    echo -e "\n${YELLOW}=========================================${NC}"
    echo -e "${YELLOW}Setup Verification${NC}"
    echo -e "${YELLOW}=========================================${NC}"
    
    echo -e "\n${BLUE}1. Node Labels:${NC}"
    kubectl get nodes -L ran,gpu,computing
    
    echo -e "\n${BLUE}2. QoSAwareWorkloads by Priority:${NC}"
    echo -e "   ${RED}HIGH (srsran):${NC}"
    kubectl get qaw -n srsran --no-headers 2>/dev/null | sed 's/^/     /' || echo "     (none)"
    
    echo -e "   ${YELLOW}MEDIUM (ricplt):${NC}"
    kubectl get qaw -n ricplt --no-headers 2>/dev/null | sed 's/^/     /' || echo "     (none)"
    
    echo -e "   ${YELLOW}MEDIUM (ricxapp):${NC}"
    kubectl get qaw -n ricxapp --no-headers 2>/dev/null | sed 's/^/     /' || echo "     (none)"
    
    echo -e "   ${GREEN}LOW (ai-workloads):${NC}"
    kubectl get qaw -n ai-workloads --no-headers 2>/dev/null | sed 's/^/     /' || echo "     (none)"
    
    echo -e "\n${BLUE}3. Actual Running Pods:${NC}"
    echo "   srsran:"
    kubectl get pods -n srsran --no-headers 2>/dev/null | grep Running | sed 's/^/     /'
    echo "   ricplt:"
    kubectl get pods -n ricplt --no-headers 2>/dev/null | grep Running | sed 's/^/     /'
    echo "   ricxapp:"
    kubectl get pods -n ricxapp --no-headers 2>/dev/null | grep Running | sed 's/^/     /'
    echo "   ai-workloads:"
    kubectl get pods -n ai-workloads --no-headers 2>/dev/null | grep Running | sed 's/^/     /'
    
    echo -e "\n${BLUE}4. Prometheus (for metrics):${NC}"
    kubectl get svc -n monitoring | grep prometheus
}

# Show expected behavior
show_behavior() {
    echo -e "\n${YELLOW}=========================================${NC}"
    echo -e "${YELLOW}Expected Orchestrator Behavior${NC}"
    echo -e "${YELLOW}=========================================${NC}"
    
    echo -e "\n${BLUE}When RAN degradation detected (latency > SLA or CPU > 85%):${NC}"
    echo "  → ai-workloads will be scaled DOWN (horizontal)"
    echo "  → Resources freed for srsran, ricplt, ricxapp"
    
    echo -e "\n${BLUE}When Digital Twin predicts traffic peak:${NC}"
    echo "  → ai-workloads scaling will be DEFERRED"
    echo "  → Workloads resume after peak subsides"
    
    echo -e "\n${BLUE}Scaling Types by Namespace:${NC}"
    echo "  srsran:       VERTICAL only (CPU/memory adjustment)"
    echo "  ricplt:       VERTICAL only"
    echo "  ricxapp:      VERTICAL only"
    echo "  ai-workloads: HORIZONTAL only (replica adjustment)"
    
    echo -e "\n${BLUE}Node Placement:${NC}"
    echo "  srsran, ricplt, ricxapp → edge162.mmwunibo.it (ran=true)"
    echo "  ai-workloads            → smontebu-gpu-worker (gpu=true, computing=true)"
}

# Main menu
main() {
    echo -e "\n${YELLOW}Select option:${NC}"
    echo "1) Full setup (label nodes + install CRDs + register workloads + build + run)"
    echo "2) Label nodes only"
    echo "3) Install CRDs + RBAC only"
    echo "4) Register workloads only"
    echo "5) Build orchestrator only"
    echo "6) Run orchestrator"
    echo "7) Verify setup"
    echo "8) Show expected behavior"
    
    read -p "Enter option (1-8): " option
    
    case $option in
        1)
            label_nodes
            create_namespace
            install_crds
            deploy_rbac
            build_orchestrator
            register_workloads
            verify_setup
            show_behavior
            echo -e "\n${GREEN}Setup complete!${NC}"
            echo -e "Run orchestrator with: ${BLUE}./hack/setup-your-cluster.sh${NC} and select option 6"
            ;;
        2)
            label_nodes
            ;;
        3)
            create_namespace
            install_crds
            deploy_rbac
            ;;
        4)
            register_workloads
            ;;
        5)
            build_orchestrator
            ;;
        6)
            run_orchestrator
            ;;
        7)
            verify_setup
            ;;
        8)
            show_behavior
            ;;
        *)
            echo -e "${RED}Invalid option${NC}"
            exit 1
            ;;
    esac
}

main "$@"
