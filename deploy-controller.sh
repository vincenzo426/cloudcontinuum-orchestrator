#!/bin/bash

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }

################################################################################
# Configurazione
################################################################################

DOCKER_USERNAME="${DOCKER_USERNAME:-}"
IMAGE_TAG="${IMAGE_TAG:-v1.0.0}"
CONTEXT="${CONTEXT:-cloud_cluster}"

if [ -z "$DOCKER_USERNAME" ]; then
    print_error "Devi settare DOCKER_USERNAME"
    echo "Esempio: export DOCKER_USERNAME=maxallegri"
    exit 1
fi

IMG="$DOCKER_USERNAME/cloudcontinuum-orchestrator:$IMAGE_TAG"

print_info "=========================================="
print_info "Deploy Cloud Continuum Orchestrator"
print_info "=========================================="
print_info "Image: $IMG"
print_info "Context: $CONTEXT"
echo ""

################################################################################
# Step 1: Build e Push
################################################################################

print_info "Step 1: Build Docker image"
make docker-build IMG=$IMG

print_info "Step 2: Push Docker image"
make docker-push IMG=$IMG

################################################################################
# Step 2: Genera manifest
################################################################################

print_info "Step 3: Genera manifest di installazione"
make build-installer IMG=$IMG

################################################################################
# Step 3: Installa CRD
################################################################################

print_info "Step 4: Installa CRD"
kubectl --context=$CONTEXT apply -f config/crd/bases/

################################################################################
# Step 4: Crea namespace e secret
################################################################################

print_info "Step 5: Crea namespace cloudcontinuum-system"
kubectl --context=$CONTEXT create namespace cloudcontinuum-system --dry-run=client -o yaml | \
    kubectl --context=$CONTEXT apply -f -

print_info "Step 6: Crea Secret multi-cluster"

if [ ! -d "$HOME/.kube/multicluster" ]; then
    print_error "Directory $HOME/.kube/multicluster non trovata"
    exit 1
fi

kubectl --context=$CONTEXT delete secret cluster-kubeconfigs -n cloudcontinuum-system --ignore-not-found

kubectl --context=$CONTEXT create secret generic cluster-kubeconfigs \
  --from-file=edge_cluster_1=$HOME/.kube/multicluster/edge-cluster-1.yaml \
  --from-file=edge_cluster_2=$HOME/.kube/multicluster/edge-cluster-2.yaml \
  --from-file=edge_cluster_3=$HOME/.kube/multicluster/edge-cluster-3.yaml \
  --from-file=cloud_cluster=$HOME/.kube/multicluster/cloud-cluster.yaml \
  -n cloudcontinuum-system

print_info "✓ Secret creato"

################################################################################
# Step 5: Deploy controller
################################################################################

print_info "Step 7: Deploy controller"
kubectl --context=$CONTEXT apply -f dist/install.yaml

################################################################################
# Step 6: Attendi deployment
################################################################################

print_info "Step 8: Attendo deployment..."
kubectl --context=$CONTEXT wait --for=condition=available --timeout=300s \
    deployment/cloudcontinuum-orchestrator-controller-manager \
    -n cloudcontinuum-orchestrator-system || print_warning "Timeout attesa deployment"

################################################################################
# Step 7: Verifica
################################################################################

print_info "Step 9: Verifica deployment"
kubectl --context=$CONTEXT get pods -n cloudcontinuum-orchestrator-system

echo ""
print_info "=========================================="
print_info "Deploy completato!"
print_info "=========================================="
echo ""
print_info "Monitora logs:"
echo "  kubectl --context=$CONTEXT logs -n cloudcontinuum-orchestrator-system -l control-plane=controller-manager -f"
echo ""
print_info "Test controller:"
echo "  kubectl --context=$CONTEXT apply -f examples/pipelineplacementrequest-configmap.yaml"
echo ""