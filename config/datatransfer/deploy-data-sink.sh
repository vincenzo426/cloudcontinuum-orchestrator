#!/bin/bash
#
# Deploy Data Sink Service to all clusters in the CloudContinuum environment
#
# This script deploys the data-sink service to all clusters, enabling
# simulated data transfer across the Submariner network.
#
# Prerequisites:
#   - kubectl configured with contexts for all clusters
#   - Submariner installed and configured on all clusters
#
# Usage:
#   ./deploy-data-sink.sh
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST_FILE="${SCRIPT_DIR}/data-sink.yaml"

# Define cluster contexts - adjust these to match your kubeconfig
CLUSTERS=(
    "cloud_cluster"
    "edge_cluster_1"
    "edge_cluster_2"
    "edge_cluster_3"
)

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if manifest exists
if [ ! -f "$MANIFEST_FILE" ]; then
    log_error "Manifest file not found: $MANIFEST_FILE"
    exit 1
fi

log_info "Starting Data Sink deployment to all clusters..."
echo ""

# Deploy to each cluster
for cluster in "${CLUSTERS[@]}"; do
    log_info "Deploying to ${cluster}..."
    
    # Check if context exists
    if ! kubectl config get-contexts "$cluster" &>/dev/null; then
        log_warn "Context '$cluster' not found, skipping..."
        continue
    fi
    
    # Apply the manifest
    if kubectl apply -f "$MANIFEST_FILE" --context="$cluster"; then
        log_info "Successfully deployed to ${cluster}"
    else
        log_error "Failed to deploy to ${cluster}"
    fi
    
    echo ""
done

log_info "Deployment complete!"
echo ""

# Verify deployments
log_info "Verifying deployments..."
echo ""

for cluster in "${CLUSTERS[@]}"; do
    if ! kubectl config get-contexts "$cluster" &>/dev/null; then
        continue
    fi
    
    echo "=== ${cluster} ==="
    kubectl get pods -n cloudcontinuum-system -l app=data-sink --context="$cluster" 2>/dev/null || echo "No pods found"
    kubectl get svc -n cloudcontinuum-system -l app=data-sink --context="$cluster" 2>/dev/null || echo "No service found"
    kubectl get serviceexports -n cloudcontinuum-system --context="$cluster" 2>/dev/null || echo "No ServiceExport found (Submariner may not be installed)"
    echo ""
done

log_info "To test connectivity between clusters, run:"
echo "  kubectl run test-transfer --rm -it --image=alpine:3.19 --context=edge_cluster_1 -- sh -c 'echo test | nc cloud-cluster.data-sink.cloudcontinuum-system.svc.clusterset.local 9999'"