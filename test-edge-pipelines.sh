#!/bin/bash

################################################################################
# Test Script - Multiple PipelinePlacementRequest for Edge Clusters
################################################################################
# Descrizione: Crea 3 PPR per testare il placement su edge clusters
# Usage: ./test-edge-pipelines.sh
################################################################################

set -e

# Colori per output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Configurazione
CONTEXT="cloud_cluster"
NAMESPACE="kubeflow"
PIPELINE_DIR="./test-pipelines"
PIPELINE_FILE_EDGE_1="${PIPELINE_DIR}/pipeline-edge-1.yaml"
PIPELINE_FILE_EDGE_2="${PIPELINE_DIR}/pipeline-edge-2.yaml"
PIPELINE_FILE_EDGE_3="${PIPELINE_DIR}/pipeline-edge-3.yaml"

################################################################################
# Funzioni Utility
################################################################################

print_header() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
}

print_step() {
    echo -e "${GREEN}▶ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

################################################################################
# Step 1: Preparazione Directory e Pipeline
################################################################################

print_header "Step 1: Preparazione Pipeline"

# Crea directory se non esiste
if [ ! -d "${PIPELINE_DIR}" ]; then
    print_step "Creando directory ${PIPELINE_DIR}..."
    mkdir -p "${PIPELINE_DIR}"
    print_success "Directory creata"
else
    print_info "Directory ${PIPELINE_DIR} già esistente"
fi

################################################################################
# Step 2: Pulizia PPR Precedenti
################################################################################

print_header "Step 2: Pulizia Risorse Precedenti"

print_step "Eliminando eventuali PipelinePlacementRequest precedenti..."
kubectl --context="${CONTEXT}" delete pipelineplacementrequest \
    hello-edge1-request hello-edge2-request hello-edge3-request \
    -n "${NAMESPACE}" --ignore-not-found=true

print_success "Pulizia completata"

################################################################################
# Step 3: Creazione PipelinePlacementRequest per Edge 1
################################################################################

print_header "Step 3: PPR per Edge Cluster 1 (Data Locality)"

PPR_EDGE1="${PIPELINE_DIR}/ppr-edge1.yaml"

cat > "${PPR_EDGE1}" <<EOF
apiVersion: orchestrator.cloudcontinuum.io/v1alpha1
kind: PipelinePlacementRequest
metadata:
  name: hello-edge1-request
  namespace: ${NAMESPACE}
spec:
  pipelineName: "hello-edge-1"
  placementStrategy: "data-locality-pipeline"
  dataLocation: "edge_cluster_1"
  experimentName: "edge-testing"
  pipelineSource:
    type: inline
    inline: |
$(sed 's/^/      /' "${PIPELINE_FILE_EDGE_1}")
EOF

print_step "Applicando PPR per Edge 1..."
kubectl --context="${CONTEXT}" apply -f "${PPR_EDGE1}"
print_success "PPR Edge 1 creata con strategia: data-locality-pipeline → edge_cluster_1"

################################################################################
# Step 4: Creazione PipelinePlacementRequest per Edge 2
################################################################################

print_header "Step 4: PPR per Edge Cluster 2 (Data Locality)"

PPR_EDGE2="${PIPELINE_DIR}/ppr-edge2.yaml"

cat > "${PPR_EDGE2}" <<EOF
apiVersion: orchestrator.cloudcontinuum.io/v1alpha1
kind: PipelinePlacementRequest
metadata:
  name: hello-edge2-request
  namespace: ${NAMESPACE}
spec:
  pipelineName: "hello-edge-2"
  placementStrategy: "data-locality-pipeline"
  dataLocation: "edge_cluster_2"
  experimentName: "edge-testing"
  pipelineSource:
    type: inline
    inline: |
$(sed 's/^/      /' "${PIPELINE_FILE_EDGE_2}")
EOF

print_step "Applicando PPR per Edge 2..."
kubectl --context="${CONTEXT}" apply -f "${PPR_EDGE2}"
print_success "PPR Edge 2 creata con strategia: data-locality-pipeline → edge_cluster_2"

################################################################################
# Step 5: Creazione PipelinePlacementRequest per Edge 3
################################################################################

print_header "Step 5: PPR per Edge Cluster 3 (Simple Heuristic)"

PPR_EDGE3="${PIPELINE_DIR}/ppr-edge3.yaml"

cat > "${PPR_EDGE3}" <<EOF
apiVersion: orchestrator.cloudcontinuum.io/v1alpha1
kind: PipelinePlacementRequest
metadata:
  name: hello-edge3-request
  namespace: ${NAMESPACE}
spec:
  pipelineName: "hello-edge-3"
  placementStrategy: "simple-heuristic-pipeline"
  dataLocation: "edge_cluster_3"
  experimentName: "edge-testing"
  pipelineSource:
    type: inline
    inline: |
$(sed 's/^/      /' "${PIPELINE_FILE_EDGE_3}")
EOF

print_step "Applicando PPR per Edge 3..."
kubectl --context="${CONTEXT}" apply -f "${PPR_EDGE3}"
print_success "PPR Edge 3 creata con strategia: simple-heuristic-pipeline"

################################################################################
# Step 6: Verifica Creazione
################################################################################

print_header "Step 6: Verifica PipelinePlacementRequest"

echo -e "${CYAN}Attendere qualche secondo per il reconcile...${NC}"
sleep 5

print_step "Lista PipelinePlacementRequest create:"
kubectl --context="${CONTEXT}" get pipelineplacementrequest \
    -n "${NAMESPACE}" -o wide

################################################################################
# Step 7: Monitoring
################################################################################

print_header "Step 7: Comandi Utili per Monitoring"

echo -e "${YELLOW}Per monitorare lo status delle PPR:${NC}"
echo -e "  ${CYAN}kubectl --context=${CONTEXT} get pipelineplacementrequest -n ${NAMESPACE} -w${NC}"
echo ""
echo -e "${YELLOW}Per vedere i dettagli di una PPR specifica:${NC}"
echo -e "  ${CYAN}kubectl --context=${CONTEXT} describe pipelineplacementrequest hello-edge1-request -n ${NAMESPACE}${NC}"
echo -e "  ${CYAN}kubectl --context=${CONTEXT} describe pipelineplacementrequest hello-edge2-request -n ${NAMESPACE}${NC}"
echo -e "  ${CYAN}kubectl --context=${CONTEXT} describe pipelineplacementrequest hello-edge3-request -n ${NAMESPACE}${NC}"
echo ""
echo -e "${YELLOW}Per vedere lo status YAML completo:${NC}"
echo -e "  ${CYAN}kubectl --context=${CONTEXT} get pipelineplacementrequest hello-edge1-request -n ${NAMESPACE} -o yaml${NC}"
echo ""
echo -e "${YELLOW}Per vedere i workflow creati sui cluster edge:${NC}"
echo -e "  ${CYAN}kubectl --context=edge_cluster_1 get workflow -n kubeflow${NC}"
echo -e "  ${CYAN}kubectl --context=edge_cluster_2 get workflow -n kubeflow${NC}"
echo -e "  ${CYAN}kubectl --context=edge_cluster_3 get workflow -n kubeflow${NC}"
echo ""
echo -e "${YELLOW}Oppure usa il Makefile:${NC}"
echo -e "  ${CYAN}make ppr-list${NC}        # Lista PPR"
echo -e "  ${CYAN}make ppr-watch${NC}       # Monitoring in tempo reale"
echo -e "  ${CYAN}make wf-all${NC}          # Workflow su tutti i cluster"
echo ""

################################################################################
# Step 8: Riepilogo
################################################################################

print_header "Riepilogo Test"

echo -e "${GREEN}✓ Pipeline creata:${NC}         ${PIPELINE_FILE}"
echo ""
echo -e "${GREEN}✓ PPR Edge 1:${NC}              hello-edge1-request"
echo -e "  ${CYAN}Pipeline Name:${NC}           hello-world-edge1"
echo -e "  ${CYAN}Strategy:${NC}                data-locality-pipeline"
echo -e "  ${CYAN}Target Cluster:${NC}          edge_cluster_1"
echo ""
echo -e "${GREEN}✓ PPR Edge 2:${NC}              hello-edge2-request"
echo -e "  ${CYAN}Pipeline Name:${NC}           hello-world-edge2"
echo -e "  ${CYAN}Strategy:${NC}                data-locality-pipeline"
echo -e "  ${CYAN}Target Cluster:${NC}          edge_cluster_2"
echo ""
echo -e "${GREEN}✓ PPR Edge 3:${NC}              hello-edge3-request"
echo -e "  ${CYAN}Pipeline Name:${NC}           hello-world-edge3"
echo -e "  ${CYAN}Strategy:${NC}                simple-heuristic-pipeline"
echo -e "  ${CYAN}Target Cluster:${NC}          (automatico in base alle risorse)"
echo ""
echo -e "${CYAN}File generati in:${NC}          ${PIPELINE_DIR}/"
echo ""
echo -e "${YELLOW}Tip:${NC} Per vedere lo status in tempo reale:"
echo -e "  ${CYAN}watch -n 2 'kubectl --context=${CONTEXT} get pipelineplacementrequest -n ${NAMESPACE} -o wide'${NC}"
echo ""

print_success "Test completato con successo!"

echo ""
echo -e "${BLUE}════════════════════════════════════════════════════${NC}"
echo -e "${YELLOW}Prossimi passi:${NC}"
echo -e "  1. Monitora le PPR: ${CYAN}make ppr-watch${NC}"
echo -e "  2. Verifica i workflow: ${CYAN}make wf-all${NC}"
echo -e "  3. Controlla i log del controller: ${CYAN}make controller-logs${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════${NC}"
echo ""