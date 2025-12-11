#!/bin/bash

################################################################################
# Setup Token Kubeflow Dedicato - Guida Completa
################################################################################
# Questo script crea un token ServiceAccount dedicato per Kubeflow e configura
# il controller per usarlo invece del token ServiceAccount standard.
################################################################################

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

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
# VERIFICA PREREQUISITI
################################################################################

print_header "Verifica Prerequisiti"

# Verifica connessione al cluster
if ! kubectl --context=cloud_cluster cluster-info &>/dev/null; then
    print_error "Impossibile connettersi al cloud_cluster"
    exit 1
fi
print_success "Connesso al cloud_cluster"

# Verifica namespace kubeflow
if ! kubectl --context=cloud_cluster get namespace kubeflow &>/dev/null; then
    print_error "Namespace 'kubeflow' non trovato"
    exit 1
fi
print_success "Namespace kubeflow trovato"

# Verifica namespace orchestrator
if ! kubectl --context=cloud_cluster get namespace cloudcontinuum-orchestrator-system &>/dev/null; then
    print_error "Namespace 'cloudcontinuum-orchestrator-system' non trovato"
    exit 1
fi
print_success "Namespace orchestrator trovato"

################################################################################
# STEP 1: CREA SERVICEACCOUNT IN NAMESPACE KUBEFLOW
################################################################################

print_header "Step 1: Creazione ServiceAccount Kubeflow"

print_step "Creazione ServiceAccount 'kubeflow-api-client' in namespace kubeflow..."

kubectl --context=cloud_cluster create serviceaccount kubeflow-api-client \
    -n kubeflow \
    --dry-run=client -o yaml | kubectl --context=cloud_cluster apply -f -

print_success "ServiceAccount creato"

################################################################################
# STEP 2: ASSEGNA PERMESSI CLUSTER-ADMIN
################################################################################

print_header "Step 2: Assegnazione Permessi"

print_step "Creazione ClusterRoleBinding per kubeflow-api-client..."

kubectl --context=cloud_cluster create clusterrolebinding kubeflow-api-client-binding \
    --clusterrole=cluster-admin \
    --serviceaccount=kubeflow:kubeflow-api-client \
    --dry-run=client -o yaml | kubectl --context=cloud_cluster apply -f -

print_success "Permessi assegnati"

print_info "Il ServiceAccount ha ora accesso completo alle API Kubeflow"

################################################################################
# STEP 3: GENERA TOKEN LONG-LIVED
################################################################################

print_header "Step 3: Generazione Token"

print_step "Generazione token con validità 10 anni..."

TOKEN=$(kubectl --context=cloud_cluster create token kubeflow-api-client \
    -n kubeflow \
    --duration=87600h)

if [ -z "$TOKEN" ]; then
    print_error "Fallito generazione token"
    exit 1
fi

print_success "Token generato"
echo ""
print_info "Token (primi 80 caratteri):"
echo "  ${TOKEN:0:80}..."
echo ""

################################################################################
# STEP 4: SALVA TOKEN COME SECRET
################################################################################

print_header "Step 4: Creazione Secret con Token"

print_step "Eliminazione Secret esistente (se presente)..."
kubectl --context=cloud_cluster delete secret kubeflow-api-token \
    -n cloudcontinuum-orchestrator-system \
    --ignore-not-found

print_step "Creazione nuovo Secret 'kubeflow-api-token'..."
kubectl --context=cloud_cluster create secret generic kubeflow-api-token \
    --from-literal=token="$TOKEN" \
    -n cloudcontinuum-orchestrator-system

print_success "Secret creato nel namespace orchestrator"

# Verifica
print_step "Verifica Secret..."
kubectl --context=cloud_cluster get secret kubeflow-api-token \
    -n cloudcontinuum-orchestrator-system \
    -o jsonpath='{.data.token}' | base64 -d | head -c 50
echo ""
print_success "Secret verificato"

################################################################################
# STEP 5: CONFIGURA DEPLOYMENT PER MONTARE IL SECRET
################################################################################

print_header "Step 5: Configurazione Deployment"

print_step "Patch deployment per montare il token Kubeflow..."

# Crea il patch YAML
cat > /tmp/deployment-patch.yaml <<EOF
spec:
  template:
    spec:
      volumes:
      - name: kubeflow-token
        secret:
          secretName: kubeflow-api-token
          optional: false
      containers:
      - name: manager
        volumeMounts:
        - name: kubeflow-token
          mountPath: /var/run/secrets/kubeflow
          readOnly: true
EOF

print_info "Patch YAML creato:"
cat /tmp/deployment-patch.yaml

print_step "Applicazione patch al deployment..."

kubectl --context=cloud_cluster patch deployment \
    cloudcontinuum-orchestrator-controller-manager \
    -n cloudcontinuum-orchestrator-system \
    --patch-file /tmp/deployment-patch.yaml

print_success "Deployment configurato"

################################################################################
# STEP 6: ATTENDI RESTART POD
################################################################################

print_header "Step 6: Attesa Restart Pod"

print_step "Attesa rollout del deployment..."

kubectl --context=cloud_cluster rollout status deployment/cloudcontinuum-orchestrator-controller-manager \
    -n cloudcontinuum-orchestrator-system \
    --timeout=3m

print_success "Rollout completato"

################################################################################
# STEP 7: VERIFICA MONTAGGIO TOKEN
################################################################################

print_header "Step 7: Verifica Token Montato"

# Attendi che il pod sia pronto
sleep 5

POD_NAME=$(kubectl --context=cloud_cluster get pods \
    -n cloudcontinuum-orchestrator-system \
    -l control-plane=controller-manager \
    -o jsonpath='{.items[0].metadata.name}')

if [ -z "$POD_NAME" ]; then
    print_error "Pod non trovato"
    exit 1
fi

print_info "Pod trovato: $POD_NAME"

print_step "Verifica montaggio volume kubeflow-token..."

kubectl --context=cloud_cluster exec -n cloudcontinuum-orchestrator-system $POD_NAME -- \
    ls -la /var/run/secrets/kubeflow/

print_success "Volume montato correttamente"

print_step "Verifica contenuto token (primi 80 caratteri)..."

TOKEN_IN_POD=$(kubectl --context=cloud_cluster exec -n cloudcontinuum-orchestrator-system $POD_NAME -- \
    head -c 80 /var/run/secrets/kubeflow/token)

echo "  Token in pod: ${TOKEN_IN_POD}..."

if [ -n "$TOKEN_IN_POD" ]; then
    print_success "Token presente nel pod"
else
    print_error "Token non trovato nel pod"
    exit 1
fi

################################################################################
# COMPLETAMENTO
################################################################################

print_header "Setup Completato!"

echo ""
echo -e "${GREEN}✓ ServiceAccount creato in namespace kubeflow${NC}"
echo -e "${GREEN}✓ Token generato e salvato come Secret${NC}"
echo -e "${GREEN}✓ Deployment configurato per montare il token${NC}"
echo -e "${GREEN}✓ Pod riavviato con nuovo token${NC}"
echo ""

print_info "Il token è ora montato in: /var/run/secrets/kubeflow/token"
print_info "Il client.go lo leggerà automaticamente"

echo ""
print_header "Prossimi Passi"
echo ""
echo "1. Aggiorna client.go con la versione che legge da /var/run/secrets/kubeflow/token"
echo "   ${YELLOW}cp /mnt/user-data/outputs/client-with-serviceaccount-token.go internal/kubeflow/client.go${NC}"
echo ""
echo "2. Rebuild e rideploy l'immagine Docker:"
echo "   ${YELLOW}make build${NC}"
echo "   ${YELLOW}make docker-build IMG=your-username/cloudcontinuum-orchestrator:v1.0.5${NC}"
echo "   ${YELLOW}make docker-push IMG=your-username/cloudcontinuum-orchestrator:v1.0.5${NC}"
echo ""
echo "3. Update deployment:"
echo "   ${YELLOW}kubectl --context=cloud_cluster set image deployment/cloudcontinuum-orchestrator-controller-manager \\${NC}"
echo "   ${YELLOW}  -n cloudcontinuum-orchestrator-system \\${NC}"
echo "   ${YELLOW}  manager=your-username/cloudcontinuum-orchestrator:v1.0.5${NC}"
echo ""
echo "4. Testa con una PipelinePlacementRequest:"
echo "   ${YELLOW}kubectl apply -f examples/pipelineplacementrequest-inline.yaml${NC}"
echo ""

print_header "Fine Setup"