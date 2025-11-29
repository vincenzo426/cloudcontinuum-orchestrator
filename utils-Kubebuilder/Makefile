# ============================================================================
# Cloud Continuum Orchestrator - Helper Makefile
# ============================================================================
# Comandi utili per sviluppo e testing
# Usa: make -f Makefile.helper <target>
# ============================================================================

.PHONY: help
.DEFAULT_GOAL := help

# Colori per output
GREEN  := \033[0;32m
YELLOW := \033[1;33m
RED    := \033[0;31m
NC     := \033[0m # No Color

# ============================================================================
# SVILUPPO
# ============================================================================

.PHONY: dev-setup
dev-setup: ## Setup completo ambiente di sviluppo
	@echo "$(GREEN)📦 Setup ambiente di sviluppo...$(NC)"
	@go mod tidy
	@go mod download
	@make manifests
	@make generate
	@echo "$(GREEN)✓ Setup completato$(NC)"

.PHONY: build
build: ## Compila il controller
	@echo "$(GREEN)🔨 Compilazione controller...$(NC)"
	@make -f Makefile build
	@echo "$(GREEN)✓ Build completata$(NC)"

.PHONY: run
run: ## Avvia il controller in locale
	@echo "$(GREEN)🚀 Avvio controller locale...$(NC)"
	@echo "$(YELLOW)⚠️  Premi CTRL+C per fermare$(NC)"
	@make -f Makefile run

.PHONY: fmt
fmt: ## Formatta il codice Go
	@echo "$(GREEN)📝 Formattazione codice...$(NC)"
	@go fmt ./...
	@echo "$(GREEN)✓ Formattazione completata$(NC)"

.PHONY: vet
vet: ## Esegue go vet
	@echo "$(GREEN)🔍 Analisi codice...$(NC)"
	@go vet ./...
	@echo "$(GREEN)✓ Analisi completata$(NC)"

.PHONY: manifests
manifests: ## Genera i manifesti CRD
	@echo "$(GREEN)📄 Generazione manifesti...$(NC)"
	@make -f Makefile manifests
	@echo "$(GREEN)✓ Manifesti generati$(NC)"

# ============================================================================
# DEPLOYMENT
# ============================================================================

.PHONY: install-crd
install-crd: ## Installa i CRD sul cluster
	@echo "$(GREEN)📥 Installazione CRD...$(NC)"
	@make -f Makefile install
	@kubectl get crd placementrequests.orchestrator.cloudcontinuum.io
	@echo "$(GREEN)✓ CRD installato$(NC)"

.PHONY: uninstall-crd
uninstall-crd: ## Rimuove i CRD dal cluster
	@echo "$(YELLOW)🗑️  Rimozione CRD...$(NC)"
	@make -f Makefile uninstall
	@echo "$(GREEN)✓ CRD rimosso$(NC)"

.PHONY: reinstall-crd
reinstall-crd: uninstall-crd install-crd ## Reinstalla i CRD

.PHONY: setup-secrets
setup-secrets: ## Crea i Secret con i kubeconfig multi-cluster
	@echo "$(GREEN)🔐 Setup Secret multi-cluster...$(NC)"
	@kubectl create namespace cloudcontinuum-system --dry-run=client -o yaml | kubectl apply -f -
	@kubectl delete secret cluster-kubeconfigs -n cloudcontinuum-system --ignore-not-found
	@mkdir -p kubeconfig-secrets
	@cp ~/.kube/multicluster/edge-cluster-1.yaml kubeconfig-secrets/edge_cluster_1
	@cp ~/.kube/multicluster/edge-cluster-2.yaml kubeconfig-secrets/edge_cluster_2
	@cp ~/.kube/multicluster/cloud-cluster.yaml kubeconfig-secrets/cloud_cluster
	@kubectl create secret generic cluster-kubeconfigs \
		--from-file=edge_cluster_1=kubeconfig-secrets/edge_cluster_1 \
		--from-file=edge_cluster_2=kubeconfig-secrets/edge_cluster_2 \
		--from-file=cloud_cluster=kubeconfig-secrets/cloud_cluster \
		-n cloudcontinuum-system
	@echo "$(GREEN)✓ Secret creato$(NC)"
	@kubectl get secret cluster-kubeconfigs -n cloudcontinuum-system

# ============================================================================
# TESTING
# ============================================================================

.PHONY: test-cloud
test-cloud: ## Testa strategia cloud-only
	@echo "$(GREEN)🧪 Test strategia cloud-only...$(NC)"
	@kubectl apply -f - <<EOF
	apiVersion: orchestrator.cloudcontinuum.io/v1alpha1
	kind: PlacementRequest
	metadata:
	  name: test-cloud
	  namespace: default
	spec:
	  workloadType: training
	  placementStrategy: cloud-only
	  resourceRequirements:
	    cpu: "100m"
	    memory: "256Mi"
	  podSpec:
	    image: nginx:latest
	    command: ["sleep"]
	    args: ["3600"]
	EOF
	@sleep 3
	@kubectl get placementrequest test-cloud

.PHONY: test-edge1
test-edge1: ## Testa strategia data-locality su edge1
	@echo "$(GREEN)🧪 Test strategia data-locality (edge1)...$(NC)"
	@kubectl apply -f - <<EOF
	apiVersion: orchestrator.cloudcontinuum.io/v1alpha1
	kind: PlacementRequest
	metadata:
	  name: test-edge1
	  namespace: default
	spec:
	  workloadType: preprocessing
	  placementStrategy: data-locality
	  dataLocation: edge_cluster_1
	  resourceRequirements:
	    cpu: "100m"
	    memory: "256Mi"
	  podSpec:
	    image: nginx:latest
	    command: ["sleep"]
	    args: ["3600"]
	EOF
	@sleep 3
	@kubectl get placementrequest test-edge1

.PHONY: test-edge2
test-edge2: ## Testa strategia data-locality su edge2
	@echo "$(GREEN)🧪 Test strategia data-locality (edge2)...$(NC)"
	@kubectl apply -f - <<EOF
	apiVersion: orchestrator.cloudcontinuum.io/v1alpha1
	kind: PlacementRequest
	metadata:
	  name: test-edge2
	  namespace: default
	spec:
	  workloadType: inference
	  placementStrategy: data-locality
	  dataLocation: edge_cluster_2
	  resourceRequirements:
	    cpu: "100m"
	    memory: "256Mi"
	  podSpec:
	    image: nginx:latest
	    command: ["sleep"]
	    args: ["3600"]
	EOF
	@sleep 3
	@kubectl get placementrequest test-edge2

.PHONY: test-heuristic
test-heuristic: ## Testa strategia simple-heuristic
	@echo "$(GREEN)🧪 Test strategia simple-heuristic...$(NC)"
	@kubectl apply -f - <<EOF
	apiVersion: orchestrator.cloudcontinuum.io/v1alpha1
	kind: PlacementRequest
	metadata:
	  name: test-heuristic
	  namespace: default
	spec:
	  workloadType: training
	  placementStrategy: simple-heuristic
	  dataLocation: edge_cluster_1
	  resourceRequirements:
	    cpu: "100m"
	    memory: "256Mi"
	  podSpec:
	    image: nginx:latest
	    command: ["sleep"]
	    args: ["3600"]
	EOF
	@sleep 3
	@kubectl get placementrequest test-heuristic

.PHONY: test-all
test-all: test-cloud test-edge1 test-edge2 test-heuristic ## Esegue tutti i test

# ============================================================================
# MONITORING
# ============================================================================

.PHONY: status
status: ## Mostra stato PlacementRequest
	@echo "$(GREEN)📊 Stato PlacementRequest:$(NC)"
	@kubectl get placementrequests -o custom-columns=NAME:.metadata.name,STRATEGY:.spec.placementStrategy,TARGET:.status.targetCluster,STATUS:.status.conditions[0].status,AGE:.metadata.creationTimestamp

.PHONY: pods-cloud
pods-cloud: ## Mostra pod su cloud_cluster
	@echo "$(GREEN)☁️  Pod su cloud_cluster:$(NC)"
	@kubectl get pods --context=cloud_cluster -n default -o wide

.PHONY: pods-edge1
pods-edge1: ## Mostra pod su edge_cluster_1
	@echo "$(GREEN)🏭 Pod su edge_cluster_1:$(NC)"
	@kubectl get pods --context=edge_cluster_1 -n default -o wide

.PHONY: pods-edge2
pods-edge2: ## Mostra pod su edge_cluster_2
	@echo "$(GREEN)🏭 Pod su edge_cluster_2:$(NC)"
	@kubectl get pods --context=edge_cluster_2 -n default -o wide

.PHONY: pods-all
pods-all: pods-cloud pods-edge1 pods-edge2 ## Mostra pod su tutti i cluster

.PHONY: describe
describe: ## Descrive tutti i PlacementRequest (usage: make describe)
	@echo "$(GREEN)📋 Dettaglio PlacementRequest:$(NC)"
	@for pr in $$(kubectl get placementrequest -o name); do \
		echo "$(YELLOW)========================================$(NC)"; \
		kubectl describe $$pr | grep -A 20 "Status:"; \
	done

.PHONY: watch
watch: ## Watch PlacementRequest in real-time
	@watch -n 2 'kubectl get placementrequests -o custom-columns=NAME:.metadata.name,STRATEGY:.spec.placementStrategy,TARGET:.status.targetCluster,STATUS:.status.conditions[0].status'

# ============================================================================
# CLEANUP
# ============================================================================

.PHONY: clean-tests
clean-tests: ## Cancella tutti i test PlacementRequest
	@echo "$(YELLOW)🗑️  Pulizia test...$(NC)"
	@kubectl delete placementrequest test-cloud --ignore-not-found
	@kubectl delete placementrequest test-edge1 --ignore-not-found
	@kubectl delete placementrequest test-edge2 --ignore-not-found
	@kubectl delete placementrequest test-heuristic --ignore-not-found
	@echo "$(GREEN)✓ Test puliti$(NC)"

.PHONY: clean-pods-cloud
clean-pods-cloud: ## Cancella pod su cloud_cluster
	@echo "$(YELLOW)🗑️  Pulizia pod cloud...$(NC)"
	@kubectl delete pods --context=cloud_cluster -n default -l placement-request --ignore-not-found
	@echo "$(GREEN)✓ Pod cloud puliti$(NC)"

.PHONY: clean-pods-edge1
clean-pods-edge1: ## Cancella pod su edge_cluster_1
	@echo "$(YELLOW)🗑️  Pulizia pod edge1...$(NC)"
	@kubectl delete pods --context=edge_cluster_1 -n default -l placement-request --ignore-not-found
	@echo "$(GREEN)✓ Pod edge1 puliti$(NC)"

.PHONY: clean-pods-edge2
clean-pods-edge2: ## Cancella pod su edge_cluster_2
	@echo "$(YELLOW)🗑️  Pulizia pod edge2...$(NC)"
	@kubectl delete pods --context=edge_cluster_2 -n default -l placement-request --ignore-not-found
	@echo "$(GREEN)✓ Pod edge2 puliti$(NC)"

.PHONY: clean-pods-all
clean-pods-all: clean-pods-cloud clean-pods-edge1 clean-pods-edge2 ## Cancella pod su tutti i cluster

.PHONY: clean-all
clean-all: clean-tests clean-pods-all ## Pulizia completa di test e pod

# ============================================================================
# DEBUG
# ============================================================================

.PHONY: debug-secrets
debug-secrets: ## Verifica Secret kubeconfig
	@echo "$(GREEN)🔍 Debug Secret:$(NC)"
	@kubectl get secret cluster-kubeconfigs -n cloudcontinuum-system -o jsonpath='{.data}' | jq 'keys'

.PHONY: debug-contexts
debug-contexts: ## Mostra context Kubernetes disponibili
	@echo "$(GREEN)🔍 Context disponibili:$(NC)"
	@kubectl config get-contexts

.PHONY: debug-nodes
debug-nodes: ## Mostra nodi di tutti i cluster
	@echo "$(GREEN)🔍 Nodi cloud_cluster:$(NC)"
	@kubectl get nodes --context=cloud_cluster
	@echo ""
	@echo "$(GREEN)🔍 Nodi edge_cluster_1:$(NC)"
	@kubectl get nodes --context=edge_cluster_1
	@echo ""
	@echo "$(GREEN)🔍 Nodi edge_cluster_2:$(NC)"
	@kubectl get nodes --context=edge_cluster_2

.PHONY: logs
logs: ## Mostra log del controller (se deployato)
	@echo "$(GREEN)📜 Log controller:$(NC)"
	@kubectl logs -n cloudcontinuum-system -l control-plane=controller-manager --tail=100 -f

# ============================================================================
# UTILITIES
# ============================================================================

.PHONY: quick-start
quick-start: dev-setup install-crd setup-secrets ## Setup completo per iniziare
	@echo "$(GREEN)✓ Setup completo! Ora puoi eseguire: make -f Makefile.helper run$(NC)"

.PHONY: full-test
full-test: clean-all test-all ## Pulizia e test completo
	@echo "$(GREEN)🎉 Test completo eseguito!$(NC)"
	@sleep 5
	@make -f Makefile.helper status
	@make -f Makefile.helper pods-all

.PHONY: health-check
health-check: ## Verifica salute del sistema
	@echo "$(GREEN)🏥 Health Check:$(NC)"
	@echo ""
	@echo "$(YELLOW)CRD installato:$(NC)"
	@kubectl get crd placementrequests.orchestrator.cloudcontinuum.io 2>/dev/null && echo "✓ OK" || echo "✗ NON INSTALLATO"
	@echo ""
	@echo "$(YELLOW)Secret configurato:$(NC)"
	@kubectl get secret cluster-kubeconfigs -n cloudcontinuum-system 2>/dev/null && echo "✓ OK" || echo "✗ NON CONFIGURATO"
	@echo ""
	@echo "$(YELLOW)Cluster raggiungibili:$(NC)"
	@kubectl get nodes --context=cloud_cluster &>/dev/null && echo "✓ cloud_cluster OK" || echo "✗ cloud_cluster ERRORE"
	@kubectl get nodes --context=edge_cluster_1 &>/dev/null && echo "✓ edge_cluster_1 OK" || echo "✗ edge_cluster_1 ERRORE"
	@kubectl get nodes --context=edge_cluster_2 &>/dev/null && echo "✓ edge_cluster_2 OK" || echo "✗ edge_cluster_2 ERRORE"

# ============================================================================
# HELP
# ============================================================================

.PHONY: help
help: ## Mostra questo messaggio di aiuto
	@echo "$(GREEN)========================================$(NC)"
	@echo "$(GREEN)  Cloud Continuum Orchestrator - Helper$(NC)"
	@echo "$(GREEN)========================================$(NC)"
	@echo ""
	@echo "$(YELLOW)Uso:$(NC) make -f Makefile.helper <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)Quick Start:$(NC)"
	@echo "  make -f Makefile.helper quick-start  # Setup completo"
	@echo "  make -f Makefile.helper run           # Avvia controller"
	@echo "  make -f Makefile.helper test-all      # Esegui tutti i test"
	@echo ""