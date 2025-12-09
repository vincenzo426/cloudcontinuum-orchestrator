################################################################################
# Makefile - Utility per Multi-Cluster Kubernetes con Submariner
################################################################################
# Descrizione: Comandi rapidi per controllo e verifica cluster
################################################################################

.PHONY: help

# Colori per output
GREEN  := \033[0;32m
YELLOW := \033[1;33m
RED    := \033[0;31m
BLUE   := \033[0;34m
NC     := \033[0m

################################################################################
# CONFIGURAZIONE
################################################################################

CLOUD := cloud_cluster
EDGE1 := edge_cluster_1
EDGE2 := edge_cluster_2
EDGE3 := edge_cluster_3

CLOUD_IP := 192.168.151.94
EDGE1_IP := 192.168.151.181
EDGE2_IP := 192.168.151.182
EDGE3_IP := 192.168.151.80

################################################################################
# HELP
################################################################################

help: ## Mostra questo help
	@echo "$(BLUE)╔══════════════════════════════════════════════╗$(NC)"
	@echo "$(BLUE)║    Utility Multi-Cluster + Submariner       ║$(NC)"
	@echo "$(BLUE)╚══════════════════════════════════════════════╝$(NC)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*?##/ { printf "  $(YELLOW)%-20s$(NC) %s\n", $$1, $$2 } /^##@/ { printf "\n$(GREEN)%s$(NC)\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
	@echo ""

################################################################################
##@ 📍 Verifica Cluster
################################################################################

nodes: ## Mostra nodi di tutti i cluster
	@echo "$(GREEN)Cloud:$(NC)"
	@kubectl get nodes -o wide --context=$(CLOUD)
	@echo ""
	@echo "$(GREEN)Edge 1:$(NC)"
	@kubectl get nodes -o wide --context=$(EDGE1)
	@echo ""
	@echo "$(GREEN)Edge 2:$(NC)"
	@kubectl get nodes -o wide --context=$(EDGE2)

pods: ## Mostra pod su tutti i cluster
	@echo "$(GREEN)Cloud:$(NC)"
	@kubectl get pods -A --context=$(CLOUD)
	@echo ""
	@echo "$(GREEN)Edge 1:$(NC)"
	@kubectl get pods -A --context=$(EDGE1)
	@echo ""
	@echo "$(GREEN)Edge 2:$(NC)"
	@kubectl get pods -A --context=$(EDGE2)

ns: ## Mostra namespace su tutti i cluster
	@echo "$(GREEN)Cloud:$(NC)"
	@kubectl get ns --context=$(CLOUD)
	@echo ""
	@echo "$(GREEN)Edge 1:$(NC)"
	@kubectl get ns --context=$(EDGE1)
	@echo ""
	@echo "$(GREEN)Edge 2:$(NC)"
	@kubectl get ns --context=$(EDGE2)

################################################################################
##@ 🌊 Submariner
################################################################################

sub-pods: ## Mostra pod Submariner
	@echo "$(GREEN)Cloud:$(NC)"
	@kubectl get pods -n submariner-operator --context=$(CLOUD)
	@echo ""
	@echo "$(GREEN)Edge 1:$(NC)"
	@kubectl get pods -n submariner-operator --context=$(EDGE1)
	@echo ""
	@echo "$(GREEN)Edge 2:$(NC)"
	@kubectl get pods -n submariner-operator --context=$(EDGE2)

sub-gw: ## Mostra gateway Submariner
	@echo "$(GREEN)Cloud:$(NC)"
	@subctl show gateways --context=$(CLOUD)
	@echo ""
	@echo "$(GREEN)Edge 1:$(NC)"
	@subctl show gateways --context=$(EDGE1)
	@echo ""
	@echo "$(GREEN)Edge 2:$(NC)"
	@subctl show gateways --context=$(EDGE2)
	@echo ""
	@echo "$(GREEN)Edge 3:$(NC)"
	@subctl show gateways --context=$(EDGE3)

sub-conn: ## Mostra connessioni cross-cluster
	@echo "$(GREEN)Dal Cloud:$(NC)"
	@subctl show connections --context=$(CLOUD)
	@echo ""
	@echo "$(GREEN)Da Edge 1:$(NC)"
	@subctl show connections --context=$(EDGE1)
	@echo ""
	@echo "$(GREEN)Da Edge 2:$(NC)"
	@subctl show connections --context=$(EDGE2)
	echo ""
	@echo "$(GREEN)Da Edge 3:$(NC)"
	@subctl show connections --context=$(EDGE3)

sub-export: ## Mostra servizi esportati
	@echo "$(GREEN)Edge 1:$(NC)"
	@kubectl get serviceexports -A --context=$(EDGE1)
	@echo ""
	@echo "$(GREEN)Edge 2:$(NC)"
	@kubectl get serviceexports -A --context=$(EDGE2)
	@echo ""
	@echo "$(GREEN)Edge 3:$(NC)"
	@kubectl get serviceexports -A --context=$(EDGE3)

sub-import: ## Mostra servizi importati
	@echo "$(GREEN)Cloud:$(NC)"
	@kubectl get serviceimports -A --context=$(CLOUD)
	@echo ""
	@echo "$(GREEN)Edge 1:$(NC)"
	@kubectl get serviceimports -A --context=$(EDGE1)
	@echo ""
	@echo "$(GREEN)Edge 2:$(NC)"
	@kubectl get serviceimports -A --context=$(EDGE2)
	@echo ""
	@echo "$(GREEN)Edge 3:$(NC)"
	@kubectl get serviceimports -A --context=$(EDGE3)

sub-endpoints: ## Mostra endpoint sul broker
	@ subctl show endpoints

################################################################################
##@ 🔍 Logs e Debug
################################################################################

logs-operator: ## Log operator Submariner (make logs-operator CTX=cloud_cluster)
	@kubectl logs -n submariner-operator -l name=submariner-operator --context=$(CTX) --tail=50

logs-gateway: ## Log gateway Submariner (make logs-gateway CTX=cloud_cluster)
	@kubectl logs -n submariner-operator -l app=submariner-gateway --context=$(CTX) --tail=50

logs-lighthouse: ## Log lighthouse agent (make logs-lighthouse CTX=cloud_cluster)
	@kubectl logs -n submariner-operator -l component=submariner-lighthouse-agent --context=$(CTX) --tail=50

events-sub: ## Eventi namespace submariner-operator
	@echo "$(GREEN)Cloud:$(NC)"
	@kubectl get events -n submariner-operator --context=$(CLOUD) --sort-by='.lastTimestamp' | tail -10
	@echo ""
	@echo "$(GREEN)Edge 1:$(NC)"
	@kubectl get events -n submariner-operator --context=$(EDGE1) --sort-by='.lastTimestamp' | tail -10
	@echo ""
	@echo "$(GREEN)Edge 2:$(NC)"
	@kubectl get events -n submariner-operator --context=$(EDGE2) --sort-by='.lastTimestamp' | tail -10

################################################################################
##@ 🧪 Test
################################################################################

ping: ## Ping tutte le VM
	@echo "$(YELLOW)Cloud ($(CLOUD_IP)):$(NC)"
	@ping -c 2 $(CLOUD_IP) && echo "$(GREEN)✅ OK$(NC)" || echo "$(RED)❌ FAIL$(NC)"
	@echo ""
	@echo "$(YELLOW)Edge 1 ($(EDGE1_IP)):$(NC)"
	@ping -c 2 $(EDGE1_IP) && echo "$(GREEN)✅ OK$(NC)" || echo "$(RED)❌ FAIL$(NC)"
	@echo ""
	@echo "$(YELLOW)Edge 2 ($(EDGE2_IP)):$(NC)"
	@ping -c 2 $(EDGE2_IP) && echo "$(GREEN)✅ OK$(NC)" || echo "$(RED)❌ FAIL$(NC)"

test-dns: ## Test DNS multicluster (richiede servizio nginx-test)
	@kubectl run test-dns --rm -i --image=busybox --restart=Never --context=$(CLOUD) -- \
		nslookup nginx-test.test-submariner.svc.clusterset.local.

test-curl: ## Test curl cross-cluster (richiede servizio nginx-test)
	@kubectl run test-curl --rm -i --image=curlimages/curl --restart=Never --context=$(CLOUD) -- \
		curl -s nginx-test.test-submariner.svc.clusterset.local.

################################################################################
##@ 🔧 Utility
################################################################################

ctx: ## Lista context disponibili
	@kubectl config get-contexts

switch-cloud: ## Switch a cloud_cluster
	@kubectl config use-context $(CLOUD)
	@echo "$(GREEN)✅ Context: $(CLOUD)$(NC)"

switch-edge1: ## Switch a edge_cluster_1
	@kubectl config use-context $(EDGE1)
	@echo "$(GREEN)✅ Context: $(EDGE1)$(NC)"

switch-edge2: ## Switch a edge_cluster_2
	@kubectl config use-context $(EDGE2)
	@echo "$(GREEN)✅ Context: $(EDGE2)$(NC)"

switch-edge3: ## Switch a edge_cluster_3
	@kubectl config use-context $(EDGE3)
	@echo "$(GREEN)✅ Context: $(EDGE3)$(NC)"

ssh-cloud: ## SSH a cloud cluster
	@ssh user@$(CLOUD_IP)

ssh-edge1: ## SSH a edge 1
	@ssh user@$(EDGE1_IP)

ssh-edge2: ## SSH a edge 2
	@ssh user@$(EDGE2_IP)

ssh-edge3: ## SSH a edge 3
	@ssh user@$(EDGE3_IP)

coredns-config: ## Mostra config CoreDNS
	@echo "$(GREEN)Cloud:$(NC)"
	@kubectl get configmap coredns -n kube-system --context=$(CLOUD) -o yaml | grep -A 5 "clusterset.local" || echo "$(RED)Non configurato$(NC)"
	@echo ""
	@echo "$(GREEN)Edge 1:$(NC)"
	@kubectl get configmap coredns -n kube-system --context=$(EDGE1) -o yaml | grep -A 5 "clusterset.local" || echo "$(RED)Non configurato$(NC)"
	@echo ""
	@echo "$(GREEN)Edge 2:$(NC)"
	@kubectl get configmap coredns -n kube-system --context=$(EDGE2) -o yaml | grep -A 5 "clusterset.local"

edge2-status: ## Status risorse edge2
	@echo "$(YELLOW)Tentativo connessione edge2...$(NC)"
	@ssh -o ConnectTimeout=3 user@$(EDGE2_IP) 'bash -s' << 'EOF' 2>/dev/null || echo "$(RED)❌ Connessione fallita$(NC)"
		echo "Memoria: $$(free -h | grep Mem | awk '{print $$3"/"$$2}')"
		echo "CPU Load: $$(uptime | awk -F'load average:' '{print $$2}')"
		echo "Disco: $$(df -h / | tail -1 | awk '{print $$5}')"
	EOF

edge1-status: ## Status risorse edge1
	@echo "$(YELLOW)Tentativo connessione edge2...$(NC)"
	@ssh -o ConnectTimeout=3 user@$(EDGE1_IP) 'bash -s' << 'EOF' 2>/dev/null || echo "$(RED)❌ Connessione fallita$(NC)"
		echo "Memoria: $$(free -h | grep Mem | awk '{print $$3"/"$$2}')"
		echo "CPU Load: $$(uptime | awk -F'load average:' '{print $$2}')"
		echo "Disco: $$(df -h / | tail -1 | awk '{print $$5}')"
	EOF