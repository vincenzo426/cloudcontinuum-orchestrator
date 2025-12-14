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

# Cluster contexts
CLOUD := cloud_cluster
EDGE1 := edge_cluster_1
EDGE2 := edge_cluster_2
EDGE3 := edge_cluster_3

# Cluster IPs
CLOUD_IP := 192.168.151.94
EDGE1_IP := 192.168.151.181
EDGE2_IP := 192.168.151.182
EDGE3_IP := 192.168.151.80

# Docker configuration
DOCKER_USER := cenzo104
PROJECT_NAME := cloudcontinuum-orchestrator
IMG_TAG := latest
IMG := docker.io/$(DOCKER_USER)/$(PROJECT_NAME):$(IMG_TAG)

# Controller namespace
CONTROLLER_NS := cloudcontinuum-orchestrator-system
CONTROLLER_DEPLOY := cloudcontinuum-orchestrator-controller-manager

################################################################################
# HELP
################################################################################

help: ## Mostra questo help
	@echo "$(BLUE)╔══════════════════════════════════════════════╗$(NC)"
	@echo "$(BLUE)║    Utility Multi-Cluster + Submariner       ║$(NC)"
	@echo "$(BLUE)╚══════════════════════════════════════════════╝$(NC)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*?##/ { printf "  $(YELLOW)%-25s$(NC) %s\n", $$1, $$2 } /^##@/ { printf "\n$(GREEN)%s$(NC)\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
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
	@echo ""
	@echo "$(GREEN)Edge 3:$(NC)"
	@kubectl get nodes -o wide --context=$(EDGE3)

pods: ## Mostra pod su tutti i cluster
	@echo "$(GREEN)Cloud:$(NC)"
	@kubectl get pods -A --context=$(CLOUD)
	@echo ""
	@echo "$(GREEN)Edge 1:$(NC)"
	@kubectl get pods -A --context=$(EDGE1)
	@echo ""
	@echo "$(GREEN)Edge 2:$(NC)"
	@kubectl get pods -A --context=$(EDGE2)
	@echo ""
	@echo "$(GREEN)Edge 3:$(NC)"
	@kubectl get pods -A --context=$(EDGE3)

ns: ## Mostra namespace su tutti i cluster
	@echo "$(GREEN)Cloud:$(NC)"
	@kubectl get ns --context=$(CLOUD)
	@echo ""
	@echo "$(GREEN)Edge 1:$(NC)"
	@kubectl get ns --context=$(EDGE1)
	@echo ""
	@echo "$(GREEN)Edge 2:$(NC)"
	@kubectl get ns --context=$(EDGE2)
	@echo ""
	@echo "$(GREEN)Edge 3:$(NC)"
	@kubectl get ns --context=$(EDGE3)

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
	@echo ""
	@echo "$(GREEN)Edge 3:$(NC)"
	@kubectl get pods -n submariner-operator --context=$(EDGE3)

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
	@echo ""
	@echo "$(GREEN)Da Edge 3:$(NC)"
	@subctl show connections --context=$(EDGE3)

sub-export: ## Mostra servizi esportati
	@echo "$(GREEN)Cloud:$(NC)"
	@kubectl get serviceexports -A --context=$(CLOUD)
	@echo ""
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
##@ 🚀 CloudContinuum Orchestrator
################################################################################

ppr-list: ## Lista tutte le PipelinePlacementRequest
	@echo "$(BLUE)╔══════════════════════════════════════════════╗$(NC)"
	@echo "$(BLUE)║     PipelinePlacementRequest Status          ║$(NC)"
	@echo "$(BLUE)╚══════════════════════════════════════════════╝$(NC)"
	@echo ""
	@kubectl --context=$(CLOUD) get pipelineplacementrequest -n kubeflow -o wide 2>/dev/null || echo "$(YELLOW)Nessuna PipelinePlacementRequest trovata$(NC)"

ppr-status: ## Mostra status dettagliato di tutte le PPR
	@echo "$(BLUE)╔══════════════════════════════════════════════╗$(NC)"
	@echo "$(BLUE)║   PipelinePlacementRequest - Status Detail   ║$(NC)"
	@echo "$(BLUE)╚══════════════════════════════════════════════╝$(NC)"
	@echo ""
	@kubectl --context=$(CLOUD) get pipelineplacementrequest -n kubeflow -o yaml | grep -A 10 status 2>/dev/null || echo "$(YELLOW)Nessuna PipelinePlacementRequest trovata$(NC)"

ppr-describe: ## Descrivi una PPR (uso: make ppr-describe NAME=simple-request)
	@if [ -z "$(NAME)" ]; then \
		echo "$(RED)Errore: specificare NAME=<nome-ppr>$(NC)"; \
		echo "Esempio: make ppr-describe NAME=simple-request"; \
		exit 1; \
	fi
	@kubectl --context=$(CLOUD) describe pipelineplacementrequest $(NAME) -n kubeflow

ppr-apply: ## Applica una PPR di esempio
	@echo "$(GREEN)Applicando PipelinePlacementRequest...$(NC)"
	@kubectl --context=$(CLOUD) apply -f config/samples/orchestrator_v1alpha1_pipelineplacementrequest.yaml -n kubeflow
	@echo "$(GREEN)✅ PipelinePlacementRequest creata$(NC)"

ppr-delete: ## Elimina tutte le PipelinePlacementRequest
	@echo "$(YELLOW)Eliminando tutte le PipelinePlacementRequest...$(NC)"
	@kubectl --context=$(CLOUD) delete pipelineplacementrequest --all -n kubeflow
	@echo "$(GREEN)✅ PipelinePlacementRequest eliminate$(NC)"

ppr-delete-one: ## Elimina una PPR specifica (uso: make ppr-delete-one NAME=simple-request)
	@if [ -z "$(NAME)" ]; then \
		echo "$(RED)Errore: specificare NAME=<nome-ppr>$(NC)"; \
		echo "Esempio: make ppr-delete-one NAME=simple-request"; \
		exit 1; \
	fi
	@kubectl --context=$(CLOUD) delete pipelineplacementrequest $(NAME) -n kubeflow
	@echo "$(GREEN)✅ PipelinePlacementRequest $(NAME) eliminata$(NC)"

ppr-watch: ## Monitora le PPR in tempo reale
	@echo "$(BLUE)Monitoraggio PipelinePlacementRequest in tempo reale...$(NC)"
	@echo "$(YELLOW)Premi CTRL+C per uscire$(NC)"
	@echo ""
	@kubectl --context=$(CLOUD) get pipelineplacementrequest -n kubeflow -w

################################################################################
##@ 🔄 Workflow Monitoring
################################################################################

wf-cloud: ## Mostra workflow su cloud cluster
	@echo "$(GREEN)Workflow su Cloud Cluster:$(NC)"
	@kubectl --context=$(CLOUD) get workflow -n kubeflow 2>/dev/null || echo "$(YELLOW)Nessun workflow trovato$(NC)"

wf-edge1: ## Mostra workflow su edge1 cluster
	@echo "$(GREEN)Workflow su Edge 1 Cluster:$(NC)"
	@kubectl --context=$(EDGE1) get workflow -n kubeflow 2>/dev/null || echo "$(YELLOW)Nessun workflow trovato$(NC)"

wf-edge2: ## Mostra workflow su edge2 cluster
	@echo "$(GREEN)Workflow su Edge 2 Cluster:$(NC)"
	@kubectl --context=$(EDGE2) get workflow -n kubeflow 2>/dev/null || echo "$(YELLOW)Nessun workflow trovato$(NC)"

wf-edge3: ## Mostra workflow su edge3 cluster
	@echo "$(GREEN)Workflow su Edge 3 Cluster:$(NC)"
	@kubectl --context=$(EDGE3) get workflow -n kubeflow 2>/dev/null || echo "$(YELLOW)Nessun workflow trovato$(NC)"

wf-all: ## Mostra workflow su tutti i cluster
	@echo "$(BLUE)╔══════════════════════════════════════════════╗$(NC)"
	@echo "$(BLUE)║         Workflows su Tutti i Cluster         ║$(NC)"
	@echo "$(BLUE)╚══════════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "$(GREEN)Cloud Cluster:$(NC)"
	@kubectl --context=$(CLOUD) get workflow -n kubeflow 2>/dev/null || echo "$(YELLOW)Nessun workflow$(NC)"
	@echo ""
	@echo "$(GREEN)Edge 1 Cluster:$(NC)"
	@kubectl --context=$(EDGE1) get workflow -n kubeflow 2>/dev/null || echo "$(YELLOW)Nessun workflow$(NC)"
	@echo ""
	@echo "$(GREEN)Edge 2 Cluster:$(NC)"
	@kubectl --context=$(EDGE2) get workflow -n kubeflow 2>/dev/null || echo "$(YELLOW)Nessun workflow$(NC)"
	@echo ""
	@echo "$(GREEN)Edge 3 Cluster:$(NC)"
	@kubectl --context=$(EDGE3) get workflow -n kubeflow 2>/dev/null || echo "$(YELLOW)Nessun workflow$(NC)"

wf-describe: ## Descrivi workflow (uso: make wf-describe NAME=<workflow> CTX=cloud_cluster)
	@if [ -z "$(NAME)" ] || [ -z "$(CTX)" ]; then \
		echo "$(RED)Errore: specificare NAME=<workflow> e CTX=<cluster>$(NC)"; \
		echo "Esempio: make wf-describe NAME=pipeline-prova-8-ckfcx CTX=cloud_cluster"; \
		exit 1; \
	fi
	@kubectl --context=$(CTX) describe workflow $(NAME) -n kubeflow

wf-delete: ## Elimina tutti i workflow (uso: make wf-delete CTX=cloud_cluster)
	@if [ -z "$(CTX)" ]; then \
		echo "$(RED)Errore: specificare CTX=<cluster>$(NC)"; \
		echo "Esempio: make wf-delete CTX=cloud_cluster"; \
		exit 1; \
	fi
	@echo "$(YELLOW)Eliminando tutti i workflow su $(CTX)...$(NC)"
	@kubectl --context=$(CTX) delete workflow --all -n kubeflow
	@echo "$(GREEN)✅ Workflow eliminati$(NC)"

wf-delete-all: ## Elimina workflow su tutti i cluster
	@echo "$(YELLOW)Eliminando workflow su tutti i cluster...$(NC)"
	@kubectl --context=$(CLOUD) delete workflow --all -n kubeflow 2>/dev/null || true
	@kubectl --context=$(EDGE1) delete workflow --all -n kubeflow 2>/dev/null || true
	@kubectl --context=$(EDGE2) delete workflow --all -n kubeflow 2>/dev/null || true
	@kubectl --context=$(EDGE3) delete workflow --all -n kubeflow 2>/dev/null || true
	@echo "$(GREEN)✅ Tutti i workflow eliminati$(NC)"

wf-watch: ## Monitora workflow in tempo reale (uso: make wf-watch CTX=cloud_cluster)
	@if [ -z "$(CTX)" ]; then \
		echo "$(RED)Errore: specificare CTX=<cluster>$(NC)"; \
		echo "Esempio: make wf-watch CTX=cloud_cluster"; \
		exit 1; \
	fi
	@echo "$(BLUE)Monitoraggio workflow su $(CTX)...$(NC)"
	@echo "$(YELLOW)Premi CTRL+C per uscire$(NC)"
	@echo ""
	@kubectl --context=$(CTX) get workflow -n kubeflow -w

################################################################################
##@ 🐳 Docker Build & Deploy
################################################################################

docker-info: ## Mostra informazioni configurazione Docker
	@echo "$(BLUE)╔══════════════════════════════════════════════╗$(NC)"
	@echo "$(BLUE)║          Docker Configuration Info           ║$(NC)"
	@echo "$(BLUE)╚══════════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "$(GREEN)Docker User:$(NC)      $(DOCKER_USER)"
	@echo "$(GREEN)Project Name:$(NC)     $(PROJECT_NAME)"
	@echo "$(GREEN)Image Tag:$(NC)        $(IMG_TAG)"
	@echo "$(GREEN)Full Image:$(NC)       $(IMG)"
	@echo "$(GREEN)Namespace:$(NC)        $(CONTROLLER_NS)"
	@echo "$(GREEN)Deployment:$(NC)       $(CONTROLLER_DEPLOY)"
	@echo ""

docker-login: ## Login a Docker Hub
	@echo "$(YELLOW)Login a Docker Hub come $(DOCKER_USER)...$(NC)"
	@docker login -u $(DOCKER_USER)
	@echo "$(GREEN)✅ Login effettuato$(NC)"

docker-build: ## Build immagine Docker del controller
	@echo "$(BLUE)╔══════════════════════════════════════════════╗$(NC)"
	@echo "$(BLUE)║           Building Docker Image              ║$(NC)"
	@echo "$(BLUE)╚══════════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "$(YELLOW)Building image: $(IMG)$(NC)"
	@docker build -t $(IMG) .
	@echo ""
	@echo "$(GREEN)✅ Build completato: $(IMG)$(NC)"

docker-push: ## Push immagine su Docker Hub
	@echo "$(BLUE)╔══════════════════════════════════════════════╗$(NC)"
	@echo "$(BLUE)║           Pushing Docker Image               ║$(NC)"
	@echo "$(BLUE)╚══════════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "$(YELLOW)Pushing image: $(IMG)$(NC)"
	@docker push $(IMG)
	@echo ""
	@echo "$(GREEN)✅ Push completato: $(IMG)$(NC)"

docker-build-push: docker-build docker-push ## Build e push immagine Docker

controller-update: ## Aggiorna immagine del controller nel cluster
	@echo "$(BLUE)╔══════════════════════════════════════════════╗$(NC)"
	@echo "$(BLUE)║         Updating Controller Image            ║$(NC)"
	@echo "$(BLUE)╚══════════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "$(YELLOW)Aggiornamento deployment con image: $(IMG)$(NC)"
	@kubectl --context=$(CLOUD) set image deployment/$(CONTROLLER_DEPLOY) \
		manager=$(IMG) \
		-n $(CONTROLLER_NS)
	@echo ""
	@echo "$(GREEN)✅ Deployment aggiornato$(NC)"

controller-rollout: ## Attendi il rollout del controller
	@echo "$(YELLOW)Attendendo il rollout del controller...$(NC)"
	@kubectl --context=$(CLOUD) rollout status deployment/$(CONTROLLER_DEPLOY) \
		-n $(CONTROLLER_NS)
	@echo "$(GREEN)✅ Rollout completato$(NC)"

controller-restart: ## Forza restart del controller
	@echo "$(YELLOW)Restart del controller...$(NC)"
	@kubectl --context=$(CLOUD) rollout restart deployment/$(CONTROLLER_DEPLOY) \
		-n $(CONTROLLER_NS)
	@kubectl --context=$(CLOUD) rollout status deployment/$(CONTROLLER_DEPLOY) \
		-n $(CONTROLLER_NS)
	@echo "$(GREEN)✅ Controller riavviato$(NC)"

controller-logs: ## Mostra log del controller in tempo reale
	@echo "$(BLUE)Log del controller (CTRL+C per uscire):$(NC)"
	@echo ""
	@kubectl --context=$(CLOUD) logs -n $(CONTROLLER_NS) \
		deployment/$(CONTROLLER_DEPLOY) -f

controller-status: ## Mostra status del controller
	@echo "$(BLUE)╔══════════════════════════════════════════════╗$(NC)"
	@echo "$(BLUE)║          Controller Status                   ║$(NC)"
	@echo "$(BLUE)╚══════════════════════════════════════════════╝$(NC)"
	@echo ""
	@kubectl --context=$(CLOUD) get deployment $(CONTROLLER_DEPLOY) \
		-n $(CONTROLLER_NS) -o wide
	@echo ""
	@echo "$(GREEN)Pods:$(NC)"
	@kubectl --context=$(CLOUD) get pods -n $(CONTROLLER_NS) \
		-l control-plane=controller-manager

controller-describe: ## Descrivi il deployment del controller
	@kubectl --context=$(CLOUD) describe deployment $(CONTROLLER_DEPLOY) \
		-n $(CONTROLLER_NS)

################################################################################
##@ 🔧 Deploy Workflow Completo
################################################################################

deploy-full: docker-build-push controller-update controller-rollout ## Build, push e deploy completo
	@echo ""
	@echo "$(GREEN)╔══════════════════════════════════════════════╗$(NC)"
	@echo "$(GREEN)║          Deploy Completato con Successo!     ║$(NC)"
	@echo "$(GREEN)╚══════════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "$(YELLOW)Immagine deployata:$(NC) $(IMG)"
	@echo ""

test-pipeline: ppr-delete wf-delete-all ppr-apply ## Test completo: pulisci + crea nuova pipeline
	@echo ""
	@echo "$(GREEN)╔══════════════════════════════════════════════╗$(NC)"
	@echo "$(GREEN)║        Test Pipeline Avviato                 ║$(NC)"
	@echo "$(GREEN)╚══════════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "$(YELLOW)Monitorare con:$(NC) make ppr-watch"
	@echo "$(YELLOW)Oppure:$(NC)         make wf-all"
	@echo ""

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
	@echo ""
	@echo "$(GREEN)Edge 3:$(NC)"
	@kubectl get events -n submariner-operator --context=$(EDGE3) --sort-by='.lastTimestamp' | tail -10

events-kubeflow: ## Eventi namespace kubeflow su cloud cluster
	@echo "$(GREEN)Eventi Kubeflow (Cloud):$(NC)"
	@kubectl --context=$(CLOUD) get events -n kubeflow --sort-by='.lastTimestamp' | tail -20

events-controller: ## Eventi del controller
	@echo "$(GREEN)Eventi Controller:$(NC)"
	@kubectl --context=$(CLOUD) get events -n $(CONTROLLER_NS) --sort-by='.lastTimestamp' | tail -20

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
	@echo ""
	@echo "$(YELLOW)Edge 3 ($(EDGE3_IP)):$(NC)"
	@ping -c 2 $(EDGE3_IP) && echo "$(GREEN)✅ OK$(NC)" || echo "$(RED)❌ FAIL$(NC)"

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
	@kubectl get configmap coredns -n kube-system --context=$(EDGE2) -o yaml | grep -A 5 "clusterset.local" || echo "$(RED)Non configurato$(NC)"
	@echo ""
	@echo "$(GREEN)Edge 3:$(NC)"
	@kubectl get configmap coredns -n kube-system --context=$(EDGE3) -o yaml | grep -A 5 "clusterset.local" || echo "$(RED)Non configurato$(NC)"

edge1-status: ## Status risorse edge1
	@echo "$(YELLOW)Tentativo connessione edge1...$(NC)"
	@ssh -o ConnectTimeout=3 user@$(EDGE1_IP) 'bash -s' << 'EOF' 2>/dev/null || echo "$(RED)❌ Connessione fallita$(NC)"
		echo "Memoria: $$(free -h | grep Mem | awk '{print $$3"/"$$2}')"
		echo "CPU Load: $$(uptime | awk -F'load average:' '{print $$2}')"
		echo "Disco: $$(df -h / | tail -1 | awk '{print $$5}')"
	EOF

edge2-status: ## Status risorse edge2
	@echo "$(YELLOW)Tentativo connessione edge2...$(NC)"
	@ssh -o ConnectTimeout=3 user@$(EDGE2_IP) 'bash -s' << 'EOF' 2>/dev/null || echo "$(RED)❌ Connessione fallita$(NC)"
		echo "Memoria: $$(free -h | grep Mem | awk '{print $$3"/"$$2}')"
		echo "CPU Load: $$(uptime | awk -F'load average:' '{print $$2}')"
		echo "Disco: $$(df -h / | tail -1 | awk '{print $$5}')"
	EOF

edge3-status: ## Status risorse edge3
	@echo "$(YELLOW)Tentativo connessione edge3...$(NC)"
	@ssh -o ConnectTimeout=3 user@$(EDGE3_IP) 'bash -s' << 'EOF' 2>/dev/null || echo "$(RED)❌ Connessione fallita$(NC)"
		echo "Memoria: $$(free -h | grep Mem | awk '{print $$3"/"$$2}')"
		echo "CPU Load: $$(uptime | awk -F'load average:' '{print $$2}')"
		echo "Disco: $$(df -h / | tail -1 | awk '{print $$5}')"
	EOF