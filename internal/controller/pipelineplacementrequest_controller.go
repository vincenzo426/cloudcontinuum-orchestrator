/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	orchestratorv1alpha1 "github.com/vincenzo426/cloudcontinuum-orchestrator/api/v1alpha1"
	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/kubeflow"
	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/metrics"
	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/multicluster"
	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/pipeline"
	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

// ============================================================================
// STRUTTURA DEL RECONCILER
// ============================================================================

// PipelinePlacementRequestReconciler è il controller principale del CloudContinuum Orchestrator.
// Gestisce il ciclo di vita delle PipelinePlacementRequest, orchestrando:
//   - Parsing delle pipeline Kubeflow
//   - Raccolta metriche dai cluster
//   - Decisioni di placement usando strategie configurabili
//   - Deploy ed esecuzione delle pipeline sui cluster target
type PipelinePlacementRequestReconciler struct {
	client.Client                                           // Client Kubernetes per il cluster principale
	Scheme             *runtime.Scheme                      // Schema di runtime per le API
	pipelineStrategies map[string]pipeline.PipelineStrategy // Strategie di placement disponibili
	ClusterManager     *multicluster.ClusterManager         // Gestore dei cluster nel continuum
	MetricsCollector   metrics.Collector                    // Collettore metriche dai cluster
	pipelineParser     *pipeline.Parser                     // Parser per Kubeflow IR v2.1.0
	kubeflowManager    *kubeflow.Manager                    // Manager per interazioni con Kubeflow API
}

// ============================================================================
// RBAC PERMISSIONS
// ============================================================================
// Definisce i permessi necessari per il controller operare correttamente

// +kubebuilder:rbac:groups=orchestrator.cloudcontinuum.io,resources=pipelineplacementrequests,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=orchestrator.cloudcontinuum.io,resources=pipelineplacementrequests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=orchestrator.cloudcontinuum.io,resources=pipelineplacementrequests/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch

// ============================================================================
// RECONCILIATION LOOP PRINCIPALE
// ============================================================================

// Reconcile implementa il loop di riconciliazione principale del controller.
// Questo metodo viene chiamato automaticamente da controller-runtime ogni volta che
// una PipelinePlacementRequest viene creata, modificata o periodicamente.
//
// Flusso di esecuzione (10 step):
//  1. Recupero della PipelinePlacementRequest dal cluster
//  2. Verifica idempotenza (skip se già processata)
//  3. Fetch del YAML della pipeline dalla sorgente specificata
//  4. Parsing del Kubeflow IR per estrarre metadata e requisiti
//  5. Raccolta metriche dai cluster disponibili
//  6. Selezione della strategia di placement
//  7. Esecuzione della decisione di placement
//  8. Calcolo delle risorse totali richieste dalla pipeline
//  9. Deploy e avvio della pipeline sul cluster target via Kubeflow API
//
// 10. Aggiornamento dello status con risultati dell'operazione
//
// Parametri:
//   - ctx: context per cancellazione e logging
//   - req: richiesta di riconciliazione contenente name/namespace della risorsa
//
// Ritorna:
//   - ctrl.Result: indica se e quando ri-eseguire la riconciliazione
//   - error: errore in caso di fallimento (causa requeue automatico)
func (r *PipelinePlacementRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("🔄 Reconciling PipelinePlacementRequest", "name", req.Name, "namespace", req.Namespace)

	// ========================================================================
	// STEP 1: Recupero della PipelinePlacementRequest
	// ========================================================================
	ppr := &orchestratorv1alpha1.PipelinePlacementRequest{}
	if err := r.Get(ctx, req.NamespacedName, ppr); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("✅ PipelinePlacementRequest not found, likely deleted")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "❌ Failed to get PipelinePlacementRequest")
		return ctrl.Result{}, err
	}

	// ========================================================================
	// STEP 2: Verifica idempotenza - skip se già processata
	// ========================================================================
	if ppr.Status.TargetCluster != "" {
		logger.Info("✅ PipelinePlacementRequest already placed (idempotent skip)",
			"cluster", ppr.Status.TargetCluster,
			"runID", ppr.Status.PipelineRunID)
		return ctrl.Result{}, nil
	}

	// ========================================================================
	// STEP 3: Fetch del YAML della pipeline
	// ========================================================================
	// Supporta tre sorgenti: inline YAML, ConfigMap, HTTP URL
	pipelineYAML, err := r.fetchPipelineYAML(ctx, ppr)
	if err != nil {
		logger.Error(err, "❌ Failed to fetch pipeline YAML")
		r.updateStatus(ctx, ppr, "", "", nil, "", "", "", "", nil, false, "Failed to fetch pipeline YAML")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}

	logger.V(1).Info("📄 Pipeline YAML fetched successfully",
		"sourceType", r.getPipelineSourceType(ppr),
		"size", len(pipelineYAML))

	// ========================================================================
	// STEP 4: Parsing del Kubeflow Pipeline IR
	// ========================================================================
	// Estrae metadata, executors e requisiti di risorse dalla pipeline
	pipelineIR, err := r.pipelineParser.ParsePipelineIR([]byte(pipelineYAML))
	if err != nil {
		logger.Error(err, "❌ Failed to parse pipeline YAML")
		r.updateStatus(ctx, ppr, "", "", nil, "", "", "", "", ppr.Spec.Parameters, false, "Failed to parse pipeline YAML")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}

	logger.Info("✅ Pipeline parsed successfully",
		"pipelineName", pipelineIR.PipelineInfo.Name,
		"executors", len(pipelineIR.DeploymentSpec.Executors),
		"schemaVersion", pipelineIR.SchemaVersion)

	// ========================================================================
	// STEP 5: Raccolta metriche dai cluster
	// ========================================================================
	// Colleziona CPU, memoria, disponibilità da tutti i cluster
	clusterMetrics, err := r.collectMetrics(ctx)
	if err != nil {
		logger.Error(err, "❌ Failed to collect metrics")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}

	logger.V(1).Info("📊 Cluster metrics collected",
		"clusters", len(clusterMetrics.Clusters))

	// ========================================================================
	// STEP 6: Selezione della strategia di placement
	// ========================================================================
	// Strategie disponibili: cloud-only, data-locality, simple-heuristic
	strategy, ok := r.pipelineStrategies[ppr.Spec.PlacementStrategy]
	if !ok {
		err := fmt.Errorf("unknown placement strategy: %s", ppr.Spec.PlacementStrategy)
		logger.Error(err, "❌ Invalid strategy")
		r.updateStatus(ctx, ppr, "", "", nil, "", "", "", "", ppr.Spec.Parameters, false, "Invalid placement strategy")
		return ctrl.Result{}, err
	}

	// ========================================================================
	// STEP 7: Esecuzione della decisione di placement
	// ========================================================================
	// La strategia decide su quale cluster eseguire la pipeline
	targetCluster, decision, err := strategy.SelectCluster(
		ctx,
		pipelineIR,
		ppr.Spec.DataLocation,
		clusterMetrics,
	)

	if err != nil {
		logger.Error(err, "❌ Failed to select cluster",
			"strategy", ppr.Spec.PlacementStrategy)
		r.updateStatus(ctx, ppr, "", decision, nil, "", "", "", "", ppr.Spec.Parameters, false, "Placement selection failed")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}

	logger.Info("🎯 Placement decision made",
		"strategy", ppr.Spec.PlacementStrategy,
		"targetCluster", targetCluster,
		"decision", decision)

	// ========================================================================
	// STEP 8: Calcolo delle risorse totali della pipeline
	// ========================================================================
	// Somma CPU, memoria, GPU di tutti gli executor della pipeline
	totalResources := pipeline.CalculatePipelineResources(pipelineIR, r.pipelineParser)

	resourcesSummary := &orchestratorv1alpha1.PipelineResourcesSummary{
		ExecutorCount: totalResources.ExecutorCount,
		TotalCPU:      totalResources.TotalCPU,
		TotalMemory:   totalResources.TotalMemory,
		TotalGPU:      totalResources.TotalGPU,
	}

	logger.Info("💾 Pipeline resources calculated",
		"executors", resourcesSummary.ExecutorCount,
		"totalCPU", resourcesSummary.TotalCPU,
		"totalMemory", resourcesSummary.TotalMemory,
		"totalGPU", resourcesSummary.TotalGPU)

	// ========================================================================
	// STEP 9: Deploy e avvio della pipeline su Kubeflow
	// ========================================================================
	// Genera nome univoco per la run usando timestamp
	runName := fmt.Sprintf("%s-%s", ppr.Name, time.Now().Format("20060102-150405"))

	logger.Info("🚀 Triggering pipeline execution on Kubeflow",
		"cluster", targetCluster,
		"pipeline", pipelineIR.PipelineInfo.Name,
		"runName", runName)

	// Conversione parametri da map[string]string a map[string]interface{}
	params := make(map[string]interface{})
	for k, v := range ppr.Spec.Parameters {
		params[k] = v
	}

	// Chiama Kubeflow API per upload + creazione run
	runID, runURL, err := r.kubeflowManager.UploadAndRunPipeline(
		ctx,
		targetCluster,
		pipelineIR.PipelineInfo.Name,
		[]byte(pipelineYAML),
		runName,
		ppr.Spec.ExperimentId,   // Experiment ID (opzionale)
		ppr.Spec.ExperimentName, // Experiment name (opzionale)
		params,
	)

	if err != nil {
		logger.Error(err, "❌ Failed to execute pipeline on Kubeflow",
			"cluster", targetCluster,
			"pipeline", pipelineIR.PipelineInfo.Name)

		r.updateStatus(ctx, ppr, targetCluster, decision, resourcesSummary, "", "",
			ppr.Spec.ExperimentId, ppr.Spec.ExperimentName, ppr.Spec.Parameters, false,
			fmt.Sprintf("Failed to execute pipeline: %v", err))

		// Retry dopo 60 secondi
		return ctrl.Result{RequeueAfter: 60 * time.Second}, err
	}

	logger.Info("✅ Pipeline execution triggered successfully",
		"cluster", targetCluster,
		"runID", runID,
		"runURL", runURL)

	// ========================================================================
	// STEP 10: Aggiornamento dello status con successo
	// ========================================================================
	if err := r.updateStatus(ctx, ppr, targetCluster, decision, resourcesSummary,
		runID, runURL, ppr.Spec.ExperimentId, ppr.Spec.ExperimentName, ppr.Spec.Parameters, true, ""); err != nil {
		logger.Error(err, "❌ Failed to update status")
		return ctrl.Result{}, err
	}

	logger.Info("✅ PipelinePlacementRequest reconciled successfully",
		"cluster", targetCluster,
		"runID", runID)

	return ctrl.Result{}, nil
}

// ============================================================================
// HELPER METHODS - FETCH PIPELINE
// ============================================================================

// fetchPipelineYAML recupera il contenuto YAML della pipeline dalla sorgente specificata.
// Supporta tre modalità:
//  1. Inline: YAML direttamente nel campo spec.pipelineSource.inline
//  2. ConfigMap: riferimento a ConfigMap (non ancora implementato)
//  3. URL: download da endpoint HTTP/HTTPS
//
// Parametri:
//   - ctx: context per cancellazione
//   - ppr: risorsa PipelinePlacementRequest contenente la configurazione sorgente
//
// Ritorna:
//   - string: contenuto YAML della pipeline
//   - error: errore se nessuna sorgente valida o fetch fallito
func (r *PipelinePlacementRequestReconciler) fetchPipelineYAML(
	ctx context.Context,
	ppr *orchestratorv1alpha1.PipelinePlacementRequest,
) (string, error) {
	source := ppr.Spec.PipelineSource

	// Modalità 1: YAML inline nel CRD
	if source.Inline != "" {
		return source.Inline, nil
	}

	// Modalità 2: ConfigMap reference (TODO: da implementare)
	// if source.ConfigMapRef != nil { ... }

	// Modalità 3: Download da URL HTTP/HTTPS
	if source.URL != "" {
		return r.fetchFromURL(ctx, source.URL)
	}

	return "", fmt.Errorf("no valid pipeline source specified")
}

// fetchFromURL scarica il contenuto YAML da un URL HTTP/HTTPS.
// Implementa timeout di 30 secondi e validazione status code.
//
// Parametri:
//   - ctx: context per cancellazione
//   - url: URL completo da cui scaricare il YAML
//
// Ritorna:
//   - string: contenuto scaricato
//   - error: errore in caso di fallimento HTTP o timeout
func (r *PipelinePlacementRequestReconciler) fetchFromURL(ctx context.Context, url string) (string, error) {
	// Crea context con timeout di 30 secondi
	httpCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Prepara richiesta HTTP GET
	req, err := http.NewRequestWithContext(httpCtx, "GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Esegui richiesta
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to fetch URL %s: %w", url, err)
	}
	defer resp.Body.Close()

	// Valida status code (deve essere 200 OK)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP request failed with status %d", resp.StatusCode)
	}

	// Leggi body completo
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	return string(body), nil
}

// ============================================================================
// HELPER METHODS - METRICS COLLECTION
// ============================================================================

// collectMetrics raccoglie metriche dai cluster disponibili nel continuum.
// Utilizza il MetricsCollector configurato per ottenere dati reali su:
//   - CPU capacity e availability
//   - Memory capacity e availability
//   - Stato di disponibilità del cluster
//
// In caso di errore nella raccolta, utilizza metriche mock come fallback.
//
// Parametri:
//   - ctx: context per cancellazione
//
// Ritorna:
//   - *placement.ClusterMetrics: metriche aggregate di tutti i cluster
//   - error: sempre nil (usa fallback in caso di errore)
func (r *PipelinePlacementRequestReconciler) collectMetrics(ctx context.Context) (*placement.ClusterMetrics, error) {
	logger := log.FromContext(ctx)

	// Tenta raccolta metriche reali
	metrics, err := r.MetricsCollector.CollectMetrics(ctx)
	if err != nil {
		logger.Error(err, "⚠️  Failed to collect real metrics, using fallback")
		// Fallback a metriche mock in caso di errore
		return r.collectMockMetrics(ctx), nil
	}

	logger.V(1).Info("📊 Real metrics collected", "clusters", len(metrics.Clusters))
	return metrics, nil
}

// collectMockMetrics fornisce metriche di fallback per testing e resilienza.
// Utilizzato quando la raccolta metriche reali fallisce.
//
// Configurazione mock:
//   - cloud_cluster: 16 CPU, 32GB RAM (potente)
//   - edge_cluster_1: 4 CPU, 8GB RAM
//   - edge_cluster_2: 4 CPU, 8GB RAM
//
// Parametri:
//   - ctx: context (non utilizzato)
//
// Ritorna:
//   - *placement.ClusterMetrics: metriche mock pre-configurate
func (r *PipelinePlacementRequestReconciler) collectMockMetrics(ctx context.Context) *placement.ClusterMetrics {
	metrics := placement.NewClusterMetrics()

	// Cloud cluster: ambiente potente con alte risorse
	metrics.SetCluster("cloud_cluster", &placement.ClusterMetric{
		Name:            "cloud_cluster",
		CPUCapacity:     16000,                   // 16 CPU core (millicores)
		CPUAvailable:    12000,                   // 12 CPU disponibili
		MemoryCapacity:  32 * 1024 * 1024 * 1024, // 32GB
		MemoryAvailable: 24 * 1024 * 1024 * 1024, // 24GB disponibili
		Available:       true,
	})

	// Edge cluster 1: risorse limitate
	metrics.SetCluster("edge_cluster_1", &placement.ClusterMetric{
		Name:            "edge_cluster_1",
		CPUCapacity:     4000,                   // 4 CPU core
		CPUAvailable:    3000,                   // 3 CPU disponibili
		MemoryCapacity:  8 * 1024 * 1024 * 1024, // 8GB
		MemoryAvailable: 6 * 1024 * 1024 * 1024, // 6GB disponibili
		Available:       true,
	})

	// Edge cluster 2: risorse limitate
	metrics.SetCluster("edge_cluster_2", &placement.ClusterMetric{
		Name:            "edge_cluster_2",
		CPUCapacity:     4000,                   // 4 CPU core
		CPUAvailable:    2500,                   // 2.5 CPU disponibili
		MemoryCapacity:  8 * 1024 * 1024 * 1024, // 8GB
		MemoryAvailable: 5 * 1024 * 1024 * 1024, // 5GB disponibili
		Available:       true,
	})

	return metrics
}

// ============================================================================
// HELPER METHODS - UTILITY
// ============================================================================

// getPipelineSourceType ritorna una stringa descrittiva del tipo di sorgente pipeline.
// Utilizzato per logging e debug.
//
// Parametri:
//   - ppr: risorsa PipelinePlacementRequest
//
// Ritorna:
//   - string: descrizione del tipo di sorgente ("inline", "configmap:...", "url:...", "unknown")
func (r *PipelinePlacementRequestReconciler) getPipelineSourceType(
	ppr *orchestratorv1alpha1.PipelinePlacementRequest,
) string {
	source := ppr.Spec.PipelineSource

	if source.Inline != "" {
		return "inline"
	}
	if source.ConfigMapRef != nil {
		return fmt.Sprintf("configmap:%s/%s", source.ConfigMapRef.Name, source.ConfigMapRef.Key)
	}
	if source.URL != "" {
		return fmt.Sprintf("url:%s", source.URL)
	}
	return "unknown"
}

// ============================================================================
// STATUS UPDATE
// ============================================================================

// updateStatus aggiorna lo status della PipelinePlacementRequest con i risultati dell'orchestrazione.
// Gestisce sia casi di successo che di fallimento, aggiornando:
//   - Cluster target selezionato
//   - Decisione di placement con motivazione
//   - Risorse totali della pipeline
//   - ID e URL della run Kubeflow
//   - Experiment ID e nome
//   - Conditions Kubernetes per stato Placed
//
// Parametri:
//   - ctx: context per cancellazione
//   - ppr: risorsa da aggiornare
//   - targetCluster: nome del cluster selezionato
//   - decision: stringa descrittiva della decisione di placement
//   - resources: summary delle risorse richieste dalla pipeline
//   - runID: ID univoco della run Kubeflow
//   - runURL: URL per accedere alla run nella UI
//   - experimentID: ID dell'experiment Kubeflow
//   - experimentName: nome dell'experiment
//   - success: true se placement riuscito, false altrimenti
//   - errorMsg: messaggio di errore in caso di fallimento
//
// Ritorna:
//   - error: errore in caso di fallimento update API
func (r *PipelinePlacementRequestReconciler) updateStatus(
	ctx context.Context,
	ppr *orchestratorv1alpha1.PipelinePlacementRequest,
	targetCluster string,
	decision string,
	resources *orchestratorv1alpha1.PipelineResourcesSummary,
	runID string,
	runURL string,
	experimentID string,
	experimentName string,
	parameters map[string]string,
	success bool,
	errorMsg string,
) error {
	// Aggiorna campi principali dello status
	ppr.Status.TargetCluster = targetCluster
	ppr.Status.PlacementDecision = decision
	ppr.Status.TotalResources = resources
	ppr.Status.PipelineRunID = runID
	ppr.Status.PipelineRunURL = runURL
	ppr.Status.ExperimentId = experimentID
	ppr.Status.ExperimentName = experimentName
	ppr.Status.Parameters = parameters

	// Timestamp del placement
	now := metav1.Now()
	ppr.Status.PlacementTime = &now

	// Prepara condition "Placed"
	condition := metav1.Condition{
		Type:               "Placed",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             "PlacementSuccessful",
		Message:            decision,
	}

	// Modifica condition in caso di fallimento
	if !success {
		condition.Status = metav1.ConditionFalse
		condition.Reason = "PlacementFailed"
		if errorMsg != "" {
			condition.Message = errorMsg
		} else {
			condition.Message = decision
		}
	}

	// Aggiorna o appendi la condition "Placed"
	found := false
	for i, cond := range ppr.Status.Conditions {
		if cond.Type == "Placed" {
			ppr.Status.Conditions[i] = condition
			found = true
			break
		}
	}
	if !found {
		ppr.Status.Conditions = append(ppr.Status.Conditions, condition)
	}

	// Persisti lo status aggiornato
	return r.Status().Update(ctx, ppr)
}

// ============================================================================
// CONTROLLER SETUP
// ============================================================================

// SetupWithManager configura e registra il controller con il Manager.
// Inizializza tutti i componenti necessari:
//   - Parser per pipeline Kubeflow IR v2.1.0
//   - Strategie di placement (cloud-only, data-locality, simple-heuristic)
//   - ClusterManager per comunicazione multi-cluster
//   - MetricsCollector per raccolta metriche dai cluster
//   - KubeflowManager per interazioni con API Kubeflow
//
// Questo metodo viene chiamato una volta all'avvio del controller.
//
// Parametri:
//   - mgr: controller-runtime Manager
//
// Ritorna:
//   - error: errore in caso di fallimento inizializzazione
func (r *PipelinePlacementRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Inizializza parser per Kubeflow Pipeline IR
	r.pipelineParser = pipeline.NewParser()

	// Inizializza strategie di placement disponibili
	r.pipelineStrategies = map[string]pipeline.PipelineStrategy{
		"cloud-only-pipeline":       pipeline.NewCloudOnlyPipelineStrategy(),
		"data-locality-pipeline":    pipeline.NewDataLocalityPipelineStrategy(),
		"simple-heuristic-pipeline": pipeline.NewSimplePipelineHeuristicStrategy(),
	}

	// Inizializza ClusterManager per accesso multi-cluster
	ctx := context.Background()
	clusterManager, err := multicluster.NewClusterManager(
		ctx,
		mgr.GetConfig(),
		"cluster-kubeconfigs",   // Nome del Secret con kubeconfig
		"cloudcontinuum-system", // Namespace del Secret
		mgr.GetScheme(),
	)
	if err != nil {
		return fmt.Errorf("failed to initialize cluster manager: %w", err)
	}
	r.ClusterManager = clusterManager

	// Inizializza MetricsCollector per raccolta metriche reali
	metricsCollector := metrics.NewRealMetricsCollector(
		clusterManager.ClusterClients,
		metrics.DefaultConfig(),
	)
	r.MetricsCollector = metricsCollector
	metricsCollector.Start(ctx) // Avvia goroutine di raccolta periodica

	// Inizializza KubeflowManager per interazioni con API
	r.kubeflowManager = kubeflow.NewManager("kubeflow") // Namespace Kubeflow

	// Log configurazione
	ctrl.Log.Info("✅ PipelinePlacementRequest controller initialized",
		"clusters", clusterManager.ListClusters(),
		"strategies", len(r.pipelineStrategies))

	// Registra controller con Manager
	return ctrl.NewControllerManagedBy(mgr).
		For(&orchestratorv1alpha1.PipelinePlacementRequest{}).
		Named("pipelineplacementrequest").
		Complete(r)
}
