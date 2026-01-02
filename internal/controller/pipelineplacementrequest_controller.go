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
	"github.com/go-logr/logr"
	"io"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"net/http"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"time"

	orchestratorv1alpha1 "github.com/vincenzo426/cloudcontinuum-orchestrator/api/v1alpha1"
	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/kubeflow"
	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/metrics"
	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/multicluster"
	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/pipeline"
	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

// Costanti di configurazione
const (
	requeueDelayShort = 30 * time.Second
	requeueDelayLong  = 60 * time.Second
	httpTimeout       = 30 * time.Second
	runNameTimeFormat = "20060102-150405"

	clusterConfigSecretName = "cluster-kubeconfigs"
	systemNamespace         = "cloudcontinuum-system"
	kubeflowNamespace       = "kubeflow"
)

// PipelinePlacementRequestReconciler gestisce il ciclo di vita delle PipelinePlacementRequest.
type PipelinePlacementRequestReconciler struct {
	client.Client
	Scheme             *runtime.Scheme
	pipelineStrategies map[string]pipeline.PipelineStrategy
	ClusterManager     *multicluster.ClusterManager
	MetricsCollector   metrics.Collector
	pipelineParser     *pipeline.Parser
	kubeflowManager    *kubeflow.Manager
}

// StatusUpdate contiene i dati per l'aggiornamento dello status.
type StatusUpdate struct {
	TargetCluster  string
	Decision       string
	Resources      *orchestratorv1alpha1.PipelineResourcesSummary
	RunID          string
	RunURL         string
	ExperimentID   string
	ExperimentName string
	Parameters     map[string]string
	Success        bool
	ErrorMsg       string
}

// RBAC permissions
// +kubebuilder:rbac:groups=orchestrator.cloudcontinuum.io,resources=pipelineplacementrequests,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=orchestrator.cloudcontinuum.io,resources=pipelineplacementrequests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=orchestrator.cloudcontinuum.io,resources=pipelineplacementrequests/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch

// Reconcile gestisce il ciclo di vita delle PipelinePlacementRequest.
// Flusso: fetch pipeline → parse → raccolta metriche → placement → deploy → update status
func (r *PipelinePlacementRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("🔄 Reconciling PipelinePlacementRequest", "name", req.Name, "namespace", req.Namespace)

	// Recupera la risorsa
	ppr, err := r.fetchPipelinePlacementRequest(ctx, req)
	if ppr == nil || err != nil {
		return ctrl.Result{}, err
	}

	// Verifica idempotenza
	if r.isAlreadyProcessed(ppr, logger) {
		return ctrl.Result{}, nil
	}

	// Fetch e parse della pipeline
	pipelineIR, pipelineYAML, err := r.fetchAndParsePipeline(ctx, ppr)
	if err != nil {
		return r.handleError(ctx, ppr, err, "Failed to fetch/parse pipeline", requeueDelayShort)
	}

	logger.Info("✅ Pipeline parsed", "name", pipelineIR.PipelineInfo.Name, "executors", len(pipelineIR.DeploymentSpec.Executors))

	// Decisione di placement
	targetCluster, decision, resources, err := r.makePlacementDecision(ctx, ppr, pipelineIR)
	if err != nil {
		return r.handleError(ctx, ppr, err, "Placement selection failed", requeueDelayShort)
	}

	logger.Info("🎯 Placement decision", "strategy", ppr.Spec.PlacementStrategy, "cluster", targetCluster)

	// Esecuzione pipeline su Kubeflow
	runID, runURL, err := r.executePipeline(ctx, ppr, pipelineIR, targetCluster, pipelineYAML)
	if err != nil {
		logger.Error(err, "❌ Failed to execute pipeline")
		update := StatusUpdate{
			TargetCluster:  targetCluster,
			Decision:       decision,
			Resources:      resources,
			ExperimentID:   ppr.Spec.ExperimentId,
			ExperimentName: ppr.Spec.ExperimentName,
			Parameters:     ppr.Spec.Parameters,
			Success:        false,
			ErrorMsg:       fmt.Sprintf("Failed to execute pipeline: %v", err),
		}
		r.updateStatus(ctx, ppr, update)
		return ctrl.Result{RequeueAfter: requeueDelayLong}, err
	}

	logger.Info("✅ Pipeline executed", "cluster", targetCluster, "runID", runID, "runURL", runURL)

	// Aggiornamento status finale
	return r.finalizeSuccess(ctx, ppr, targetCluster, decision, resources, runID, runURL)
}

// fetchPipelinePlacementRequest recupera la risorsa dal cluster.
func (r *PipelinePlacementRequestReconciler) fetchPipelinePlacementRequest(ctx context.Context, req ctrl.Request) (*orchestratorv1alpha1.PipelinePlacementRequest, error) {
	logger := log.FromContext(ctx)
	ppr := &orchestratorv1alpha1.PipelinePlacementRequest{}

	if err := r.Get(ctx, req.NamespacedName, ppr); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("✅ PipelinePlacementRequest not found, likely deleted")
			return nil, nil
		}
		logger.Error(err, "❌ Failed to get PipelinePlacementRequest")
		return nil, err
	}

	return ppr, nil
}

// isAlreadyProcessed verifica se la risorsa è già stata processata (idempotenza).
func (r *PipelinePlacementRequestReconciler) isAlreadyProcessed(ppr *orchestratorv1alpha1.PipelinePlacementRequest, logger logr.Logger) bool {
	if ppr.Status.TargetCluster != "" {
		logger.Info("✅ Already placed (idempotent skip)", "cluster", ppr.Status.TargetCluster, "runID", ppr.Status.PipelineRunID)
		return true
	}
	return false
}

// fetchAndParsePipeline recupera e parsifica il YAML della pipeline.
func (r *PipelinePlacementRequestReconciler) fetchAndParsePipeline(ctx context.Context, ppr *orchestratorv1alpha1.PipelinePlacementRequest) (*pipeline.PipelineIR, string, error) {
	logger := log.FromContext(ctx)

	pipelineYAML, err := r.fetchPipelineYAML(ctx, ppr)
	if err != nil {
		logger.Error(err, "❌ Failed to fetch pipeline YAML")
		return nil, "", fmt.Errorf("fetch YAML: %w", err)
	}

	logger.V(1).Info("📄 Pipeline YAML fetched", "sourceType", r.getPipelineSourceType(ppr), "size", len(pipelineYAML))

	pipelineIR, err := r.pipelineParser.ParsePipelineIR([]byte(pipelineYAML))
	if err != nil {
		logger.Error(err, "❌ Failed to parse pipeline YAML")
		return nil, "", fmt.Errorf("parse YAML: %w", err)
	}

	return pipelineIR, pipelineYAML, nil
}

// makePlacementDecision esegue la strategia di placement e calcola le risorse.
func (r *PipelinePlacementRequestReconciler) makePlacementDecision(ctx context.Context, ppr *orchestratorv1alpha1.PipelinePlacementRequest, pipelineIR *pipeline.PipelineIR) (string, string, *orchestratorv1alpha1.PipelineResourcesSummary, error) {
	logger := log.FromContext(ctx)

	// Raccolta metriche
	clusterMetrics, err := r.collectMetrics(ctx)
	if err != nil {
		logger.Error(err, "❌ Failed to collect metrics")
		return "", "", nil, err
	}

	logger.V(1).Info("📊 Cluster metrics collected", "clusters", len(clusterMetrics.Clusters))

	// Selezione strategia
	strategy, ok := r.pipelineStrategies[ppr.Spec.PlacementStrategy]
	if !ok {
		err := fmt.Errorf("unknown placement strategy: %s", ppr.Spec.PlacementStrategy)
		logger.Error(err, "❌ Invalid strategy")
		return "", "", nil, err
	}

	// Esecuzione placement
	targetCluster, decision, err := strategy.SelectCluster(ctx, pipelineIR, ppr.Spec.DataLocation, clusterMetrics)
	if err != nil {
		logger.Error(err, "❌ Failed to select cluster", "strategy", ppr.Spec.PlacementStrategy)
		return "", "", nil, err
	}

	// Calcolo risorse
	totalResources := pipeline.CalculatePipelineResources(pipelineIR, r.pipelineParser)
	resourcesSummary := &orchestratorv1alpha1.PipelineResourcesSummary{
		ExecutorCount: totalResources.ExecutorCount,
		TotalCPU:      totalResources.TotalCPU,
		TotalMemory:   totalResources.TotalMemory,
		TotalGPU:      totalResources.TotalGPU,
	}

	logger.Info("💾 Pipeline resources", "executors", resourcesSummary.ExecutorCount,
		"cpu", resourcesSummary.TotalCPU, "memory", resourcesSummary.TotalMemory, "gpu", resourcesSummary.TotalGPU)

	return targetCluster, decision, resourcesSummary, nil
}

// executePipeline esegue la pipeline sul cluster target tramite Kubeflow API.
func (r *PipelinePlacementRequestReconciler) executePipeline(ctx context.Context, ppr *orchestratorv1alpha1.PipelinePlacementRequest, pipelineIR *pipeline.PipelineIR, targetCluster, pipelineYAML string) (string, string, error) {
	logger := log.FromContext(ctx)

	runName := fmt.Sprintf("%s-%s", ppr.Name, time.Now().Format(runNameTimeFormat))

	logger.Info("🚀 Triggering pipeline execution", "cluster", targetCluster, "pipeline", pipelineIR.PipelineInfo.Name, "runName", runName)

	// Conversione parametri
	params := make(map[string]interface{})
	for k, v := range ppr.Spec.Parameters {
		params[k] = v
	}

	// Chiamata Kubeflow API
	runID, runURL, err := r.kubeflowManager.UploadAndRunPipeline(
		ctx,
		targetCluster,
		pipelineIR.PipelineInfo.Name,
		[]byte(pipelineYAML),
		runName,
		ppr.Spec.ExperimentId,
		ppr.Spec.ExperimentName,
		params,
	)

	return runID, runURL, err
}

// handleError gestisce errori durante la riconciliazione.
func (r *PipelinePlacementRequestReconciler) handleError(ctx context.Context, ppr *orchestratorv1alpha1.PipelinePlacementRequest, err error, msg string, requeueAfter time.Duration) (ctrl.Result, error) {
	log.FromContext(ctx).Error(err, "❌ "+msg)

	update := StatusUpdate{
		Parameters: ppr.Spec.Parameters,
		Success:    false,
		ErrorMsg:   msg,
	}
	r.updateStatus(ctx, ppr, update)

	return ctrl.Result{RequeueAfter: requeueAfter}, err
}

// finalizeSuccess completa con successo la riconciliazione.
func (r *PipelinePlacementRequestReconciler) finalizeSuccess(ctx context.Context, ppr *orchestratorv1alpha1.PipelinePlacementRequest, targetCluster, decision string, resources *orchestratorv1alpha1.PipelineResourcesSummary, runID, runURL string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	update := StatusUpdate{
		TargetCluster:  targetCluster,
		Decision:       decision,
		Resources:      resources,
		RunID:          runID,
		RunURL:         runURL,
		ExperimentID:   ppr.Spec.ExperimentId,
		ExperimentName: ppr.Spec.ExperimentName,
		Parameters:     ppr.Spec.Parameters,
		Success:        true,
	}

	if err := r.updateStatus(ctx, ppr, update); err != nil {
		logger.Error(err, "❌ Failed to update status")
		return ctrl.Result{}, err
	}

	logger.Info("✅ Reconciled successfully", "cluster", targetCluster, "runID", runID)
	return ctrl.Result{}, nil
}

// fetchPipelineYAML recupera il YAML della pipeline dalla sorgente configurata.
// Supporta: inline, ConfigMap (TODO), URL HTTP/HTTPS.
func (r *PipelinePlacementRequestReconciler) fetchPipelineYAML(ctx context.Context, ppr *orchestratorv1alpha1.PipelinePlacementRequest) (string, error) {
	source := ppr.Spec.PipelineSource

	if source.Inline != "" {
		return source.Inline, nil
	}

	// TODO: implementare ConfigMap support
	// if source.ConfigMapRef != nil { ... }

	if source.URL != "" {
		return r.fetchFromURL(ctx, source.URL)
	}

	return "", fmt.Errorf("no valid pipeline source specified")
}

// fetchFromURL scarica il contenuto YAML da un URL HTTP/HTTPS.
func (r *PipelinePlacementRequestReconciler) fetchFromURL(ctx context.Context, url string) (string, error) {
	httpCtx, cancel := context.WithTimeout(ctx, httpTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(httpCtx, "GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to fetch URL %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP request failed with status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	return string(body), nil
}

// collectMetrics raccoglie metriche dai cluster disponibili.
// Usa metriche mock come fallback in caso di errore.
func (r *PipelinePlacementRequestReconciler) collectMetrics(ctx context.Context) (*placement.ClusterMetrics, error) {
	logger := log.FromContext(ctx)

	metrics, err := r.MetricsCollector.CollectMetrics(ctx)
	if err != nil {
		logger.Error(err, "⚠️  Failed to collect real metrics, using fallback")
		return r.collectMockMetrics(ctx), nil
	}

	logger.V(1).Info("📊 Real metrics collected", "clusters", len(metrics.Clusters))
	return metrics, nil
}

// collectMockMetrics fornisce metriche di fallback per testing.
func (r *PipelinePlacementRequestReconciler) collectMockMetrics(ctx context.Context) *placement.ClusterMetrics {
	metrics := placement.NewClusterMetrics()

	metrics.SetCluster("cloud_cluster", &placement.ClusterMetric{
		Name:            "cloud_cluster",
		CPUCapacity:     16000,
		CPUAvailable:    12000,
		MemoryCapacity:  32 * 1024 * 1024 * 1024,
		MemoryAvailable: 24 * 1024 * 1024 * 1024,
		Available:       true,
	})

	metrics.SetCluster("edge_cluster_1", &placement.ClusterMetric{
		Name:            "edge_cluster_1",
		CPUCapacity:     4000,
		CPUAvailable:    3000,
		MemoryCapacity:  8 * 1024 * 1024 * 1024,
		MemoryAvailable: 6 * 1024 * 1024 * 1024,
		Available:       true,
	})

	metrics.SetCluster("edge_cluster_2", &placement.ClusterMetric{
		Name:            "edge_cluster_2",
		CPUCapacity:     4000,
		CPUAvailable:    2500,
		MemoryCapacity:  8 * 1024 * 1024 * 1024,
		MemoryAvailable: 5 * 1024 * 1024 * 1024,
		Available:       true,
	})

	return metrics
}

// getPipelineSourceType ritorna il tipo di sorgente per logging.
func (r *PipelinePlacementRequestReconciler) getPipelineSourceType(ppr *orchestratorv1alpha1.PipelinePlacementRequest) string {
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

// updateStatus aggiorna lo status della PipelinePlacementRequest.
func (r *PipelinePlacementRequestReconciler) updateStatus(ctx context.Context, ppr *orchestratorv1alpha1.PipelinePlacementRequest, update StatusUpdate) error {
	ppr.Status.TargetCluster = update.TargetCluster
	ppr.Status.PlacementDecision = update.Decision
	ppr.Status.TotalResources = update.Resources
	ppr.Status.PipelineRunID = update.RunID
	ppr.Status.PipelineRunURL = update.RunURL
	ppr.Status.ExperimentId = update.ExperimentID
	ppr.Status.ExperimentName = update.ExperimentName
	ppr.Status.Parameters = update.Parameters

	now := metav1.Now()
	ppr.Status.PlacementTime = &now

	condition := metav1.Condition{
		Type:               "Placed",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             "PlacementSuccessful",
		Message:            update.Decision,
	}

	if !update.Success {
		condition.Status = metav1.ConditionFalse
		condition.Reason = "PlacementFailed"
		if update.ErrorMsg != "" {
			condition.Message = update.ErrorMsg
		} else {
			condition.Message = update.Decision
		}
	}

	// Aggiorna o appendi condition
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

	return r.Status().Update(ctx, ppr)
}

// SetupWithManager configura il controller con il Manager.
func (r *PipelinePlacementRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Inizializza parser
	r.pipelineParser = pipeline.NewParser()

	// Inizializza strategie di placement
	r.pipelineStrategies = map[string]pipeline.PipelineStrategy{
		"cloud-only-pipeline":       pipeline.NewCloudOnlyPipelineStrategy(),
		"data-locality-pipeline":    pipeline.NewDataLocalityPipelineStrategy(),
		"simple-heuristic-pipeline": pipeline.NewSimplePipelineHeuristicStrategy(),
	}

	// Inizializza ClusterManager
	ctx := context.Background()
	clusterManager, err := multicluster.NewClusterManager(
		ctx,
		mgr.GetConfig(),
		clusterConfigSecretName,
		systemNamespace,
		mgr.GetScheme(),
	)
	if err != nil {
		return fmt.Errorf("failed to initialize cluster manager: %w", err)
	}
	r.ClusterManager = clusterManager

	// Inizializza MetricsCollector
	metricsCollector := metrics.NewRealMetricsCollector(
		clusterManager.ClusterClients,
		metrics.DefaultConfig(),
	)
	r.MetricsCollector = metricsCollector
	metricsCollector.Start(ctx)

	// Inizializza KubeflowManager
	r.kubeflowManager = kubeflow.NewManager(kubeflowNamespace)

	ctrl.Log.Info("✅ PipelinePlacementRequest controller initialized",
		"clusters", clusterManager.ListClusters(),
		"strategies", len(r.pipelineStrategies))

	return ctrl.NewControllerManagedBy(mgr).
		For(&orchestratorv1alpha1.PipelinePlacementRequest{}).
		Named("pipelineplacementrequest").
		Complete(r)
}
