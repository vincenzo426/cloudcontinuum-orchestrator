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

	corev1 "k8s.io/api/core/v1"
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

// RBAC permissions
// +kubebuilder:rbac:groups=orchestrator.cloudcontinuum.io,resources=pipelineplacementrequests,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=orchestrator.cloudcontinuum.io,resources=pipelineplacementrequests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=orchestrator.cloudcontinuum.io,resources=pipelineplacementrequests/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch

// Reconcile gestisce il ciclo di vita delle PipelinePlacementRequest.
func (r *PipelinePlacementRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("[RECONCILE] Started", "name", req.Name, "namespace", req.Namespace)

	// Fetch risorsa
	ppr := &orchestratorv1alpha1.PipelinePlacementRequest{}
	if err := r.Get(ctx, req.NamespacedName, ppr); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("[RECONCILE] Resource not found (deleted)")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Verifica idempotenza
	if ppr.Status.TargetCluster != "" {
		logger.Info("[RECONCILE] Already processed", "cluster", ppr.Status.TargetCluster)
		return ctrl.Result{}, nil
	}

	// Fetch e parse pipeline
	pipelineIR, pipelineYAML, err := r.fetchAndParsePipeline(ctx, ppr)
	if err != nil {
		return r.failWithStatus(ctx, ppr, err, "Pipeline fetch/parse failed", requeueDelayShort)
	}

	logger.Info("[PARSE] Pipeline parsed", "name", pipelineIR.PipelineInfo.Name, "executors", len(pipelineIR.DeploymentSpec.Executors))

	// Decisione placement
	targetCluster, decision, resources, err := r.makePlacementDecision(ctx, ppr, pipelineIR)
	if err != nil {
		return r.failWithStatus(ctx, ppr, err, "Placement selection failed", requeueDelayShort)
	}

	logger.Info("[PLACEMENT] Decision made", "strategy", ppr.Spec.PlacementStrategy, "cluster", targetCluster)

	// Esecuzione pipeline
	runID, runURL, err := r.executePipeline(ctx, ppr, pipelineIR, targetCluster, pipelineYAML)
	if err != nil {
		return r.failWithStatus(ctx, ppr, err, "Pipeline execution failed", requeueDelayLong)
	}

	logger.Info("[SUCCESS] Pipeline deployed", "cluster", targetCluster, "runID", runID)

	// Update status finale
	return r.updateStatusAndComplete(ctx, ppr, targetCluster, decision, resources, runID, runURL, true, "")
}

// fetchAndParsePipeline recupera e parsifica il YAML della pipeline.
func (r *PipelinePlacementRequestReconciler) fetchAndParsePipeline(ctx context.Context, ppr *orchestratorv1alpha1.PipelinePlacementRequest) (*pipeline.PipelineIR, string, error) {
	logger := log.FromContext(ctx)

	// Fetch YAML
	pipelineYAML, err := r.fetchPipelineYAML(ctx, ppr)
	if err != nil {
		return nil, "", fmt.Errorf("fetch YAML: %w", err)
	}

	logger.V(1).Info("[DEBUG] Pipeline YAML fetched", "source", r.sourceType(ppr), "size", len(pipelineYAML))

	// Parse YAML
	pipelineIR, err := r.pipelineParser.ParsePipelineIR([]byte(pipelineYAML))
	if err != nil {
		return nil, "", fmt.Errorf("parse YAML: %w", err)
	}

	// Analizza struttura pipeline (usa funzioni non utilizzate)
	executors := r.pipelineParser.GetExecutorNames(pipelineIR)
	tasks := r.pipelineParser.GetTaskNames(pipelineIR)
	dependencies := r.pipelineParser.GetTaskDependencies(pipelineIR)

	logger.V(1).Info("[DEBUG] Pipeline structure",
		"executors", len(executors),
		"tasks", len(tasks),
		"hasDependencies", len(dependencies) > 0)

	return pipelineIR, pipelineYAML, nil
}

// makePlacementDecision esegue la strategia di placement e calcola le risorse.
func (r *PipelinePlacementRequestReconciler) makePlacementDecision(ctx context.Context, ppr *orchestratorv1alpha1.PipelinePlacementRequest, pipelineIR *pipeline.PipelineIR) (string, string, *orchestratorv1alpha1.PipelineResourcesSummary, error) {
	logger := log.FromContext(ctx)

	// Raccolta metriche
	clusterMetrics, err := r.collectMetrics(ctx)
	if err != nil {
		return "", "", nil, err
	}

	logger.V(1).Info("[METRICS] Collected", "clusters", len(clusterMetrics.Clusters))

	// Selezione strategia
	strategy, ok := r.pipelineStrategies[ppr.Spec.PlacementStrategy]
	if !ok {
		return "", "", nil, fmt.Errorf("unknown placement strategy: %s", ppr.Spec.PlacementStrategy)
	}

	// Esecuzione placement
	targetCluster, decision, err := strategy.SelectCluster(ctx, pipelineIR, ppr.Spec.DataLocation, clusterMetrics)
	if err != nil {
		return "", "", nil, fmt.Errorf("cluster selection failed: %w", err)
	}

	// Calcolo risorse
	totalResources := pipeline.CalculatePipelineResources(pipelineIR, r.pipelineParser)

	// Log summary dettagliato (usa NewPipelinePlacement e Summary)
	placementResult := pipeline.NewPipelinePlacement(targetCluster, decision, totalResources)
	logger.Info("[PLACEMENT] Complete",
		"cluster", targetCluster,
		"strategy", ppr.Spec.PlacementStrategy,
		"executors", totalResources.ExecutorCount,
		"cpuCores", fmt.Sprintf("%.2f", float64(totalResources.TotalCPU)/1000.0))
	logger.V(1).Info("[DEBUG] Placement details\n" + placementResult.Summary())

	// Converti per CRD
	return targetCluster, decision, r.toResourcesSummary(totalResources), nil
}

// executePipeline esegue la pipeline sul cluster target.
func (r *PipelinePlacementRequestReconciler) executePipeline(ctx context.Context, ppr *orchestratorv1alpha1.PipelinePlacementRequest, pipelineIR *pipeline.PipelineIR, targetCluster, pipelineYAML string) (string, string, error) {
	logger := log.FromContext(ctx)

	runName := fmt.Sprintf("%s-%s", ppr.Name, time.Now().Format(runNameTimeFormat))
	logger.Info("[EXECUTION] Triggering pipeline", "cluster", targetCluster, "pipeline", pipelineIR.PipelineInfo.Name)

	// Conversione parametri
	params := make(map[string]interface{})
	for k, v := range ppr.Spec.Parameters {
		params[k] = v
	}

	return r.kubeflowManager.UploadAndRunPipeline(
		ctx, targetCluster, pipelineIR.PipelineInfo.Name,
		[]byte(pipelineYAML), runName,
		ppr.Spec.ExperimentId, ppr.Spec.ExperimentName, params,
	)
}

// fetchPipelineYAML recupera il YAML della pipeline dalla sorgente configurata.
func (r *PipelinePlacementRequestReconciler) fetchPipelineYAML(ctx context.Context, ppr *orchestratorv1alpha1.PipelinePlacementRequest) (string, error) {
	source := ppr.Spec.PipelineSource

	if source.Inline != "" {
		return source.Inline, nil
	}

	if source.ConfigMapRef != nil {
		return r.fetchFromConfigMap(ctx, ppr.Namespace, source.ConfigMapRef)
	}

	if source.URL != "" {
		return r.fetchFromURL(ctx, source.URL)
	}

	return "", fmt.Errorf("no valid pipeline source specified")
}

// fetchFromURL scarica il contenuto YAML da un URL.
func (r *PipelinePlacementRequestReconciler) fetchFromURL(ctx context.Context, url string) (string, error) {
	httpCtx, cancel := context.WithTimeout(ctx, httpTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(httpCtx, "GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to fetch URL: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	return string(body), nil
}

// fetchFromConfigMap recupera il contenuto YAML da una ConfigMap.
func (r *PipelinePlacementRequestReconciler) fetchFromConfigMap(ctx context.Context, namespace string, ref *orchestratorv1alpha1.ConfigMapReference) (string, error) {
	logger := log.FromContext(ctx)

	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: namespace}, cm); err != nil {
		if errors.IsNotFound(err) {
			return "", fmt.Errorf("configmap %s/%s not found", namespace, ref.Name)
		}
		return "", fmt.Errorf("failed to get configmap: %w", err)
	}

	content, ok := cm.Data[ref.Key]
	if !ok {
		return "", fmt.Errorf("key %s not found in configmap %s/%s", ref.Key, namespace, ref.Name)
	}

	logger.V(1).Info("[DEBUG] Pipeline loaded from ConfigMap", "configmap", ref.Name, "key", ref.Key, "size", len(content))

	return content, nil
}

// collectMetrics raccoglie metriche dai cluster.
func (r *PipelinePlacementRequestReconciler) collectMetrics(ctx context.Context) (*placement.ClusterMetrics, error) {
	logger := log.FromContext(ctx)

	metrics, err := r.MetricsCollector.CollectMetrics(ctx)
	if err != nil {
		logger.Error(err, "[WARN] Using fallback metrics")
		return r.mockMetrics(), nil
	}

	logger.V(1).Info("[METRICS] Real metrics collected", "clusters", len(metrics.Clusters))
	return metrics, nil
}

// mockMetrics fornisce metriche di fallback.
func (r *PipelinePlacementRequestReconciler) mockMetrics() *placement.ClusterMetrics {
	metrics := placement.NewClusterMetrics()
	metrics.SetCluster("cloud_cluster", &placement.ClusterMetric{
		Name: "cloud_cluster", CPUCapacity: 16000, CPUAvailable: 12000,
		MemoryCapacity: 32 * 1024 * 1024 * 1024, MemoryAvailable: 24 * 1024 * 1024 * 1024, Available: true,
	})
	metrics.SetCluster("edge_cluster_1", &placement.ClusterMetric{
		Name: "edge_cluster_1", CPUCapacity: 4000, CPUAvailable: 3000,
		MemoryCapacity: 8 * 1024 * 1024 * 1024, MemoryAvailable: 6 * 1024 * 1024 * 1024, Available: true,
	})
	metrics.SetCluster("edge_cluster_2", &placement.ClusterMetric{
		Name: "edge_cluster_2", CPUCapacity: 4000, CPUAvailable: 2500,
		MemoryCapacity: 8 * 1024 * 1024 * 1024, MemoryAvailable: 5 * 1024 * 1024 * 1024, Available: true,
	})
	return metrics
}

// failWithStatus gestisce errori aggiornando lo status.
func (r *PipelinePlacementRequestReconciler) failWithStatus(ctx context.Context, ppr *orchestratorv1alpha1.PipelinePlacementRequest, err error, msg string, requeueAfter time.Duration) (ctrl.Result, error) {
	log.FromContext(ctx).Error(err, "[ERROR] "+msg)
	r.updateStatusAndComplete(ctx, ppr, "", "", nil, "", "", false, msg)
	return ctrl.Result{RequeueAfter: requeueAfter}, err
}

// updateStatusAndComplete aggiorna lo status e completa la riconciliazione.
func (r *PipelinePlacementRequestReconciler) updateStatusAndComplete(ctx context.Context, ppr *orchestratorv1alpha1.PipelinePlacementRequest, targetCluster, decision string, resources *orchestratorv1alpha1.PipelineResourcesSummary, runID, runURL string, success bool, errorMsg string) (ctrl.Result, error) {
	ppr.Status.TargetCluster = targetCluster
	ppr.Status.PlacementDecision = decision
	ppr.Status.TotalResources = resources
	ppr.Status.PipelineRunID = runID
	ppr.Status.PipelineRunURL = runURL
	ppr.Status.ExperimentId = ppr.Spec.ExperimentId
	ppr.Status.ExperimentName = ppr.Spec.ExperimentName
	ppr.Status.Parameters = ppr.Spec.Parameters

	now := metav1.Now()
	ppr.Status.PlacementTime = &now

	condition := metav1.Condition{
		Type:               "Placed",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             "PlacementSuccessful",
		Message:            decision,
	}

	if !success {
		condition.Status = metav1.ConditionFalse
		condition.Reason = "PlacementFailed"
		if errorMsg != "" {
			condition.Message = errorMsg
		}
	}

	// Update condition
	updated := false
	for i, cond := range ppr.Status.Conditions {
		if cond.Type == "Placed" {
			ppr.Status.Conditions[i] = condition
			updated = true
			break
		}
	}
	if !updated {
		ppr.Status.Conditions = append(ppr.Status.Conditions, condition)
	}

	if err := r.Status().Update(ctx, ppr); err != nil {
		return ctrl.Result{}, err
	}

	if success {
		log.FromContext(ctx).Info("[SUCCESS] Reconciled", "cluster", targetCluster, "runID", runID)
	}

	return ctrl.Result{}, nil
}

// Helper functions

func (r *PipelinePlacementRequestReconciler) sourceType(ppr *orchestratorv1alpha1.PipelinePlacementRequest) string {
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

func (r *PipelinePlacementRequestReconciler) toResourcesSummary(res pipeline.PipelineResources) *orchestratorv1alpha1.PipelineResourcesSummary {
	return &orchestratorv1alpha1.PipelineResourcesSummary{
		ExecutorCount: res.ExecutorCount,
		TotalCPU:      res.TotalCPU,
		TotalMemory:   res.TotalMemory,
		TotalGPU:      res.TotalGPU,
	}
}

// SetupWithManager configura il controller.
func (r *PipelinePlacementRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.pipelineParser = pipeline.NewParser()

	r.pipelineStrategies = map[string]pipeline.PipelineStrategy{
		"cloud-only-pipeline":       pipeline.NewCloudOnlyPipelineStrategy(),
		"data-locality-pipeline":    pipeline.NewDataLocalityPipelineStrategy(),
		"simple-heuristic-pipeline": pipeline.NewSimplePipelineHeuristicStrategy(),
	}

	ctx := context.Background()
	clusterManager, err := multicluster.NewClusterManager(ctx, mgr.GetConfig(), clusterConfigSecretName, systemNamespace, mgr.GetScheme())
	if err != nil {
		return fmt.Errorf("failed to initialize cluster manager: %w", err)
	}
	r.ClusterManager = clusterManager

	metricsCollector := metrics.NewRealMetricsCollector(clusterManager.ClusterClients, metrics.DefaultConfig())
	r.MetricsCollector = metricsCollector
	metricsCollector.Start(ctx)

	r.kubeflowManager = kubeflow.NewManager(kubeflowNamespace)

	ctrl.Log.Info("[INIT] PipelinePlacementRequest controller initialized", "clusters", clusterManager.ListClusters(), "strategies", len(r.pipelineStrategies))

	return ctrl.NewControllerManagedBy(mgr).
		For(&orchestratorv1alpha1.PipelinePlacementRequest{}).
		Named("pipelineplacementrequest").
		Complete(r)
}
