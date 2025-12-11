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
	//corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	//"k8s.io/apimachinery/pkg/types"
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

// PipelinePlacementRequestReconciler reconciles a PipelinePlacementRequest object
type PipelinePlacementRequestReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Pipeline strategies
	pipelineStrategies map[string]pipeline.PipelineStrategy

	// Multi-cluster manager
	ClusterManager *multicluster.ClusterManager

	// Metrics collector
	MetricsCollector metrics.Collector

	// Pipeline parser
	pipelineParser *pipeline.Parser

	// Kubeflow manager
	kubeflowManager *kubeflow.Manager
}

// +kubebuilder:rbac:groups=orchestrator.cloudcontinuum.io,resources=pipelineplacementrequests,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=orchestrator.cloudcontinuum.io,resources=pipelineplacementrequests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=orchestrator.cloudcontinuum.io,resources=pipelineplacementrequests/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop
func (r *PipelinePlacementRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling PipelinePlacementRequest", "name", req.Name, "namespace", req.Namespace)

	logger.Info("")
	// =========================================================================
	// STEP 1: Fetch the PipelinePlacementRequest
	// =========================================================================
	ppr := &orchestratorv1alpha1.PipelinePlacementRequest{}
	if err := r.Get(ctx, req.NamespacedName, ppr); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("PipelinePlacementRequest not found, likely deleted")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get PipelinePlacementRequest")
		return ctrl.Result{}, err
	}

	// =========================================================================
	// STEP 2: Check if already placed (idempotenza)
	// =========================================================================
	if ppr.Status.TargetCluster != "" {
		logger.Info("PipelinePlacementRequest already placed",
			"cluster", ppr.Status.TargetCluster,
			"runID", ppr.Status.PipelineRunID)
		return ctrl.Result{}, nil
	}

	// =========================================================================
	// STEP 3: Fetch pipeline YAML from source
	// =========================================================================
	pipelineYAML, err := r.fetchPipelineYAML(ctx, ppr)
	if err != nil {
		logger.Error(err, "Failed to fetch pipeline YAML")
		r.updateStatus(ctx, ppr, "", "", nil, "", "", "", "", false, "Failed to fetch pipeline YAML")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}

	logger.V(1).Info("Pipeline YAML fetched successfully",
		"sourceType", r.getPipelineSourceType(ppr),
		"size", len(pipelineYAML))

	// =========================================================================
	// STEP 4: Parse pipeline IR
	// =========================================================================
	pipelineIR, err := r.pipelineParser.ParsePipelineIR([]byte(pipelineYAML))
	if err != nil {
		logger.Error(err, "Failed to parse pipeline YAML")
		r.updateStatus(ctx, ppr, "", "", nil, "", "", "", "", false, "Failed to parse pipeline YAML")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}

	logger.Info("Pipeline parsed successfully",
		"pipelineName", pipelineIR.PipelineInfo.Name,
		"executors", len(pipelineIR.DeploymentSpec.Executors),
		"schemaVersion", pipelineIR.SchemaVersion)

	// =========================================================================
	// STEP 5: Collect cluster metrics
	// =========================================================================
	clusterMetrics, err := r.collectMetrics(ctx)
	if err != nil {
		logger.Error(err, "Failed to collect metrics")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}

	logger.V(1).Info("Cluster metrics collected",
		"clusters", len(clusterMetrics.Clusters))

	// =========================================================================
	// STEP 6: Select placement strategy
	// =========================================================================
	strategy, ok := r.pipelineStrategies[ppr.Spec.PlacementStrategy]
	if !ok {
		err := fmt.Errorf("unknown placement strategy: %s", ppr.Spec.PlacementStrategy)
		logger.Error(err, "Invalid strategy")
		r.updateStatus(ctx, ppr, "", "", nil, "", "", "", "", false, "Invalid placement strategy")
		return ctrl.Result{}, err
	}

	// =========================================================================
	// STEP 7: Execute placement decision
	// =========================================================================
	targetCluster, decision, err := strategy.SelectCluster(
		ctx,
		pipelineIR,
		ppr.Spec.DataLocation,
		clusterMetrics,
	)

	if err != nil {
		logger.Error(err, "Failed to select cluster",
			"strategy", ppr.Spec.PlacementStrategy)
		r.updateStatus(ctx, ppr, "", decision, nil, "", "", "", "", false, "Placement selection failed")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}

	logger.Info("Placement decision made",
		"strategy", ppr.Spec.PlacementStrategy,
		"targetCluster", targetCluster,
		"decision", decision)

	// =========================================================================
	// STEP 8: Calculate total pipeline resources
	// =========================================================================
	totalResources := pipeline.CalculatePipelineResources(pipelineIR, r.pipelineParser)

	resourcesSummary := &orchestratorv1alpha1.PipelineResourcesSummary{
		ExecutorCount: totalResources.ExecutorCount,
		TotalCPU:      totalResources.TotalCPU,
		TotalMemory:   totalResources.TotalMemory,
		TotalGPU:      totalResources.TotalGPU,
	}

	logger.Info("Pipeline resources calculated",
		"executors", resourcesSummary.ExecutorCount,
		"totalCPU", resourcesSummary.TotalCPU,
		"totalMemory", resourcesSummary.TotalMemory,
		"totalGPU", resourcesSummary.TotalGPU)

	// =========================================================================
	// STEP 9: Trigger pipeline execution on Kubeflow
	// =========================================================================
	runName := fmt.Sprintf("%s-%s", ppr.Name, time.Now().Format("20060102-150405"))

	logger.Info("Triggering pipeline execution on Kubeflow",
		"cluster", targetCluster,
		"pipeline", pipelineIR.PipelineInfo.Name,
		"runName", runName)

	runID, runURL, err := r.kubeflowManager.UploadAndRunPipeline(
		ctx,
		targetCluster,
		pipelineIR.PipelineInfo.Name,
		[]byte(pipelineYAML),
		runName,
		ppr.Spec.ExperimentId,
		ppr.Spec.ExperimentName,
		nil,
	)

	if err != nil {
		logger.Error(err, "Failed to execute pipeline on Kubeflow",
			"cluster", targetCluster,
			"pipeline", pipelineIR.PipelineInfo.Name)

		r.updateStatus(ctx, ppr, targetCluster, decision, resourcesSummary, "", "",
			ppr.Spec.ExperimentId, ppr.Spec.ExperimentName, false, fmt.Sprintf("Failed to execute pipeline: %v", err))

		// Retry after 60 seconds
		return ctrl.Result{RequeueAfter: 60 * time.Second}, err
	}

	logger.Info("Pipeline execution triggered successfully",
		"cluster", targetCluster,
		"runID", runID,
		"runURL", runURL)

	// =========================================================================
	// STEP 10: Update status with success
	// =========================================================================
	if err := r.updateStatus(ctx, ppr, targetCluster, decision, resourcesSummary,
		runID, runURL, ppr.Spec.ExperimentId, ppr.Spec.ExperimentName, true, ""); err != nil {
		logger.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	logger.Info("PipelinePlacementRequest reconciled successfully",
		"cluster", targetCluster,
		"runID", runID)

	return ctrl.Result{}, nil
}

// fetchPipelineYAML ottiene il YAML della pipeline dalla sorgente specificata
func (r *PipelinePlacementRequestReconciler) fetchPipelineYAML(
	ctx context.Context,
	ppr *orchestratorv1alpha1.PipelinePlacementRequest,
) (string, error) {
	source := ppr.Spec.PipelineSource

	// Opzione 1: Inline
	if source.Inline != "" {
		return source.Inline, nil
	}

	// Opzione 3: URL
	if source.URL != "" {
		return r.fetchFromURL(ctx, source.URL)
	}

	return "", fmt.Errorf("no valid pipeline source specified")
}

// fetchFromURL scarica il YAML da un URL HTTP/HTTPS
func (r *PipelinePlacementRequestReconciler) fetchFromURL(ctx context.Context, url string) (string, error) {
	// Timeout per la richiesta HTTP
	httpCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
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

// collectMetrics raccoglie metriche reali dai cluster
func (r *PipelinePlacementRequestReconciler) collectMetrics(ctx context.Context) (*placement.ClusterMetrics, error) {
	logger := log.FromContext(ctx)

	// Usa il MetricsCollector per ottenere metriche reali
	metrics, err := r.MetricsCollector.CollectMetrics(ctx)
	if err != nil {
		logger.Error(err, "Failed to collect real metrics, using fallback")
		// Fallback su metriche mock solo in caso di errore critico
		return r.collectMockMetrics(ctx), nil
	}

	logger.V(1).Info("Real metrics collected", "clusters", len(metrics.Clusters))
	return metrics, nil
}

// collectMockMetrics fornisce metriche di fallback
func (r *PipelinePlacementRequestReconciler) collectMockMetrics(ctx context.Context) *placement.ClusterMetrics {
	metrics := placement.NewClusterMetrics()

	// Cloud cluster: potente
	metrics.SetCluster("cloud_cluster", &placement.ClusterMetric{
		Name:            "cloud_cluster",
		CPUCapacity:     16000,
		CPUAvailable:    12000,
		MemoryCapacity:  32 * 1024 * 1024 * 1024,
		MemoryAvailable: 24 * 1024 * 1024 * 1024,
		Available:       true,
	})

	// Edge cluster 1
	metrics.SetCluster("edge_cluster_1", &placement.ClusterMetric{
		Name:            "edge_cluster_1",
		CPUCapacity:     4000,
		CPUAvailable:    3000,
		MemoryCapacity:  8 * 1024 * 1024 * 1024,
		MemoryAvailable: 6 * 1024 * 1024 * 1024,
		Available:       true,
	})

	// Edge cluster 2
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

// getPipelineSourceType ritorna il tipo di sorgente pipeline per logging
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

// updateStatus aggiorna lo status del PipelinePlacementRequest
func (r *PipelinePlacementRequestReconciler) updateStatus(
	ctx context.Context,
	ppr *orchestratorv1alpha1.PipelinePlacementRequest,
	targetCluster string,
	decision string,
	resources *orchestratorv1alpha1.PipelineResourcesSummary,
	runID string,
	runURL string,
	experimentID string, // NUOVO
	experimentName string, // NUOVO
	success bool,
	errorMsg string,
) error {
	// Update main status fields
	ppr.Status.TargetCluster = targetCluster
	ppr.Status.PlacementDecision = decision
	ppr.Status.TotalResources = resources
	ppr.Status.PipelineRunID = runID
	ppr.Status.PipelineRunURL = runURL
	ppr.Status.ExperimentId = experimentID
	ppr.Status.ExperimentName = experimentName

	now := metav1.Now()
	ppr.Status.PlacementTime = &now

	// Update conditions
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
		} else {
			condition.Message = decision
		}
	}

	// Replace or append condition
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

// SetupWithManager sets up the controller with the Manager
func (r *PipelinePlacementRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Initialize pipeline parser
	r.pipelineParser = pipeline.NewParser()

	// Initialize pipeline strategies
	r.pipelineStrategies = map[string]pipeline.PipelineStrategy{
		"cloud-only-pipeline":       pipeline.NewCloudOnlyPipelineStrategy(),
		"data-locality-pipeline":    pipeline.NewDataLocalityPipelineStrategy(),
		"simple-heuristic-pipeline": pipeline.NewSimplePipelineHeuristicStrategy(),
	}

	// Initialize multi-cluster manager
	ctx := context.Background()
	clusterManager, err := multicluster.NewClusterManager(
		ctx,
		mgr.GetConfig(),
		"cluster-kubeconfigs",
		"cloudcontinuum-system",
		mgr.GetScheme(),
	)
	if err != nil {
		return fmt.Errorf("failed to initialize cluster manager: %w", err)
	}
	r.ClusterManager = clusterManager

	// Initialize real metrics collector
	metricsCollector := metrics.NewRealMetricsCollector(
		clusterManager.ClusterClients,
		metrics.DefaultConfig(),
	)
	r.MetricsCollector = metricsCollector
	metricsCollector.Start(ctx)

	// Initialize Kubeflow manager
	r.kubeflowManager = kubeflow.NewManager("kubeflow-user-example-com")

	// Log configured clusters
	ctrl.Log.Info("PipelinePlacementRequest controller initialized",
		"clusters", clusterManager.ListClusters(),
		"strategies", len(r.pipelineStrategies))

	return ctrl.NewControllerManagedBy(mgr).
		For(&orchestratorv1alpha1.PipelinePlacementRequest{}).
		Named("pipelineplacementrequest").
		Complete(r)
}
