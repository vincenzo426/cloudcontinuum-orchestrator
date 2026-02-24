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
	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/datatransfer"
	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/kubeflow"
	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/metrics"
	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/multicluster"
	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/pipeline"
	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

// Configuration constants
const (
	requeueDelayShort = 30 * time.Second
	requeueDelayLong  = 60 * time.Second
	httpTimeout       = 30 * time.Second
	runNameTimeFormat = "20060102-150405"

	clusterConfigSecretName = "cluster-kubeconfigs"
	systemNamespace         = "cloudcontinuum-system"
	kubeflowNamespace       = "kubeflow"
)

// PipelinePlacementRequestReconciler manages the lifecycle of PipelinePlacementRequests.
type PipelinePlacementRequestReconciler struct {
	client.Client
	Scheme             *runtime.Scheme
	pipelineStrategies map[string]pipeline.PipelineStrategy
	ClusterManager     *multicluster.ClusterManager
	MetricsCollector   metrics.Collector
	pipelineParser     *pipeline.Parser
	kubeflowManager    *kubeflow.Manager
	transferCalculator *datatransfer.Calculator

	// Reference to RealMetricsCollector for reservation support
	realMetricsCollector *metrics.RealMetricsCollector
}

// RBAC permissions
// +kubebuilder:rbac:groups=orchestrator.cloudcontinuum.io,resources=pipelineplacementrequests,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=orchestrator.cloudcontinuum.io,resources=pipelineplacementrequests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=orchestrator.cloudcontinuum.io,resources=pipelineplacementrequests/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile handles the lifecycle of PipelinePlacementRequests.
func (r *PipelinePlacementRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("[RECONCILE] Started", "name", req.Name, "namespace", req.Namespace)

	// Fetch resource
	ppr := &orchestratorv1alpha1.PipelinePlacementRequest{}
	if err := r.Get(ctx, req.NamespacedName, ppr); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("[RECONCILE] Resource not found (deleted)")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Check idempotency
	if ppr.Status.TargetCluster != "" {
		logger.Info("[RECONCILE] Already processed", "cluster", ppr.Status.TargetCluster)
		return ctrl.Result{}, nil
	}

	// Fetch and parse pipeline
	pipelineIR, pipelineYAML, err := r.fetchAndParsePipeline(ctx, ppr)
	if err != nil {
		return r.failWithStatus(ctx, ppr, err, "Pipeline fetch/parse failed", requeueDelayShort)
	}

	logger.Info("[PARSE] Pipeline parsed", "name", pipelineIR.PipelineInfo.Name, "executors", len(pipelineIR.DeploymentSpec.Executors))

	// Placement decision (with adjusted metrics for reservation)
	targetCluster, decision, resources, transferInfo, err := r.makePlacementDecision(ctx, ppr, pipelineIR)
	if err != nil {
		return r.failWithStatus(ctx, ppr, err, decision, requeueDelayShort)
	}

	logger.Info("[PLACEMENT] Decision made", "strategy", ppr.Spec.PlacementStrategy, "cluster", targetCluster)

	// Add reservation after successful decision
	r.addReservationForPipeline(ppr.Name, targetCluster, resources)

	// Execute pipeline
	runID, runURL, err := r.executePipeline(ctx, ppr, pipelineIR, targetCluster, pipelineYAML)
	if err != nil {
		// Remove reservation on failure
		r.removeReservationForPipeline(ppr.Name)
		return r.failWithStatus(ctx, ppr, err, "Pipeline execution failed", requeueDelayLong)
	}

	logger.Info("[SUCCESS] Pipeline deployed", "cluster", targetCluster, "runID", runID)

	// Update final status
	return r.updateStatusAndComplete(ctx, ppr, targetCluster, decision, resources, transferInfo, runID, runURL, true, "")
}

// =============================================================================
// RESERVATION HELPERS
// =============================================================================

func (r *PipelinePlacementRequestReconciler) addReservationForPipeline(pipelineName, targetCluster string, resources *orchestratorv1alpha1.PipelineResourcesSummary) {
	if r.realMetricsCollector == nil || resources == nil {
		return
	}

	r.realMetricsCollector.AddReservation(
		pipelineName,
		targetCluster,
		resources.TotalCPU,
		resources.TotalMemory,
	)
}

func (r *PipelinePlacementRequestReconciler) removeReservationForPipeline(pipelineName string) {
	if r.realMetricsCollector == nil {
		return
	}
	r.realMetricsCollector.RemoveReservation(pipelineName)
}

// =============================================================================
// PLACEMENT DECISION
// =============================================================================

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

	// Analyze pipeline structure
	executors := r.pipelineParser.GetExecutorNames(pipelineIR)
	tasks := r.pipelineParser.GetTaskNames(pipelineIR)
	dependencies := r.pipelineParser.GetTaskDependencies(pipelineIR)

	logger.V(1).Info("[DEBUG] Pipeline structure",
		"executors", len(executors),
		"tasks", len(tasks),
		"hasDependencies", len(dependencies) > 0)

	return pipelineIR, pipelineYAML, nil
}

func (r *PipelinePlacementRequestReconciler) makePlacementDecision(
	ctx context.Context,
	ppr *orchestratorv1alpha1.PipelinePlacementRequest,
	pipelineIR *pipeline.PipelineIR,
) (string, string, *orchestratorv1alpha1.PipelineResourcesSummary, *datatransfer.TransferInfo, error) {
	logger := log.FromContext(ctx)

	// Collect metrics with reservation support
	clusterMetrics, metricsAge, err := r.collectMetricsWithReservation(ctx)
	if err != nil {
		return "", "", nil, nil, err
	}

	logger.Info("[METRICS] Collected",
		"clusters", len(clusterMetrics.Clusters),
		"metricsAge", fmt.Sprintf("%.2fs", metricsAge),
		"activeReservations", r.getActiveReservationsCount())

	// Select strategy
	strategy, ok := r.pipelineStrategies[ppr.Spec.PlacementStrategy]
	if !ok {
		return "", "", nil, nil, fmt.Errorf("unknown placement strategy: %s", ppr.Spec.PlacementStrategy)
	}

	// Execute placement - use appropriate method based on strategy type
	var targetCluster, decision string

	// Check if this is the RL strategy (supports dataSize)
	if rlStrategy, isRL := strategy.(*pipeline.RLPipelineStrategy); isRL {
		// RL strategy: pass dataSize for transfer time consideration
		targetCluster, decision, err = rlStrategy.SelectCluster(
			ctx, pipelineIR, ppr.Spec.DataLocation, ppr.Spec.DataSize, clusterMetrics,
		)
	} else {
		// Other strategies: use standard interface (no dataSize)
		targetCluster, decision, err = strategy.SelectCluster(
			ctx, pipelineIR, ppr.Spec.DataLocation, "", clusterMetrics,
		)
	}

	if err != nil {
		return "", decision, nil, nil, fmt.Errorf("cluster selection failed: %w", err)
	}

	// Calculate resources
	totalResources := pipeline.CalculatePipelineResources(pipelineIR, r.pipelineParser)

	// Calculate data transfer (uniform for all strategies)
	transferInfo := r.calculateDataTransfer(
		ppr.Spec.DataLocation,
		targetCluster,
		ppr.Spec.DataSize,
		clusterMetrics,
	)

	// Log detailed summary
	placementResult := pipeline.NewPipelinePlacement(targetCluster, decision, totalResources)
	logger.Info("[PLACEMENT] Complete",
		"cluster", targetCluster,
		"strategy", ppr.Spec.PlacementStrategy,
		"metricsAge", fmt.Sprintf("%.2fs", metricsAge),
		"dataSize", ppr.Spec.DataSize,
		"transferTime", fmt.Sprintf("%dms", transferInfo.TransferTime),
		"executors", totalResources.ExecutorCount,
		"cpuCores", fmt.Sprintf("%.2f", float64(totalResources.TotalCPU)/1000.0),
		"memoryGB", fmt.Sprintf("%.2f", float64(totalResources.TotalMemory)/(1024*1024*1024)))
	logger.V(1).Info("[DEBUG] Placement details\n" + placementResult.Summary())
	if transferInfo.TransferTime > 0 {
		logger.Info("[DATA TRANSFER]", "details", transferInfo.TransferDetails)
	}

	return targetCluster, decision, r.toResourcesSummary(totalResources), transferInfo, nil
}

func (r *PipelinePlacementRequestReconciler) collectMetricsWithReservation(ctx context.Context) (*placement.ClusterMetrics, float64, error) {
	logger := log.FromContext(ctx)

	var metricsAge float64 = 0.0

	if r.realMetricsCollector != nil {
		metricsAge = r.realMetricsCollector.GetMetricsAge()

		adjustedMetrics, err := r.realMetricsCollector.GetAdjustedMetrics(ctx)
		if err != nil {
			logger.Error(err, "[WARN] Failed to get adjusted metrics, using fallback")
		}

		logger.V(1).Info("[METRICS] Using adjusted metrics (with reservations)",
			"metricsAge", fmt.Sprintf("%.2fs", metricsAge))

		return adjustedMetrics, metricsAge, nil
	}

	// Fallback: use standard Collector interface
	metrics, err := r.MetricsCollector.CollectMetrics(ctx)
	if err != nil {
		logger.Error(err, "[WARN] Using fallback metrics")
	}

	logger.V(1).Info("[METRICS] Real metrics collected (no reservation support)")
	return metrics, metricsAge, nil
}

func (r *PipelinePlacementRequestReconciler) getActiveReservationsCount() int {
	if r.realMetricsCollector != nil {
		return r.realMetricsCollector.GetActiveReservationsCount()
	}
	return 0
}

// =============================================================================
// PIPELINE EXECUTION
// =============================================================================

func (r *PipelinePlacementRequestReconciler) executePipeline(ctx context.Context, ppr *orchestratorv1alpha1.PipelinePlacementRequest, pipelineIR *pipeline.PipelineIR, targetCluster, pipelineYAML string) (string, string, error) {
	logger := log.FromContext(ctx)

	runName := fmt.Sprintf("%s-%s", ppr.Name, time.Now().Format(runNameTimeFormat))
	logger.Info("[EXECUTION] Triggering pipeline", "cluster", targetCluster, "pipeline", pipelineIR.PipelineInfo.Name)

	// Convert parameters
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

// =============================================================================
// YAML FETCHING
// =============================================================================

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

func (r *PipelinePlacementRequestReconciler) fetchFromURL(ctx context.Context, url string) (string, error) {
	client := &http.Client{Timeout: httpTimeout}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read body: %w", err)
	}

	return string(body), nil
}

func (r *PipelinePlacementRequestReconciler) fetchFromConfigMap(ctx context.Context, namespace string, ref *orchestratorv1alpha1.ConfigMapReference) (string, error) {
	cm := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: namespace, Name: ref.Name}

	if err := r.Get(ctx, key, cm); err != nil {
		return "", fmt.Errorf("get configmap %s: %w", ref.Name, err)
	}

	data, ok := cm.Data[ref.Key]
	if !ok {
		return "", fmt.Errorf("key %s not found in configmap %s", ref.Key, ref.Name)
	}

	return data, nil
}

// =============================================================================
// STATUS UPDATES
// =============================================================================

func (r *PipelinePlacementRequestReconciler) failWithStatus(ctx context.Context, ppr *orchestratorv1alpha1.PipelinePlacementRequest, err error, message string, requeueAfter time.Duration) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	errorMsg := message
	if err != nil {
		errorMsg = fmt.Sprintf("%s: %v", message, err)
	}

	logger.Error(err, "[FAILED]", "message", message)

	// Update status with failure
	now := metav1.Now()
	ppr.Status.PlacementTime = &now

	condition := metav1.Condition{
		Type:               "Placed",
		Status:             metav1.ConditionFalse,
		LastTransitionTime: now,
		Reason:             "PlacementFailed",
		Message:            errorMsg,
	}

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

	if statusErr := r.Status().Update(ctx, ppr); statusErr != nil {
		logger.Error(statusErr, "Failed to update status")
	}

	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

func (r *PipelinePlacementRequestReconciler) updateStatusAndComplete(ctx context.Context, ppr *orchestratorv1alpha1.PipelinePlacementRequest, targetCluster, decision string, resources *orchestratorv1alpha1.PipelineResourcesSummary, transferInfo *datatransfer.TransferInfo, runID, runURL string, success bool, errorMsg string) (ctrl.Result, error) {
	ppr.Status.TargetCluster = targetCluster
	ppr.Status.PlacementDecision = decision
	ppr.Status.TotalResources = resources
	ppr.Status.PipelineRunID = runID
	ppr.Status.PipelineRunURL = runURL
	ppr.Status.ExperimentId = ppr.Spec.ExperimentId
	ppr.Status.ExperimentName = ppr.Spec.ExperimentName
	ppr.Status.Parameters = ppr.Spec.Parameters

	// Add data transfer info
	if transferInfo != nil {
		ppr.Status.DataTransferLatency = transferInfo.NetworkLatency
		ppr.Status.DataTransferTime = transferInfo.TransferTime
		ppr.Status.DataTransferDetails = transferInfo.TransferDetails
	}

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
		log.FromContext(ctx).Info("[SUCCESS] Reconciled",
			"cluster", targetCluster,
			"runID", runID,
			"transferTime", fmt.Sprintf("%dms", transferInfo.TransferTime),
			"activeReservations", r.getActiveReservationsCount())
	}

	return ctrl.Result{}, nil
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

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

func (r *PipelinePlacementRequestReconciler) calculateDataTransfer(
	dataLocation, targetCluster, dataSize string,
	metrics *placement.ClusterMetrics,
) *datatransfer.TransferInfo {

	// Case 1: No transfer needed
	if dataLocation == "" || dataLocation == "none" || dataLocation == targetCluster {
		return datatransfer.NoTransferNeeded()
	}

	// Case 2: Use default dataSize if not specified
	if dataSize == "" {
		dataSize = "1GB"
	}

	// Case 3: Get network latency
	networkLatency := r.getNetworkLatency(dataLocation, targetCluster, metrics)

	// Case 4: Calculate total transfer time
	if r.transferCalculator != nil {
		result, err := r.transferCalculator.CalculateTransferTime(
			dataLocation, targetCluster, dataSize, networkLatency,
		)
		if err == nil {
			return &datatransfer.TransferInfo{
				SourceCluster:   dataLocation,
				TargetCluster:   targetCluster,
				DataSize:        dataSize,
				NetworkLatency:  result.NetworkLatency,
				TransferTime:    result.DataTransferTime,
				TransferDetails: result.TransferDetails,
			}
		}
	}

	// Fallback: network latency only
	return &datatransfer.TransferInfo{
		SourceCluster:  dataLocation,
		TargetCluster:  targetCluster,
		DataSize:       dataSize,
		NetworkLatency: networkLatency,
		TransferTime:   networkLatency,
		TransferDetails: fmt.Sprintf("Data transfer from %s to %s (network latency only: %d ms)",
			dataLocation, targetCluster, networkLatency),
	}
}

func (r *PipelinePlacementRequestReconciler) getNetworkLatency(
	source, target string,
	metrics *placement.ClusterMetrics,
) int64 {
	sourceMetric := metrics.GetCluster(source)
	if sourceMetric == nil {
		return 9999
	}

	switch target {
	case "edge_cluster_1":
		return sourceMetric.LatencyToEdge1
	case "edge_cluster_2":
		return sourceMetric.LatencyToEdge2
	case "edge_cluster_3":
		return sourceMetric.LatencyToEdge3
	case "cloud_cluster":
		return sourceMetric.LatencyToCloud
	default:
		return 9999
	}
}

// =============================================================================
// SETUP
// =============================================================================

func (r *PipelinePlacementRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.pipelineParser = pipeline.NewParser()

	r.pipelineStrategies = map[string]pipeline.PipelineStrategy{
		"cloud-only-pipeline":       pipeline.NewCloudOnlyPipelineStrategy(),
		"data-locality-pipeline":    pipeline.NewDataLocalityPipelineStrategy(),
		"simple-heuristic-pipeline": pipeline.NewSimplePipelineHeuristicStrategy(),
		"random-pipeline":           pipeline.NewRandomPipelineStrategy(),
		"rl-based-pipeline":         pipeline.NewRLPipelineStrategy("http://rl-service.kubeflow.svc.cluster.local:5000"),
	}

	ctx := context.Background()

	// Initialize transfer calculator
	transferCalc, err := datatransfer.NewCalculator(ctx, mgr.GetClient())
	if err != nil {
		ctrl.Log.Error(err, "Failed to initialize data transfer calculator")
	}
	r.transferCalculator = transferCalc

	clusterManager, err := multicluster.NewClusterManager(ctx, mgr.GetConfig(), clusterConfigSecretName, systemNamespace, mgr.GetScheme())
	if err != nil {
		return fmt.Errorf("failed to initialize cluster manager: %w", err)
	}
	r.ClusterManager = clusterManager

	// Create metrics collector with reservation support
	metricsCollector := metrics.NewRealMetricsCollector(clusterManager.ClusterClients, metrics.DefaultConfig())
	r.MetricsCollector = metricsCollector
	r.realMetricsCollector = metricsCollector
	metricsCollector.Start(ctx)

	// Kubeflow Manager
	kubeflowMgr, err := kubeflow.NewManager(ctx, mgr.GetConfig(), mgr.GetScheme())
	if err != nil {
		return fmt.Errorf("failed to initialize kubeflow manager: %w", err)
	}
	r.kubeflowManager = kubeflowMgr

	ctrl.Log.Info("[INIT] PipelinePlacementRequest controller initialized",
		"clusters", clusterManager.ListClusters(),
		"strategies", len(r.pipelineStrategies),
		"reservationSupport", true)

	return ctrl.NewControllerManagedBy(mgr).
		For(&orchestratorv1alpha1.PipelinePlacementRequest{}).
		Named("pipelineplacementrequest").
		Complete(r)
}
