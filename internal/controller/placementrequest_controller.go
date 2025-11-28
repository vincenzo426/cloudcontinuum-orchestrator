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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	orchestratorv1alpha1 "github.com/vincenzo426/cloudcontinuum-orchestrator/api/v1alpha1"
	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/multicluster"
	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

// PlacementRequestReconciler reconciles a PlacementRequest object
type PlacementRequestReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Placement strategies
	strategies map[string]placement.Strategy

	// Multi-cluster manager
	ClusterManager *multicluster.ClusterManager
}

// +kubebuilder:rbac:groups=orchestrator.cloudcontinuum.io,resources=placementrequests,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=orchestrator.cloudcontinuum.io,resources=placementrequests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=orchestrator.cloudcontinuum.io,resources=placementrequests/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop
func (r *PlacementRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling PlacementRequest", "name", req.Name, "namespace", req.Namespace)

	// Fetch the PlacementRequest
	placementReq := &orchestratorv1alpha1.PlacementRequest{}
	if err := r.Get(ctx, req.NamespacedName, placementReq); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("PlacementRequest not found, likely deleted")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get PlacementRequest")
		return ctrl.Result{}, err
	}

	// Check if already placed
	if placementReq.Status.TargetCluster != "" {
		logger.Info("PlacementRequest already placed", "cluster", placementReq.Status.TargetCluster)
		return ctrl.Result{}, nil
	}

	// Collect metrics from clusters
	metrics, err := r.collectMetrics(ctx)
	if err != nil {
		logger.Error(err, "Failed to collect metrics")
		return ctrl.Result{}, err
	}

	// Select strategy
	strategy, ok := r.strategies[placementReq.Spec.PlacementStrategy]
	if !ok {
		err := fmt.Errorf("unknown placement strategy: %s", placementReq.Spec.PlacementStrategy)
		logger.Error(err, "Invalid strategy")
		return ctrl.Result{}, err
	}

	// Execute placement decision
	targetCluster, decision, err := strategy.SelectCluster(ctx, placementReq, metrics)
	if err != nil {
		logger.Error(err, "Failed to select cluster", "strategy", placementReq.Spec.PlacementStrategy)
		r.updateStatus(ctx, placementReq, "", decision, false)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}

	logger.Info("Placement decision made",
		"strategy", placementReq.Spec.PlacementStrategy,
		"targetCluster", targetCluster,
		"decision", decision)

	// Create pod on target cluster (for now, create locally - we'll add multi-cluster later)
	if err := r.createPod(ctx, placementReq, targetCluster); err != nil {
		logger.Error(err, "Failed to create pod", "cluster", targetCluster)
		r.updateStatus(ctx, placementReq, targetCluster, decision, false)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}

	// Update status
	if err := r.updateStatus(ctx, placementReq, targetCluster, decision, true); err != nil {
		logger.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	logger.Info("PlacementRequest reconciled successfully", "cluster", targetCluster)
	return ctrl.Result{}, nil
}

// collectMetrics gathers metrics from all clusters
func (r *PlacementRequestReconciler) collectMetrics(ctx context.Context) (*placement.ClusterMetrics, error) {
	logger := log.FromContext(ctx)
	logger.Info("Collecting cluster metrics")

	metrics := placement.NewClusterMetrics()

	// TODO: Replace with real metrics from clusters
	// For now, use mock data

	// Edge Cluster 1 (Imola)
	metrics.SetCluster("edge_cluster_1", &placement.ClusterMetric{
		Name:            "edge_cluster_1",
		CPUCapacity:     4000,                   // 4 cores
		CPUUsed:         1000,                   // 1 core used
		CPUAvailable:    3000,                   // 3 cores available
		MemoryCapacity:  8 * 1024 * 1024 * 1024, // 8Gi
		MemoryUsed:      2 * 1024 * 1024 * 1024, // 2Gi
		MemoryAvailable: 6 * 1024 * 1024 * 1024, // 6Gi
		LatencyToEdge1:  0,
		LatencyToEdge2:  15, // 15ms to edge2
		LatencyToCloud:  30, // 30ms to cloud
		Available:       true,
	})

	// Edge Cluster 2 (Lugo)
	metrics.SetCluster("edge_cluster_2", &placement.ClusterMetric{
		Name:            "edge_cluster_2",
		CPUCapacity:     4000,
		CPUUsed:         1500,
		CPUAvailable:    2500,
		MemoryCapacity:  8 * 1024 * 1024 * 1024,
		MemoryUsed:      3 * 1024 * 1024 * 1024,
		MemoryAvailable: 5 * 1024 * 1024 * 1024,
		LatencyToEdge1:  15,
		LatencyToEdge2:  0,
		LatencyToCloud:  25,
		Available:       true,
	})

	// Cloud Cluster (Bologna)
	metrics.SetCluster("cloud_cluster", &placement.ClusterMetric{
		Name:            "cloud_cluster",
		CPUCapacity:     16000, // 16 cores
		CPUUsed:         4000,
		CPUAvailable:    12000,
		MemoryCapacity:  32 * 1024 * 1024 * 1024, // 32Gi
		MemoryUsed:      8 * 1024 * 1024 * 1024,
		MemoryAvailable: 24 * 1024 * 1024 * 1024,
		LatencyToEdge1:  30,
		LatencyToEdge2:  25,
		LatencyToCloud:  0,
		Available:       true,
	})

	logger.Info("Metrics collected",
		"edge1_available_cpu", metrics.GetCluster("edge_cluster_1").CPUAvailable,
		"edge2_available_cpu", metrics.GetCluster("edge_cluster_2").CPUAvailable,
		"cloud_available_cpu", metrics.GetCluster("cloud_cluster").CPUAvailable)

	return metrics, nil
}

// createPod creates a pod on the target cluster
func (r *PlacementRequestReconciler) createPod(ctx context.Context, pr *orchestratorv1alpha1.PlacementRequest, targetCluster string) error {
	logger := log.FromContext(ctx)
	logger.Info("Creating pod on remote cluster", "cluster", targetCluster)

	// Get the client for the target cluster
	remoteClient, err := r.ClusterManager.GetClient(targetCluster)
	if err != nil {
		logger.Error(err, "Failed to get client for cluster", "cluster", targetCluster)
		return err
	}

	// Check if pod already exists on target cluster
	podName := fmt.Sprintf("%s-pod", pr.Name)
	existingPod := &corev1.Pod{}
	err = remoteClient.Get(ctx, types.NamespacedName{Name: podName, Namespace: pr.Namespace}, existingPod)
	if err == nil {
		logger.Info("Pod already exists on target cluster", "pod", podName, "cluster", targetCluster)
		return nil
	}
	if !errors.IsNotFound(err) {
		return fmt.Errorf("error checking existing pod on cluster %s: %w", targetCluster, err)
	}

	// Create pod spec
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: pr.Namespace,
			Labels: map[string]string{
				"app":                pr.Name,
				"placement-request":  pr.Name,
				"target-cluster":     targetCluster,
				"placement-strategy": pr.Spec.PlacementStrategy,
			},
			Annotations: map[string]string{
				"cloudcontinuum.io/target-cluster":    targetCluster,
				"cloudcontinuum.io/placement-request": pr.Name,
				"cloudcontinuum.io/source-cluster":    "cloud_cluster",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "workload",
					Image: r.getImage(pr),
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(pr.Spec.ResourceRequirements.CPU),
							corev1.ResourceMemory: resource.MustParse(pr.Spec.ResourceRequirements.Memory),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(pr.Spec.ResourceRequirements.CPU),
							corev1.ResourceMemory: resource.MustParse(pr.Spec.ResourceRequirements.Memory),
						},
					},
					Command: r.getCommand(pr),
					Args:    r.getArgs(pr),
				},
			},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}

	// NOTE: We cannot set controller reference cross-cluster
	// Owner references only work within the same cluster
	// We use labels and annotations for tracking instead

	// Create the pod on the REMOTE cluster
	if err := remoteClient.Create(ctx, pod); err != nil {
		return fmt.Errorf("failed to create pod on cluster %s: %w", targetCluster, err)
	}

	logger.Info("Pod created successfully on remote cluster", "pod", podName, "cluster", targetCluster)
	return nil
}

// getImage returns the container image to use
func (r *PlacementRequestReconciler) getImage(pr *orchestratorv1alpha1.PlacementRequest) string {
	if pr.Spec.PodSpec != nil && pr.Spec.PodSpec.Image != "" {
		return pr.Spec.PodSpec.Image
	}
	// Default image for testing
	return "nginx:latest"
}

// getCommand returns the command to run
func (r *PlacementRequestReconciler) getCommand(pr *orchestratorv1alpha1.PlacementRequest) []string {
	if pr.Spec.PodSpec != nil && len(pr.Spec.PodSpec.Command) > 0 {
		return pr.Spec.PodSpec.Command
	}
	return nil
}

// getArgs returns the arguments
func (r *PlacementRequestReconciler) getArgs(pr *orchestratorv1alpha1.PlacementRequest) []string {
	if pr.Spec.PodSpec != nil && len(pr.Spec.PodSpec.Args) > 0 {
		return pr.Spec.PodSpec.Args
	}
	return nil
}

// updateStatus updates the PlacementRequest status
func (r *PlacementRequestReconciler) updateStatus(ctx context.Context, pr *orchestratorv1alpha1.PlacementRequest,
	targetCluster, decision string, success bool) error {

	pr.Status.TargetCluster = targetCluster
	pr.Status.PlacementDecision = decision
	now := metav1.Now()
	pr.Status.PlacementTime = &now

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
	}

	// Replace or append condition
	found := false
	for i, cond := range pr.Status.Conditions {
		if cond.Type == "Placed" {
			pr.Status.Conditions[i] = condition
			found = true
			break
		}
	}
	if !found {
		pr.Status.Conditions = append(pr.Status.Conditions, condition)
	}

	return r.Status().Update(ctx, pr)
}

// SetupWithManager sets up the controller with the Manager
// SetupWithManager sets up the controller with the Manager
func (r *PlacementRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Initialize strategies
	r.strategies = map[string]placement.Strategy{
		"cloud-only":       placement.NewCloudOnlyStrategy(),
		"data-locality":    placement.NewDataLocalityStrategy(),
		"simple-heuristic": placement.NewSimpleHeuristicStrategy(),
	}

	// Initialize multi-cluster manager
	ctx := context.Background()
	clusterManager, err := multicluster.NewClusterManager(
		ctx,
		mgr.GetClient(),
		"cluster-kubeconfigs",
		"cloudcontinuum-system",
		mgr.GetScheme(),
	)
	if err != nil {
		return fmt.Errorf("failed to initialize cluster manager: %w", err)
	}
	r.ClusterManager = clusterManager

	// Log configured clusters
	ctrl.Log.Info("Multi-cluster manager initialized", "clusters", clusterManager.ListClusters())

	return ctrl.NewControllerManagedBy(mgr).
		For(&orchestratorv1alpha1.PlacementRequest{}).
		Named("placementrequest").
		Complete(r)
}
