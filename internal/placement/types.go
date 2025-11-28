package placement

import (
	"context"
	orchestratorv1alpha1 "github.com/vincenzo426/cloudcontinuum-orchestrator/api/v1alpha1"
)

// Strategy defines the interface for placement strategies
type Strategy interface {
	// Name returns the strategy name
	Name() string

	// SelectCluster chooses the best cluster for the given PlacementRequest
	SelectCluster(ctx context.Context, pr *orchestratorv1alpha1.PlacementRequest, metrics *ClusterMetrics) (string, string, error)
}

// ClusterMetrics holds metrics for all clusters
type ClusterMetrics struct {
	Clusters map[string]*ClusterMetric
}

// ClusterMetric holds metrics for a single cluster
type ClusterMetric struct {
	Name string

	// CPU metrics
	CPUCapacity  int64 // Total CPU in millicores
	CPUUsed      int64 // Used CPU in millicores
	CPUAvailable int64 // Available CPU in millicores

	// Memory metrics
	MemoryCapacity  int64 // Total memory in bytes
	MemoryUsed      int64 // Used memory in bytes
	MemoryAvailable int64 // Available memory in bytes

	// Latency to other clusters (in milliseconds)
	LatencyToEdge1 int64
	LatencyToEdge2 int64
	LatencyToCloud int64

	// Availability
	Available bool
}

// NewClusterMetrics creates a new ClusterMetrics instance
func NewClusterMetrics() *ClusterMetrics {
	return &ClusterMetrics{
		Clusters: make(map[string]*ClusterMetric),
	}
}

// GetCluster returns metrics for a specific cluster
func (cm *ClusterMetrics) GetCluster(name string) *ClusterMetric {
	return cm.Clusters[name]
}

// SetCluster sets metrics for a specific cluster
func (cm *ClusterMetrics) SetCluster(name string, metric *ClusterMetric) {
	cm.Clusters[name] = metric
}
