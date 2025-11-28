package placement

import (
	"context"
	"fmt"
	orchestratorv1alpha1 "github.com/vincenzo426/cloudcontinuum-orchestrator/api/v1alpha1"
)

// DataLocalityStrategy places workloads where data resides
type DataLocalityStrategy struct{}

// NewDataLocalityStrategy creates a new DataLocalityStrategy
func NewDataLocalityStrategy() *DataLocalityStrategy {
	return &DataLocalityStrategy{}
}

// Name returns the strategy name
func (s *DataLocalityStrategy) Name() string {
	return "data-locality"
}

// SelectCluster selects the cluster where data is located
func (s *DataLocalityStrategy) SelectCluster(ctx context.Context, pr *orchestratorv1alpha1.PlacementRequest, metrics *ClusterMetrics) (string, string, error) {
	dataLocation := pr.Spec.DataLocation

	// If no data location specified, default to cloud
	if dataLocation == "" || dataLocation == "none" {
		return "cloud_cluster", "No data location specified, defaulting to cloud cluster", nil
	}

	// Check if target cluster is available
	targetMetrics := metrics.GetCluster(dataLocation)
	if targetMetrics == nil || !targetMetrics.Available {
		return "", "", fmt.Errorf("target cluster %s not available", dataLocation)
	}

	decision := fmt.Sprintf("Data-locality strategy: Data is on %s, placing workload there. Cluster has %d mCPU available.",
		dataLocation, targetMetrics.CPUAvailable)

	return dataLocation, decision, nil
}
