package placement

import (
	"context"
	"fmt"
	orchestratorv1alpha1 "github.com/vincenzo426/cloudcontinuum-orchestrator/api/v1alpha1"
)

// CloudOnlyStrategy always places workloads on the cloud cluster
type CloudOnlyStrategy struct{}

// NewCloudOnlyStrategy creates a new CloudOnlyStrategy
func NewCloudOnlyStrategy() *CloudOnlyStrategy {
	return &CloudOnlyStrategy{}
}

// Name returns the strategy name
func (s *CloudOnlyStrategy) Name() string {
	return "cloud-only"
}

// SelectCluster always selects the cloud cluster
func (s *CloudOnlyStrategy) SelectCluster(ctx context.Context, pr *orchestratorv1alpha1.PlacementRequest, metrics *ClusterMetrics) (string, string, error) {
	cloudCluster := "cloud_cluster"

	// Check if cloud cluster is available
	cloudMetrics := metrics.GetCluster(cloudCluster)
	if cloudMetrics == nil || !cloudMetrics.Available {
		return "", "", fmt.Errorf("cloud cluster not available")
	}

	decision := fmt.Sprintf("Cloud-only strategy: Always place on cloud cluster. Cloud cluster has %d mCPU available.",
		cloudMetrics.CPUAvailable)

	return cloudCluster, decision, nil
}
