package pipeline

import (
	"fmt"

	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

// Costanti condivise per le strategie
const (
	cloudClusterName       = "cloud_cluster"
	defaultDataLocation    = "cloud_cluster"
	heavyPipelineThreshold = 2000 // millicores
)

// validateClusterAvailability verifica che un cluster sia disponibile.
func validateClusterAvailability(
	clusterName string,
	metrics *placement.ClusterMetrics,
) (*placement.ClusterMetric, error) {
	clusterMetric := metrics.GetCluster(clusterName)
	if clusterMetric == nil || !clusterMetric.Available {
		return nil, fmt.Errorf("cluster %s not available", clusterName)
	}
	return clusterMetric, nil
}

// validateClusterResources verifica che un cluster abbia risorse sufficienti.
func validateClusterResources(
	clusterName string,
	clusterMetric *placement.ClusterMetric,
	resources PipelineResources,
) error {
	if clusterMetric.CPUAvailable < resources.TotalCPU {
		return fmt.Errorf(
			"insufficient CPU on %s: needs %d mCores, available %d mCores",
			clusterName, resources.TotalCPU, clusterMetric.CPUAvailable)
	}

	if clusterMetric.MemoryAvailable < resources.TotalMemory {
		return fmt.Errorf(
			"insufficient memory on %s: needs %d bytes, available %d bytes",
			clusterName, resources.TotalMemory, clusterMetric.MemoryAvailable)
	}

	return nil
}

// formatResourceRequirements formatta i requisiti di risorse per le decisioni.
func formatResourceRequirements(resources PipelineResources) string {
	return fmt.Sprintf(
		"%d mCores CPU (%.2f cores), %d bytes memory (%.2f GB)",
		resources.TotalCPU,
		float64(resources.TotalCPU)/millicoresToCores,
		resources.TotalMemory,
		float64(resources.TotalMemory)/bytesToGigabytes,
	)
}

// formatCPUCores formatta CPU in cores.
func formatCPUCores(millicores int64) string {
	return fmt.Sprintf("%.2f cores", float64(millicores)/millicoresToCores)
}
