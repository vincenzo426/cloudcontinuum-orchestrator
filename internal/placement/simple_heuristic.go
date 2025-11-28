package placement

import (
    "context"
    "fmt"
    orchestratorv1alpha1 "github.com/vincenzo426/cloudcontinuum-orchestrator/api/v1alpha1"
    "k8s.io/apimachinery/pkg/api/resource"
)

// SimpleHeuristicStrategy uses simple heuristics for placement
type SimpleHeuristicStrategy struct{}

// NewSimpleHeuristicStrategy creates a new SimpleHeuristicStrategy
func NewSimpleHeuristicStrategy() *SimpleHeuristicStrategy {
    return &SimpleHeuristicStrategy{}
}

// Name returns the strategy name
func (s *SimpleHeuristicStrategy) Name() string {
    return "simple-heuristic"
}

// SelectCluster uses simple heuristics to select a cluster
func (s *SimpleHeuristicStrategy) SelectCluster(ctx context.Context, pr *orchestratorv1alpha1.PlacementRequest, metrics *ClusterMetrics) (string, string, error) {
    // Parse requested CPU
    cpuQuantity, err := resource.ParseQuantity(pr.Spec.ResourceRequirements.CPU)
    if err != nil {
        return "", "", fmt.Errorf("invalid CPU quantity: %w", err)
    }
    requestedCPU := cpuQuantity.MilliValue()
    
    dataLocation := pr.Spec.DataLocation
    
    // Heuristic 1: If data location is specified and has enough resources, use it
    if dataLocation != "" && dataLocation != "none" {
        dataMetrics := metrics.GetCluster(dataLocation)
        if dataMetrics != nil && dataMetrics.Available && dataMetrics.CPUAvailable >= requestedCPU {
            decision := fmt.Sprintf("Simple-heuristic: Data is on %s and it has sufficient resources (%d mCPU available). Using data locality.",
                dataLocation, dataMetrics.CPUAvailable)
            return dataLocation, decision, nil
        }
    }
    
    // Heuristic 2: Find cluster with most available CPU
    var bestCluster string
    var maxAvailableCPU int64 = -1
    
    for name, clusterMetrics := range metrics.Clusters {
        if !clusterMetrics.Available {
            continue
        }
        
        if clusterMetrics.CPUAvailable > maxAvailableCPU && clusterMetrics.CPUAvailable >= requestedCPU {
            maxAvailableCPU = clusterMetrics.CPUAvailable
            bestCluster = name
        }
    }
    
    if bestCluster == "" {
        return "", "", fmt.Errorf("no cluster has enough available CPU (%d mCPU requested)", requestedCPU)
    }
    
    decision := fmt.Sprintf("Simple-heuristic: Selected %s as it has most available CPU (%d mCPU) and meets requirements.",
        bestCluster, maxAvailableCPU)
    
    return bestCluster, decision, nil
}
