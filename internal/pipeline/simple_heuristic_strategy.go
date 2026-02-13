package pipeline

import (
	"context"
	"fmt"

	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

// SimplePipelineHeuristicStrategy usa euristiche e delega alle strategie base.
type SimplePipelineHeuristicStrategy struct {
	parser               *Parser
	cloudStrategy        *CloudOnlyPipelineStrategy
	dataLocalityStrategy *DataLocalityPipelineStrategy
}

// NewSimplePipelineHeuristicStrategy crea una nuova strategia euristica.
func NewSimplePipelineHeuristicStrategy() *SimplePipelineHeuristicStrategy {
	return &SimplePipelineHeuristicStrategy{
		parser:               NewParser(),
		cloudStrategy:        NewCloudOnlyPipelineStrategy(),
		dataLocalityStrategy: NewDataLocalityPipelineStrategy(),
	}
}

// Name ritorna il nome della strategia.
func (s *SimplePipelineHeuristicStrategy) Name() string {
	return "simple-heuristic-pipeline"
}

// SelectCluster usa euristiche per selezionare il cluster migliore.
func (s *SimplePipelineHeuristicStrategy) SelectCluster(
	ctx context.Context,
	pipeline *PipelineIR,
	dataLocation string,
	dataSize string,
	metrics *placement.ClusterMetrics,
) (string, string, error) {

	totalResources := CalculatePipelineResources(pipeline, s.parser)

	// Euristica 1: GPU → cloud obbligatorio
	if totalResources.TotalGPU > 0 {
		cluster, decision, err := s.cloudStrategy.SelectCluster(ctx, pipeline, dataLocation, "", metrics)
		if err != nil {
			return "", fmt.Sprintf("Simple-heuristic: Pipeline requires %d GPU but %s", totalResources.TotalGPU, err.Error()), err
		}
		// Sovrascrivi decisione con prefisso euristica
		decision = fmt.Sprintf("Simple-heuristic: Pipeline requires %d GPU, delegating to cloud-only. %s", totalResources.TotalGPU, decision)
		return cluster, decision, nil
	}

	// Euristica 2: Pipeline pesante → preferisci cloud
	if totalResources.TotalCPU > heavyPipelineThreshold {
		cluster, decision, err := s.cloudStrategy.SelectCluster(ctx, pipeline, dataLocation, "", metrics)
		if err == nil {
			decision = fmt.Sprintf("Simple-heuristic: Heavy pipeline (%s), delegating to cloud-only. %s",
				formatCPUCores(totalResources.TotalCPU), decision)
			return cluster, decision, nil
		}
		// Se cloud fallisce, continua con altre euristiche
	}

	// Euristica 3: Data locality per pipeline leggere
	if dataLocation != "" && dataLocation != "none" {
		cluster, decision, err := s.dataLocalityStrategy.SelectCluster(ctx, pipeline, dataLocation, "", metrics)
		if err == nil {
			decision = fmt.Sprintf("Simple-heuristic: Light pipeline (%s), delegating to data-locality. %s",
				formatCPUCores(totalResources.TotalCPU), decision)
			return cluster, decision, nil
		}
		// Se data locality fallisce, continua con fallback
	}

	// Euristica 4: Fallback → cluster con più risorse
	return s.selectBestAvailableCluster(totalResources, metrics)
}

// selectBestAvailableCluster seleziona il cluster con più risorse disponibili.
func (s *SimplePipelineHeuristicStrategy) selectBestAvailableCluster(
	resources PipelineResources,
	metrics *placement.ClusterMetrics,
) (string, string, error) {

	var bestCluster string
	var maxAvailableCPU int64 = -1

	for clusterName, clusterMetric := range metrics.Clusters {
		if !clusterMetric.Available {
			continue
		}

		if clusterMetric.CPUAvailable >= resources.TotalCPU &&
			clusterMetric.MemoryAvailable >= resources.TotalMemory &&
			clusterMetric.CPUAvailable > maxAvailableCPU {

			maxAvailableCPU = clusterMetric.CPUAvailable
			bestCluster = clusterName
		}
	}

	if bestCluster == "" {
		decision := fmt.Sprintf(
			"Simple-heuristic: No cluster has sufficient resources (needs %s)",
			formatResourceRequirements(resources))
		return "", decision, fmt.Errorf("no cluster has sufficient resources")
	}

	decision := fmt.Sprintf(
		"Simple-heuristic: Fallback to %s (most available: %d mCores). Required: %s.",
		bestCluster,
		maxAvailableCPU,
		formatResourceRequirements(resources),
	)

	return bestCluster, decision, nil
}
