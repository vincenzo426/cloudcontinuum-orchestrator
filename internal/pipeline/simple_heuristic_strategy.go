package pipeline

import (
	"context"
	"fmt"

	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

// SimplePipelineHeuristicStrategy usa euristiche semplici per selezionare il cluster
type SimplePipelineHeuristicStrategy struct {
	parser *Parser
}

// NewSimplePipelineHeuristicStrategy crea una nuova strategia euristica
func NewSimplePipelineHeuristicStrategy() *SimplePipelineHeuristicStrategy {
	return &SimplePipelineHeuristicStrategy{
		parser: NewParser(),
	}
}

// Name ritorna il nome della strategia
func (s *SimplePipelineHeuristicStrategy) Name() string {
	return "simple-heuristic-pipeline"
}

// SelectCluster usa euristiche per selezionare il cluster migliore
func (s *SimplePipelineHeuristicStrategy) SelectCluster(
	ctx context.Context,
	pipeline *PipelineIR,
	dataLocation string,
	metrics *placement.ClusterMetrics,
) (string, string, error) {
	// Calcola risorse totali richieste dalla pipeline
	totalResources := CalculatePipelineResources(pipeline, s.parser)

	// Euristica 1: Se la pipeline richiede GPU, DEVE andare sul cloud
	if totalResources.TotalGPU > 0 {
		cloudMetrics := metrics.GetCluster("cloud_cluster")
		if cloudMetrics != nil && cloudMetrics.Available &&
			cloudMetrics.CPUAvailable >= totalResources.TotalCPU &&
			cloudMetrics.MemoryAvailable >= totalResources.TotalMemory {

			decision := fmt.Sprintf(
				"Simple-heuristic: Pipeline requires %d GPU, placed on cloud_cluster. "+
					"Total: %d mCores CPU, %d bytes memory.",
				totalResources.TotalGPU,
				totalResources.TotalCPU,
				totalResources.TotalMemory,
			)
			return "cloud_cluster", decision, nil
		}
		decision := "Simple-heuristic: Pipeline requires GPU but cloud cluster has insufficient resources."
		return "", decision, fmt.Errorf("pipeline requires GPU but cloud cluster has insufficient resources")
	}

	// Euristica 2: Pipeline pesante (>2 cores totali) → preferisci cloud
	if totalResources.TotalCPU > 2000 {
		cloudMetrics := metrics.GetCluster("cloud_cluster")
		if cloudMetrics != nil && cloudMetrics.Available &&
			cloudMetrics.CPUAvailable >= totalResources.TotalCPU &&
			cloudMetrics.MemoryAvailable >= totalResources.TotalMemory {

			decision := fmt.Sprintf(
				"Simple-heuristic: Heavy pipeline (%.2f cores) placed on cloud_cluster for better performance.",
				float64(totalResources.TotalCPU)/1000,
			)
			return "cloud_cluster", decision, nil
		}
	}

	// Euristica 3: Pipeline leggera + dataLocation specificato → preferisci data locality
	if dataLocation != "" && dataLocation != "none" {
		dataMetrics := metrics.GetCluster(dataLocation)
		if dataMetrics != nil && dataMetrics.Available &&
			dataMetrics.CPUAvailable >= totalResources.TotalCPU &&
			dataMetrics.MemoryAvailable >= totalResources.TotalMemory {

			decision := fmt.Sprintf(
				"Simple-heuristic: Light pipeline (%.2f cores) placed on %s (data location) to minimize latency.",
				float64(totalResources.TotalCPU)/1000,
				dataLocation,
			)
			return dataLocation, decision, nil
		}
	}

	// Euristica 4: Fallback → cluster con più risorse disponibili
	bestCluster, bestAvailable := s.findBestCluster(totalResources, metrics)
	if bestCluster == "" {
		decision := fmt.Sprintf(
			"no cluster has sufficient resources for pipeline (needs %d mCores CPU, %d bytes memory)",
			totalResources.TotalCPU, totalResources.TotalMemory)
		return "", decision, fmt.Errorf(
			"no cluster has sufficient resources for pipeline (needs %d mCores CPU, %d bytes memory)",
			totalResources.TotalCPU, totalResources.TotalMemory)
	}

	decision := fmt.Sprintf(
		"Simple-heuristic: Pipeline placed on %s (most available resources: %d mCores). "+
			"Required: %d mCores CPU (%.2f cores), %d bytes memory (%.2f GB).",
		bestCluster,
		bestAvailable,
		totalResources.TotalCPU,
		float64(totalResources.TotalCPU)/1000,
		totalResources.TotalMemory,
		float64(totalResources.TotalMemory)/1_000_000_000,
	)

	return bestCluster, decision, nil
}

// findBestCluster trova il cluster con più risorse disponibili che soddisfa i requisiti
func (s *SimplePipelineHeuristicStrategy) findBestCluster(
	totalResources PipelineResources,
	metrics *placement.ClusterMetrics,
) (string, int64) {
	var bestCluster string
	var maxAvailableCPU int64 = -1

	for clusterName, clusterMetrics := range metrics.Clusters {
		if !clusterMetrics.Available {
			continue
		}

		// Deve avere risorse sufficienti
		if clusterMetrics.CPUAvailable >= totalResources.TotalCPU &&
			clusterMetrics.MemoryAvailable >= totalResources.TotalMemory {

			// Preferisci quello con più CPU disponibile
			if clusterMetrics.CPUAvailable > maxAvailableCPU {
				maxAvailableCPU = clusterMetrics.CPUAvailable
				bestCluster = clusterName
			}
		}
	}
	return bestCluster, maxAvailableCPU
}
