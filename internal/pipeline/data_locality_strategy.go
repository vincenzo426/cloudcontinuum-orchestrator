package pipeline

import (
	"context"
	"fmt"

	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

// DataLocalityPipelineStrategy esegue la pipeline dove risiedono i dati
type DataLocalityPipelineStrategy struct {
	parser *Parser
}

// NewDataLocalityPipelineStrategy crea una nuova strategia data-locality
func NewDataLocalityPipelineStrategy() *DataLocalityPipelineStrategy {
	return &DataLocalityPipelineStrategy{
		parser: NewParser(),
	}
}

// Name ritorna il nome della strategia
func (s *DataLocalityPipelineStrategy) Name() string {
	return "data-locality-pipeline"
}

// SelectCluster seleziona il cluster dove risiedono i dati
func (s *DataLocalityPipelineStrategy) SelectCluster(
	ctx context.Context,
	pipeline *PipelineIR,
	dataLocation string,
	metrics *placement.ClusterMetrics,
) (string, string, error) {
	// Se dataLocation non specificato, usa cloud come default
	if dataLocation == "" || dataLocation == "none" {
		dataLocation = "cloud_cluster"
	}

	// Verifica che il cluster target sia disponibile
	targetMetrics := metrics.GetCluster(dataLocation)
	if targetMetrics == nil || !targetMetrics.Available {
		return "", "", fmt.Errorf("target cluster %s not available", dataLocation)
	}

	// Calcola risorse totali richieste dalla pipeline
	totalResources := CalculatePipelineResources(pipeline, s.parser)

	// Verifica che abbia risorse sufficienti per l'INTERA pipeline
	if targetMetrics.CPUAvailable < totalResources.TotalCPU {
		return "", "", fmt.Errorf(
			"insufficient CPU on %s: pipeline needs %d mCores, available %d mCores",
			dataLocation, totalResources.TotalCPU, targetMetrics.CPUAvailable)
	}

	if targetMetrics.MemoryAvailable < totalResources.TotalMemory {
		return "", "", fmt.Errorf(
			"insufficient memory on %s: pipeline needs %d bytes, available %d bytes",
			dataLocation, totalResources.TotalMemory, targetMetrics.MemoryAvailable)
	}

	decision := fmt.Sprintf(
		"Data-locality strategy: Entire pipeline (%d executors) placed on %s (data location). "+
			"Required: %d mCores CPU (%.2f cores), %d bytes memory (%.2f GB). "+
			"Minimizes data transfer latency.",
		totalResources.ExecutorCount,
		dataLocation,
		totalResources.TotalCPU,
		float64(totalResources.TotalCPU)/1000,
		totalResources.TotalMemory,
		float64(totalResources.TotalMemory)/1_000_000_000,
	)

	return dataLocation, decision, nil
}
