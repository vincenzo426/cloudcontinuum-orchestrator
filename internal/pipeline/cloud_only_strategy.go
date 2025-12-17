package pipeline

import (
	"context"
	"fmt"

	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

// CloudOnlyPipelineStrategy esegue l'intera pipeline sul cloud cluster
type CloudOnlyPipelineStrategy struct {
	parser *Parser
}

// NewCloudOnlyPipelineStrategy crea una nuova strategia cloud-only
func NewCloudOnlyPipelineStrategy() *CloudOnlyPipelineStrategy {
	return &CloudOnlyPipelineStrategy{
		parser: NewParser(),
	}
}

// Name ritorna il nome della strategia
func (s *CloudOnlyPipelineStrategy) Name() string {
	return "cloud-only-pipeline"
}

// SelectCluster seleziona il cloud cluster per eseguire la pipeline
func (s *CloudOnlyPipelineStrategy) SelectCluster(
	ctx context.Context,
	pipeline *PipelineIR,
	dataLocation string,
	metrics *placement.ClusterMetrics,
) (string, string, error) {
	cloudCluster := "cloud_cluster"

	// Verifica che il cloud cluster sia disponibile
	cloudMetrics := metrics.GetCluster(cloudCluster)
	if cloudMetrics == nil || !cloudMetrics.Available {
		decision := "Cloud-only strategy: cloud_cluster is not available."
		return "", decision, fmt.Errorf("cloud cluster not available")
	}

	// Calcola risorse totali richieste dalla pipeline
	totalResources := CalculatePipelineResources(pipeline, s.parser)

	// Verifica che il cloud abbia risorse sufficienti per l'INTERA pipeline
	if cloudMetrics.CPUAvailable < totalResources.TotalCPU {
		decision := fmt.Sprintf(
			"Cloud-only strategy: Insufficient CPU on cloud cluster. "+
				"Pipeline needs %d mCores, available %d mCores.",
			totalResources.TotalCPU, cloudMetrics.CPUAvailable)
		return "", decision, fmt.Errorf("insufficient CPU on cloud cluster: pipeline needs %d mCores, available %d mCores",
			totalResources.TotalCPU, cloudMetrics.CPUAvailable)
	}

	if cloudMetrics.MemoryAvailable < totalResources.TotalMemory {
		decision := fmt.Sprintf(
			"Cloud-only strategy: Insufficient memory on cloud cluster. "+
				"Pipeline needs %d bytes, available %d bytes.",
			totalResources.TotalMemory, cloudMetrics.MemoryAvailable)
		return "", decision, fmt.Errorf(
			"insufficient memory on cloud cluster: pipeline needs %d bytes, available %d bytes",
			totalResources.TotalMemory, cloudMetrics.MemoryAvailable)
	}

	decision := fmt.Sprintf(
		"Cloud-only strategy: Entire pipeline (%d executors) placed on cloud_cluster. "+
			"Required: %d mCores CPU (%.2f cores), %d bytes memory (%.2f GB). "+
			"Available after placement: %d mCores CPU.",
		totalResources.ExecutorCount,
		totalResources.TotalCPU,
		float64(totalResources.TotalCPU)/1000,
		totalResources.TotalMemory,
		float64(totalResources.TotalMemory)/1_000_000_000,
		cloudMetrics.CPUAvailable-totalResources.TotalCPU,
	)

	return cloudCluster, decision, nil
}
