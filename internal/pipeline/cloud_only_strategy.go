package pipeline

import (
	"context"
	"fmt"

	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

// CloudOnlyPipelineStrategy esegue l'intera pipeline sul cloud cluster.
type CloudOnlyPipelineStrategy struct {
	parser *Parser
}

// NewCloudOnlyPipelineStrategy crea una nuova strategia cloud-only.
func NewCloudOnlyPipelineStrategy() *CloudOnlyPipelineStrategy {
	return &CloudOnlyPipelineStrategy{
		parser: NewParser(),
	}
}

// Name ritorna il nome della strategia.
func (s *CloudOnlyPipelineStrategy) Name() string {
	return "cloud-only-pipeline"
}

// SelectCluster seleziona sempre il cloud cluster.
func (s *CloudOnlyPipelineStrategy) SelectCluster(
	ctx context.Context,
	pipeline *PipelineIR,
	dataLocation string,
	dataSize string,
	metrics *placement.ClusterMetrics,
) (string, string, error) {

	// Valida disponibilità cloud
	cloudMetric, err := validateClusterAvailability(cloudClusterName, metrics)
	if err != nil {
		decision := fmt.Sprintf("Cloud-only: %s", err.Error())
		return "", decision, err
	}

	// Calcola risorse richieste
	totalResources := CalculatePipelineResources(pipeline, s.parser)

	// Valida risorse sufficienti
	if err := validateClusterResources(cloudClusterName, cloudMetric, totalResources); err != nil {
		decision := fmt.Sprintf("Cloud-only: %s", err.Error())
		return "", decision, err
	}

	// Genera decisione
	decision := fmt.Sprintf(
		"Cloud-only: Entire pipeline (%d executors) on cloud_cluster. "+
			"Required: %s. Available after: %d mCores CPU.",
		totalResources.ExecutorCount,
		formatResourceRequirements(totalResources),
		cloudMetric.CPUAvailable-totalResources.TotalCPU,
	)

	return cloudClusterName, decision, nil
}
