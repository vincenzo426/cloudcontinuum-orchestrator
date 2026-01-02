package pipeline

import (
	"context"
	"fmt"

	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

// DataLocalityPipelineStrategy esegue la pipeline dove risiedono i dati.
type DataLocalityPipelineStrategy struct {
	parser *Parser
}

// NewDataLocalityPipelineStrategy crea una nuova strategia data-locality.
func NewDataLocalityPipelineStrategy() *DataLocalityPipelineStrategy {
	return &DataLocalityPipelineStrategy{
		parser: NewParser(),
	}
}

// Name ritorna il nome della strategia.
func (s *DataLocalityPipelineStrategy) Name() string {
	return "data-locality-pipeline"
}

// SelectCluster seleziona il cluster dove risiedono i dati.
func (s *DataLocalityPipelineStrategy) SelectCluster(
	ctx context.Context,
	pipeline *PipelineIR,
	dataLocation string,
	metrics *placement.ClusterMetrics,
) (string, string, error) {

	// Default a cloud se dataLocation non specificato
	if dataLocation == "" || dataLocation == "none" {
		dataLocation = defaultDataLocation
	}

	// Valida disponibilità cluster target
	targetMetric, err := validateClusterAvailability(dataLocation, metrics)
	if err != nil {
		decision := fmt.Sprintf("Data-locality: %s", err.Error())
		return "", decision, err
	}

	// Calcola risorse richieste
	totalResources := CalculatePipelineResources(pipeline, s.parser)

	// Valida risorse sufficienti
	if err := validateClusterResources(dataLocation, targetMetric, totalResources); err != nil {
		decision := fmt.Sprintf("Data-locality: %s", err.Error())
		return "", decision, err
	}

	// Genera decisione
	decision := fmt.Sprintf(
		"Data-locality: Entire pipeline (%d executors) on %s (data location). "+
			"Required: %s. Minimizes data transfer latency.",
		totalResources.ExecutorCount,
		dataLocation,
		formatResourceRequirements(totalResources),
	)

	return dataLocation, decision, nil
}
