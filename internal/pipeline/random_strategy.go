package pipeline

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

// RandomPipelineStrategy seleziona un cluster casuale tra quelli disponibili.
type RandomPipelineStrategy struct {
	parser *Parser
	rng    *rand.Rand
}

// NewRandomPipelineStrategy crea una nuova strategia random.
func NewRandomPipelineStrategy() *RandomPipelineStrategy {
	return &RandomPipelineStrategy{
		parser: NewParser(),
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Name ritorna il nome della strategia.
func (s *RandomPipelineStrategy) Name() string {
	return "random-pipeline"
}

// SelectCluster seleziona un cluster casuale tra quelli con risorse sufficienti.
func (s *RandomPipelineStrategy) SelectCluster(
	ctx context.Context,
	pipeline *PipelineIR,
	dataLocation string,
	dataSize string,
	metrics *placement.ClusterMetrics,
) (string, string, error) {

	totalResources := CalculatePipelineResources(pipeline, s.parser)

	// Raccogli tutti i cluster disponibili con risorse sufficienti
	var eligibleClusters []string

	for clusterName, clusterMetric := range metrics.Clusters {
		if !clusterMetric.Available {
			continue
		}

		if clusterMetric.CPUAvailable >= totalResources.TotalCPU &&
			clusterMetric.MemoryAvailable >= totalResources.TotalMemory {
			eligibleClusters = append(eligibleClusters, clusterName)
		}
	}

	// Nessun cluster disponibile
	if len(eligibleClusters) == 0 {
		decision := fmt.Sprintf(
			"Random: No cluster has sufficient resources (needs %s)",
			formatResourceRequirements(totalResources))
		return "", decision, fmt.Errorf("no cluster has sufficient resources")
	}

	// Selezione casuale
	selectedIdx := s.rng.Intn(len(eligibleClusters))
	selectedCluster := eligibleClusters[selectedIdx]

	selectedMetric := metrics.GetCluster(selectedCluster)

	decision := fmt.Sprintf(
		"Random: Selected %s randomly from %d eligible clusters. "+
			"Required: %s. Available on target: %d mCores CPU, %.2f GB memory.",
		selectedCluster,
		len(eligibleClusters),
		formatResourceRequirements(totalResources),
		selectedMetric.CPUAvailable,
		float64(selectedMetric.MemoryAvailable)/bytesToGigabytes,
	)

	return selectedCluster, decision, nil
}
