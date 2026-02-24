package pipeline

import (
	"context"
	"fmt"
	"sort"
	"strings"

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

	// Euristica 3: Data locality per pipeline leggere con offloading graduale sugli edge
	if dataLocation != "" && dataLocation != "none" {
		// 3a. Prima prova sul cluster dove risiedono i dati
		cluster, decision, err := s.dataLocalityStrategy.SelectCluster(ctx, pipeline, dataLocation, "", metrics)
		if err == nil {
			decision = fmt.Sprintf("Simple-heuristic: Light pipeline (%s), delegating to data-locality. %s",
				formatCPUCores(totalResources.TotalCPU), decision)
			return cluster, decision, nil
		}

		// 3b. Data locality fallita, prova offloading sugli altri edge
		cluster, decision, err = s.tryEdgeOffloading(totalResources, dataLocation, metrics)
		if err == nil {
			return cluster, decision, nil
		}
		// Se anche l'offloading sugli edge fallisce, continua con fallback
	}

	// Euristica 4: Fallback → cluster con più risorse (incluso cloud)
	return s.selectBestAvailableCluster(totalResources, metrics)
}

// tryEdgeOffloading prova a piazzare la pipeline sugli altri cluster edge
// in ordine di risorse disponibili (dal più capiente al meno capiente).
func (s *SimplePipelineHeuristicStrategy) tryEdgeOffloading(
	resources PipelineResources,
	excludeCluster string,
	metrics *placement.ClusterMetrics,
) (string, string, error) {

	// Raccogli tutti gli edge disponibili (escluso quello dove risiede il dato)
	type edgeCandidate struct {
		name         string
		cpuAvailable int64
		metric       *placement.ClusterMetric
	}

	var candidates []edgeCandidate

	for clusterName, clusterMetric := range metrics.Clusters {
		// Salta il cluster escluso (quello dove risiedono i dati, già provato)
		if clusterName == excludeCluster {
			continue
		}

		// Salta il cloud (lo useremo solo nel fallback finale)
		if clusterName == cloudClusterName {
			continue
		}

		// Salta cluster non disponibili
		if !clusterMetric.Available {
			continue
		}

		// Verifica che sia un edge (nome contiene "edge")
		if !isEdgeCluster(clusterName) {
			continue
		}

		candidates = append(candidates, edgeCandidate{
			name:         clusterName,
			cpuAvailable: clusterMetric.CPUAvailable,
			metric:       clusterMetric,
		})
	}

	// Ordina gli edge per CPU disponibile (decrescente)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].cpuAvailable > candidates[j].cpuAvailable
	})

	// Prova ogni edge in ordine
	var triedEdges []string
	for _, candidate := range candidates {
		triedEdges = append(triedEdges, candidate.name)

		// Verifica risorse sufficienti
		if candidate.metric.CPUAvailable >= resources.TotalCPU &&
			candidate.metric.MemoryAvailable >= resources.TotalMemory {

			decision := fmt.Sprintf(
				"Simple-heuristic: Edge offloading to %s (data at %s unavailable). "+
					"Available: %d mCores. Required: %s. Tried edges: [%s].",
				candidate.name,
				excludeCluster,
				candidate.cpuAvailable,
				formatResourceRequirements(resources),
				strings.Join(triedEdges, ", "),
			)
			return candidate.name, decision, nil
		}
	}

	// Nessun edge ha risorse sufficienti
	if len(triedEdges) > 0 {
		return "", fmt.Sprintf(
			"Simple-heuristic: All edge clusters saturated. Tried: [%s]. Required: %s.",
			strings.Join(triedEdges, ", "),
			formatResourceRequirements(resources),
		), fmt.Errorf("all edge clusters saturated")
	}

	return "", "Simple-heuristic: No other edge clusters available.", fmt.Errorf("no edge clusters available")
}

// isEdgeCluster verifica se un cluster è un edge basandosi sul nome.
func isEdgeCluster(clusterName string) bool {
	return strings.Contains(strings.ToLower(clusterName), "edge")
}

// selectBestAvailableCluster seleziona il cluster con più risorse disponibili.
// Questo è il fallback finale che include anche il cloud.
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
