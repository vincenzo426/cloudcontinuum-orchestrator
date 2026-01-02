package pipeline

import (
	"context"
	"fmt"

	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

// Costanti per formattazione
const (
	millicoresToCores = 1000.0
	bytesToGigabytes  = 1_000_000_000.0
)

// PipelineStrategy definisce l'interfaccia per le strategie di placement.
// Ogni strategia decide su quale cluster eseguire l'intera pipeline.
type PipelineStrategy interface {
	// Name ritorna il nome della strategia.
	Name() string

	// SelectCluster decide su quale cluster eseguire la pipeline.
	// Ritorna: cluster name, decisione motivata, errore.
	SelectCluster(
		ctx context.Context,
		pipeline *PipelineIR,
		dataLocation string,
		metrics *placement.ClusterMetrics,
	) (string, string, error)
}

// PipelinePlacement rappresenta la decisione di placement.
type PipelinePlacement struct {
	TargetCluster           string
	Decision                string
	TotalResources          PipelineResources
	AvailableAfterPlacement int64
}

// PipelineResources rappresenta le risorse totali aggregate.
type PipelineResources struct {
	TotalCPU      int64
	TotalMemory   int64
	TotalGPU      int
	ExecutorCount int
}

// NewPipelinePlacement crea un nuovo oggetto PipelinePlacement.
func NewPipelinePlacement(
	targetCluster string,
	decision string,
	resources PipelineResources,
) PipelinePlacement {
	return PipelinePlacement{
		TargetCluster:  targetCluster,
		Decision:       decision,
		TotalResources: resources,
	}
}

// Summary genera un riepilogo testuale del placement.
func (pp *PipelinePlacement) Summary() string {
	return fmt.Sprintf(
		"Pipeline placed on: %s\n"+
			"\nTotal resources required:\n"+
			"  Executors: %d\n"+
			"  CPU: %d mCores (%.2f cores)\n"+
			"  Memory: %d bytes (%.2f GB)\n"+
			"%s"+
			"%s",
		pp.TargetCluster,
		pp.TotalResources.ExecutorCount,
		pp.TotalResources.TotalCPU,
		float64(pp.TotalResources.TotalCPU)/millicoresToCores,
		pp.TotalResources.TotalMemory,
		float64(pp.TotalResources.TotalMemory)/bytesToGigabytes,
		pp.formatGPU(),
		pp.formatDecision(),
	)
}

// formatGPU formatta l'output GPU se presente.
func (pp *PipelinePlacement) formatGPU() string {
	if pp.TotalResources.TotalGPU > 0 {
		return fmt.Sprintf("  GPU: %d\n", pp.TotalResources.TotalGPU)
	}
	return ""
}

// formatDecision formatta la motivazione se presente.
func (pp *PipelinePlacement) formatDecision() string {
	if pp.Decision != "" {
		return fmt.Sprintf("\nDecision rationale:\n  %s\n", pp.Decision)
	}
	return ""
}

// CalculatePipelineResources calcola le risorse totali richieste.
func CalculatePipelineResources(
	pipeline *PipelineIR,
	parser *Parser,
) PipelineResources {
	var resources PipelineResources

	if pipeline == nil || pipeline.DeploymentSpec.Executors == nil {
		return resources
	}

	resources.ExecutorCount = len(pipeline.DeploymentSpec.Executors)

	for _, executor := range pipeline.DeploymentSpec.Executors {
		cpu, memory, gpu := parser.ParseExecutorResources(executor)
		resources.TotalCPU += cpu
		resources.TotalMemory += memory
		resources.TotalGPU += gpu
	}

	return resources
}
