package pipeline

import (
	"context"
	"fmt"

	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

// PipelineStrategy definisce l'interfaccia per strategie di placement di pipeline
type PipelineStrategy interface {
	// Name ritorna il nome della strategia
	Name() string

	// SelectCluster decide su quale cluster eseguire l'INTERA pipeline
	// Ritorna: clusterName, decisione motivata, error
	SelectCluster(
		ctx context.Context,
		pipeline *PipelineIR,
		dataLocation string,
		metrics *placement.ClusterMetrics,
	) (string, string, error)
}

// PipelinePlacement rappresenta la decisione di placement per una pipeline
type PipelinePlacement struct {
	// TargetCluster è il cluster su cui verrà eseguita l'intera pipeline
	TargetCluster string

	// Decision spiega perché questo cluster è stato scelto
	Decision string

	// TotalResources traccia le risorse totali richieste dalla pipeline
	TotalResources PipelineResources

	// AvailableAfterPlacement traccia le risorse che rimarranno disponibili
	AvailableAfterPlacement int64 // CPU in millicores
}

// PipelineResources rappresenta le risorse totali di una pipeline
type PipelineResources struct {
	TotalCPU      int64 // millicores
	TotalMemory   int64 // bytes
	TotalGPU      int
	ExecutorCount int
}

// NewPipelinePlacement crea un nuovo PipelinePlacement
func NewPipelinePlacement(targetCluster, decision string, resources PipelineResources) PipelinePlacement {
	return PipelinePlacement{
		TargetCluster:  targetCluster,
		Decision:       decision,
		TotalResources: resources,
	}
}

// Summary genera un riepilogo testuale del placement
func (pp *PipelinePlacement) Summary() string {
	summary := fmt.Sprintf("Pipeline placed on: %s\n", pp.TargetCluster)
	summary += fmt.Sprintf("\nTotal resources required:\n")
	summary += fmt.Sprintf("  Executors: %d\n", pp.TotalResources.ExecutorCount)
	summary += fmt.Sprintf("  CPU: %d mCores (%.2f cores)\n",
		pp.TotalResources.TotalCPU, float64(pp.TotalResources.TotalCPU)/1000)
	summary += fmt.Sprintf("  Memory: %d bytes (%.2f GB)\n",
		pp.TotalResources.TotalMemory, float64(pp.TotalResources.TotalMemory)/1_000_000_000)
	if pp.TotalResources.TotalGPU > 0 {
		summary += fmt.Sprintf("  GPU: %d\n", pp.TotalResources.TotalGPU)
	}

	if pp.Decision != "" {
		summary += fmt.Sprintf("\nDecision rationale:\n  %s\n", pp.Decision)
	}

	return summary
}

// CalculatePipelineResources calcola le risorse totali di una pipeline
func CalculatePipelineResources(pipeline *PipelineIR, parser *Parser) PipelineResources {
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
