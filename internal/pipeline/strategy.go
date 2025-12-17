package pipeline

import (
	"context"
	"fmt"

	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

// ============================================================================
// INTERFACCIA STRATEGIA
// ============================================================================

// PipelineStrategy definisce l'interfaccia per le strategie di placement delle pipeline.
// Ogni strategia implementa una diversa logica per decidere su quale cluster
// eseguire un'intera pipeline Kubeflow.
type PipelineStrategy interface {
	// Name ritorna il nome identificativo della strategia.
	// Deve corrispondere al valore usato in PipelinePlacementRequest.spec.placementStrategy
	Name() string

	// SelectCluster decide su quale cluster eseguire l'INTERA pipeline.
	// Questa funzione tratta la pipeline come unità atomica che deve essere eseguita su un singolo cluster.
	//
	// Parametri:
	//   - ctx: context per timeout e cancellazione
	//   - pipeline: Pipeline IR parsata con metadata e requisiti risorse
	//   - dataLocation: suggerimento sulla località dei dati (può essere ignorato)
	//   - metrics: metriche correnti di tutti i cluster disponibili
	//
	// Ritorna:
	//   - string: nome del cluster selezionato (es. "cloud_cluster", "edge_cluster_1")
	//   - string: decisione motivata
	//   - error: errore se nessun cluster soddisfa i requisiti
	SelectCluster(
		ctx context.Context,
		pipeline *PipelineIR,
		dataLocation string,
		metrics *placement.ClusterMetrics,
	) (string, string, error)
}

// ============================================================================
// STRUTTURE DATI - PLACEMENT
// ============================================================================

// PipelinePlacement rappresenta la decisione di placement per una pipeline.
// Contiene tutte le informazioni sulla decisione presa, incluse risorse
// richieste e disponibilità residua sul cluster target.
type PipelinePlacement struct {
	TargetCluster string // Cluster su cui verrà eseguita l'intera pipeline

	Decision string // Spiegazione della scelta

	TotalResources PipelineResources // Risorse totali richieste dalla pipeline

	AvailableAfterPlacement int64 // CPU disponibile dopo placement (millicores)
}

// PipelineResources rappresenta le risorse totali aggregate di una pipeline.
// Somma i requisiti di tutti gli executor/container della pipeline.
type PipelineResources struct {
	TotalCPU      int64 // Totale CPU in millicores (es. 2000 = 2 cores)
	TotalMemory   int64 // Totale memoria in bytes
	TotalGPU      int   // Totale GPU richieste
	ExecutorCount int   // Numero di executor nella pipeline
}

// ============================================================================
// COSTRUTTORI
// ============================================================================

// NewPipelinePlacement crea un nuovo oggetto PipelinePlacement.
//
// Parametri:
//   - targetCluster: nome del cluster selezionato
//   - decision: motivazione della scelta
//   - resources: risorse totali richieste
//
// Ritorna un PipelinePlacement popolato.
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

// ============================================================================
// METODI DI UTILITÀ
// ============================================================================

// Summary genera un riepilogo testuale human-readable del placement.
// Utile per logging e debugging delle decisioni di placement.
//
// Ritorna una stringa multi-linea formattata con:
//   - Cluster target
//   - Risorse richieste (CPU, memoria, GPU se presente)
//   - Motivazione della decisione
func (pp *PipelinePlacement) Summary() string {
	summary := fmt.Sprintf("Pipeline placed on: %s\n", pp.TargetCluster)
	summary += fmt.Sprintf("\nTotal resources required:\n")
	summary += fmt.Sprintf("  Executors: %d\n", pp.TotalResources.ExecutorCount)
	summary += fmt.Sprintf("  CPU: %d mCores (%.2f cores)\n",
		pp.TotalResources.TotalCPU,
		float64(pp.TotalResources.TotalCPU)/1000)
	summary += fmt.Sprintf("  Memory: %d bytes (%.2f GB)\n",
		pp.TotalResources.TotalMemory,
		float64(pp.TotalResources.TotalMemory)/1_000_000_000)

	// Includi GPU solo se richiesta
	if pp.TotalResources.TotalGPU > 0 {
		summary += fmt.Sprintf("  GPU: %d\n", pp.TotalResources.TotalGPU)
	}

	// Aggiungi motivazione se disponibile
	if pp.Decision != "" {
		summary += fmt.Sprintf("\nDecision rationale:\n  %s\n", pp.Decision)
	}

	return summary
}

// ============================================================================
// CALCOLO RISORSE
// ============================================================================

// CalculatePipelineResources calcola le risorse totali richieste da una pipeline.
// Somma i requisiti di tutti gli executor definiti nel DeploymentSpec.
//
// Parametri:
//   - pipeline: Pipeline IR parsata
//   - parser: Parser per estrarre risorse dagli executor
//
// Ritorna:
//   - PipelineResources: aggregato delle risorse richieste
func CalculatePipelineResources(
	pipeline *PipelineIR,
	parser *Parser,
) PipelineResources {
	var resources PipelineResources

	// Gestisci pipeline vuote o malformate
	if pipeline == nil || pipeline.DeploymentSpec.Executors == nil {
		return resources
	}

	resources.ExecutorCount = len(pipeline.DeploymentSpec.Executors)

	// Somma risorse di ogni executor
	for _, executor := range pipeline.DeploymentSpec.Executors {
		cpu, memory, gpu := parser.ParseExecutorResources(executor)
		resources.TotalCPU += cpu
		resources.TotalMemory += memory
		resources.TotalGPU += gpu
	}

	return resources
}