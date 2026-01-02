package pipeline

import (
	"fmt"

	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/resource"
)

// ============================================================================
// PARSER
// ============================================================================

// Parser gestisce il parsing dei file YAML Kubeflow Pipeline IR v2.1.0.
// Estrae metadata, componenti, executor e requisiti di risorse dalle pipeline.
//
// Supporto:
//   - Schema version: 2.1.0
//   - Resource parsing: CPU, memoria, GPU
//   - Formato risorse: float, int, Kubernetes quantity strings
type Parser struct{}

// NewParser crea una nuova istanza del parser.
func NewParser() *Parser {
	return &Parser{}
}

// ============================================================================
// STRUTTURE DATI - PIPELINE IR v2.1.0
// ============================================================================

// PipelineIR rappresenta la struttura completa di una Kubeflow Pipeline v2.1.0.
// Questa è la rappresentazione intermedia (IR) usata da Kubeflow Pipelines SDK.
type PipelineIR struct {
	SchemaVersion  string               `yaml:"schemaVersion"`  // Deve essere "2.1.0"
	SDKVersion     string               `yaml:"sdkVersion"`     // Es. "kfp-2.5.0"
	PipelineInfo   PipelineInfo         `yaml:"pipelineInfo"`   // Metadata della pipeline
	Components     map[string]Component `yaml:"components"`     // Componenti riutilizzabili
	DeploymentSpec DeploymentSpec       `yaml:"deploymentSpec"` // Executor (container)
	Root           Root                 `yaml:"root"`           // DAG delle task
}

// PipelineInfo contiene i metadata descrittivi della pipeline.
type PipelineInfo struct {
	Name        string `yaml:"name"`                  // Nome univoco della pipeline
	Description string `yaml:"description,omitempty"` // Descrizione opzionale
}

// Component rappresenta un componente riutilizzabile della pipeline.
// Ogni componente definisce input/output e punta a un executor.
type Component struct {
	ExecutorLabel     string       `yaml:"executorLabel"`               // Label dell'executor da usare
	InputDefinitions  *Definitions `yaml:"inputDefinitions,omitempty"`  // Input del componente
	OutputDefinitions *Definitions `yaml:"outputDefinitions,omitempty"` // Output del componente
}

// Definitions contiene definizioni di input/output (artifacts e parameters).
type Definitions struct {
	Artifacts  map[string]interface{} `yaml:"artifacts,omitempty"`  // File/oggetti
	Parameters map[string]interface{} `yaml:"parameters,omitempty"` // Valori semplici
}

// DeploymentSpec contiene tutti gli executor (container) della pipeline.
// Ogni executor definisce come eseguire un componente.
type DeploymentSpec struct {
	Executors map[string]Executor `yaml:"executors"` // Mappa executorLabel -> Executor
}

// Executor rappresenta un executor (tipicamente un container).
// Definisce l'immagine, comandi e risorse necessarie per l'esecuzione.
type Executor struct {
	Container Container `yaml:"container"` // Specifica del container
}

// Container contiene la configurazione completa del container.
type Container struct {
	Image     string              `yaml:"image"`               // Immagine Docker
	Command   []string            `yaml:"command,omitempty"`   // Entrypoint
	Args      []string            `yaml:"args,omitempty"`      // Argomenti
	Resources *ContainerResources `yaml:"resources,omitempty"` // CPU, memoria, GPU
	Env       []map[string]string `yaml:"env,omitempty"`       // Variabili ambiente
}

// ContainerResources specifica i requisiti di risorse del container.
// Supporta vari formati: float, int, Kubernetes quantity strings.
type ContainerResources struct {
	CPULimit    interface{} `yaml:"cpuLimit,omitempty"`    // CPU limit
	MemoryLimit interface{} `yaml:"memoryLimit,omitempty"` // Memory limit
	GPULimit    interface{} `yaml:"gpuLimit,omitempty"`    // GPU limit
}

// Root contiene il DAG (Direct Acyclic Graph) della pipeline.
// Definisce le task e il loro ordine di esecuzione.
type Root struct {
	DAG              DAG          `yaml:"dag"`                        // Grafo delle task
	InputDefinitions *Definitions `yaml:"inputDefinitions,omitempty"` // Input della pipeline
}

// DAG rappresenta il grafo delle task con dipendenze.
type DAG struct {
	Tasks map[string]Task `yaml:"tasks"` // Mappa taskName -> Task
}

// Task rappresenta una singola task nel DAG.
// Ogni task referenzia un componente e definisce dipendenze.
type Task struct {
	ComponentRef   ComponentRef           `yaml:"componentRef"`             // Componente da eseguire
	DependentTasks []string               `yaml:"dependentTasks,omitempty"` // Task che devono completare prima
	Inputs         *TaskInputs            `yaml:"inputs,omitempty"`         // Input della task
	TaskInfo       TaskInfo               `yaml:"taskInfo"`                 // Metadata della task
	CachingOptions map[string]interface{} `yaml:"cachingOptions,omitempty"` // Opzioni di cache
}

// ComponentRef referenzia un componente definito in Components.
type ComponentRef struct {
	Name string `yaml:"name"` // Nome del componente
}

// TaskInputs contiene gli input di una task.
type TaskInputs struct {
	Artifacts  map[string]interface{} `yaml:"artifacts,omitempty"`  // Input artifacts
	Parameters map[string]interface{} `yaml:"parameters,omitempty"` // Input parameters
}

// TaskInfo contiene metadata della task.
type TaskInfo struct {
	Name string `yaml:"name"` // Nome display della task
}

// ============================================================================
// PARSING PRINCIPALE
// ============================================================================

// ParsePipelineIR esegue il parsing completo di un file YAML Kubeflow Pipeline IR.
//
// Parametri:
//   - yamlData: contenuto del file YAML come byte array
//
// Ritorna:
//   - *PipelineIR: pipeline parsata
//   - error: errore di parsing o versione non supportata
//
// Validazioni:
//   - Schema version deve essere "2.1.0"
//   - YAML deve essere ben formato
func (p *Parser) ParsePipelineIR(yamlData []byte) (*PipelineIR, error) {
	var pipeline PipelineIR

	// Unmarshal YAML
	if err := yaml.Unmarshal(yamlData, &pipeline); err != nil {
		return nil, fmt.Errorf("failed to parse pipeline YAML: %w", err)
	}

	// Valida schema version
	if pipeline.SchemaVersion != "2.1.0" {
		return nil, fmt.Errorf("unsupported schema version: %s (expected 2.1.0)",
			pipeline.SchemaVersion)
	}

	return &pipeline, nil
}

// ============================================================================
// CALCOLO RISORSE
// ============================================================================

// CalculateTotalResources calcola le risorse totali richieste dalla pipeline.
// Somma i requisiti di tutti gli executor definiti nel DeploymentSpec.
//
// Parametri:
//   - pipeline: Pipeline IR parsata
//
// Ritorna:
//   - cpu: CPU totale in millicores
//   - memory: memoria totale in bytes
//   - gpu: GPU totali richieste
//
// Note:
//   - Ritorna (0,0,0) per pipeline vuote o malformate
//   - Applica default Kubernetes per executor senza spec risorse
func (p *Parser) CalculateTotalResources(pipeline *PipelineIR) (cpu, memory int64, gpu int) {
	if pipeline == nil || pipeline.DeploymentSpec.Executors == nil {
		return 0, 0, 0
	}

	var totalCPU, totalMemory int64
	var totalGPU int

	// Somma risorse di ogni executor
	for _, executor := range pipeline.DeploymentSpec.Executors {
		execCPU, execMem, execGPU := p.parseExecutorResources(executor)
		totalCPU += execCPU
		totalMemory += execMem
		totalGPU += execGPU
	}

	return totalCPU, totalMemory, totalGPU
}

// parseExecutorResources estrae le risorse da un singolo executor.
// Applica default Kubernetes per container senza spec risorse.
//
// Parametri:
//   - executor: Executor da analizzare
//
// Ritorna:
//   - cpu: CPU in millicores (default: 100m)
//   - memory: memoria in bytes (default: 128Mi)
//   - gpu: numero GPU (default: 0)
//
// Default Kubernetes:
//   - CPU: 100 millicores (0.1 core)
//   - Memory: 128 MiB (134217728 bytes)
//   - GPU: 0
func (p *Parser) parseExecutorResources(executor Executor) (cpu, memory int64, gpu int) {
	// Default Kubernetes per container senza limiti/requests
	const (
		defaultCPU    = int64(100)       // 100 millicores
		defaultMemory = int64(134217728) // 128 MiB in bytes
	)

	// Nessuna spec risorse: usa default
	if executor.Container.Resources == nil {
		return defaultCPU, defaultMemory, 0
	}

	// ========================================================================
	// Parse CPU
	// ========================================================================
	if executor.Container.Resources.CPULimit != nil {
		parsedCPU, err := p.parseResourceValue(executor.Container.Resources.CPULimit, true)
		if err == nil {
			cpu = parsedCPU
		} else {
			cpu = defaultCPU
		}
	} else {
		cpu = defaultCPU
	}

	// ========================================================================
	// Parse Memory
	// ========================================================================
	if executor.Container.Resources.MemoryLimit != nil {
		parsedMem, err := p.parseResourceValue(executor.Container.Resources.MemoryLimit, false)
		if err == nil {
			memory = parsedMem
		} else {
			memory = defaultMemory
		}
	} else {
		memory = defaultMemory
	}

	// ========================================================================
	// Parse GPU
	// ========================================================================
	if executor.Container.Resources.GPULimit != nil {
		switch v := executor.Container.Resources.GPULimit.(type) {
		case int:
			gpu = v
		case float64:
			gpu = int(v)
		case string:
			// GPU è tipicamente un numero intero
			fmt.Sscanf(v, "%d", &gpu)
		}
	}

	return cpu, memory, gpu
}

// ParseExecutorResources è l'API pubblica per parseExecutorResources.
// Esposta per permettere a strategie di placement di usare il parser.
func (p *Parser) ParseExecutorResources(executor Executor) (cpu, memory int64, gpu int) {
	return p.parseExecutorResources(executor)
}

// ============================================================================
// PARSING VALORI RISORSE
// ============================================================================

// parseResourceValue converte un valore di risorsa in int64.
// Supporta formati multipli per compatibilità con Kubeflow IR.
//
// Parametri:
//   - value: valore da parsare (può essere float64, int, string)
//   - isCPU: true per CPU, false per memoria (cambia interpretazione)
//
// Ritorna:
//   - int64: valore parsato (millicores per CPU, bytes per memoria)
//   - error: errore di parsing
//
// Formati supportati:
//   - float64: per CPU = cores (es. 2.5 = 2500m), per memoria = bytes decimali
//   - int: per CPU = cores, per memoria = bytes
//   - string: formato Kubernetes quantity (es. "500m", "2Gi")
//
// Formato memoria float:
//   - Il float rappresenta bytes in notazione decimale (×10^9), NON gibibytes
//   - Esempio: 0.536870912 = 536,870,912 bytes = esattamente 512 MiB
//   - Questo corrisponde al formato usato da Kubeflow Python SDK
func (p *Parser) parseResourceValue(value interface{}, isCPU bool) (int64, error) {
	switch v := value.(type) {
	case float64:
		if isCPU {
			// CPU: float rappresenta cores, converti in millicores
			// Esempio: 2.5 cores = 2500 millicores
			return int64(v * 1000), nil
		}
		// Memory: float rappresenta bytes in notazione decimale (×10^9)
		// Esempio: 0.536870912 = 536,870,912 bytes = 512 MiB
		return int64(v * 1_000_000_000), nil

	case int:
		if isCPU {
			// CPU: int assume cores, converti in millicores
			return int64(v * 1000), nil
		}
		// Memory: int assume bytes
		return int64(v), nil

	case string:
		// Usa Kubernetes resource.Quantity per parsing robusto
		// Supporta: "500m", "2", "1Gi", "512Mi", etc.
		quantity, err := resource.ParseQuantity(v)
		if err != nil {
			return 0, fmt.Errorf("invalid resource quantity: %w", err)
		}

		if isCPU {
			// Converti in millicores
			return quantity.MilliValue(), nil
		}
		// Converti in bytes
		return quantity.Value(), nil

	default:
		return 0, fmt.Errorf("unsupported resource value type: %T", value)
	}
}

// ============================================================================
// ESTRAZIONE METADATA
// ============================================================================

// GetTaskDependencies estrae la mappa delle dipendenze tra task.
// Utile per analisi della topologia del DAG.
//
// Parametri:
//   - pipeline: Pipeline IR parsata
//
// Ritorna:
//   - map[string][]string: mappa taskName -> lista di task dipendenti
//
// Note:
//   - Mappa vuota per pipeline senza dipendenze
//   - Solo task con dipendenze sono incluse
func (p *Parser) GetTaskDependencies(pipeline *PipelineIR) map[string][]string {
	dependencies := make(map[string][]string)

	if pipeline == nil || pipeline.Root.DAG.Tasks == nil {
		return dependencies
	}

	for taskName, task := range pipeline.Root.DAG.Tasks {
		if len(task.DependentTasks) > 0 {
			dependencies[taskName] = task.DependentTasks
		}
	}

	return dependencies
}

// GetExecutorNames ritorna la lista dei nomi degli executor.
// Utile per debugging e validazione.
//
// Parametri:
//   - pipeline: Pipeline IR parsata
//
// Ritorna:
//   - []string: lista di nomi executor (ordine non garantito)
func (p *Parser) GetExecutorNames(pipeline *PipelineIR) []string {
	if pipeline == nil || pipeline.DeploymentSpec.Executors == nil {
		return nil
	}

	names := make([]string, 0, len(pipeline.DeploymentSpec.Executors))
	for name := range pipeline.DeploymentSpec.Executors {
		names = append(names, name)
	}
	return names
}

// GetTaskNames ritorna la lista dei nomi delle task nel DAG.
// Utile per debugging e validazione.
//
// Parametri:
//   - pipeline: Pipeline IR parsata
//
// Ritorna:
//   - []string: lista di nomi task (ordine non garantito)
func (p *Parser) GetTaskNames(pipeline *PipelineIR) []string {
	if pipeline == nil || pipeline.Root.DAG.Tasks == nil {
		return nil
	}

	names := make([]string, 0, len(pipeline.Root.DAG.Tasks))
	for name := range pipeline.Root.DAG.Tasks {
		names = append(names, name)
	}
	return names
}
