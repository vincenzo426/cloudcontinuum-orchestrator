package pipeline

import (
	"fmt"

	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Parser gestisce il parsing di Kubeflow Pipeline IR YAML
type Parser struct{}

// NewParser crea un nuovo parser
func NewParser() *Parser {
	return &Parser{}
}

// PipelineIR rappresenta la struttura completa di una Kubeflow Pipeline v2.1.0
type PipelineIR struct {
	SchemaVersion  string               `yaml:"schemaVersion"`
	SDKVersion     string               `yaml:"sdkVersion"`
	PipelineInfo   PipelineInfo         `yaml:"pipelineInfo"`
	Components     map[string]Component `yaml:"components"`
	DeploymentSpec DeploymentSpec       `yaml:"deploymentSpec"`
	Root           Root                 `yaml:"root"`
}

// PipelineInfo contiene metadati della pipeline
type PipelineInfo struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description,omitempty"`
}

// Component rappresenta un componente della pipeline
type Component struct {
	ExecutorLabel     string       `yaml:"executorLabel"`
	InputDefinitions  *Definitions `yaml:"inputDefinitions,omitempty"`
	OutputDefinitions *Definitions `yaml:"outputDefinitions,omitempty"`
}

// Definitions contiene definizioni di input/output
type Definitions struct {
	Artifacts  map[string]interface{} `yaml:"artifacts,omitempty"`
	Parameters map[string]interface{} `yaml:"parameters,omitempty"`
}

// DeploymentSpec contiene gli executors
type DeploymentSpec struct {
	Executors map[string]Executor `yaml:"executors"`
}

// Executor rappresenta un executor (container)
type Executor struct {
	Container Container `yaml:"container"`
}

// Container contiene la specifica del container
type Container struct {
	Image     string              `yaml:"image"`
	Command   []string            `yaml:"command,omitempty"`
	Args      []string            `yaml:"args,omitempty"`
	Resources *ContainerResources `yaml:"resources,omitempty"`
	Env       []map[string]string `yaml:"env,omitempty"`
}

// ContainerResources specifica le risorse del container
type ContainerResources struct {
	CPULimit    interface{} `yaml:"cpuLimit,omitempty"`
	MemoryLimit interface{} `yaml:"memoryLimit,omitempty"`
	GPULimit    interface{} `yaml:"gpuLimit,omitempty"`
}

// Root contiene il DAG della pipeline
type Root struct {
	DAG              DAG          `yaml:"dag"`
	InputDefinitions *Definitions `yaml:"inputDefinitions,omitempty"`
}

// DAG rappresenta il grafo delle task
type DAG struct {
	Tasks map[string]Task `yaml:"tasks"`
}

// Task rappresenta una singola task
type Task struct {
	ComponentRef   ComponentRef           `yaml:"componentRef"`
	DependentTasks []string               `yaml:"dependentTasks,omitempty"`
	Inputs         *TaskInputs            `yaml:"inputs,omitempty"`
	TaskInfo       TaskInfo               `yaml:"taskInfo"`
	CachingOptions map[string]interface{} `yaml:"cachingOptions,omitempty"`
}

// ComponentRef referenzia un componente
type ComponentRef struct {
	Name string `yaml:"name"`
}

// TaskInputs contiene gli input di una task
type TaskInputs struct {
	Artifacts  map[string]interface{} `yaml:"artifacts,omitempty"`
	Parameters map[string]interface{} `yaml:"parameters,omitempty"`
}

// TaskInfo contiene informazioni sulla task
type TaskInfo struct {
	Name string `yaml:"name"`
}

// ParsePipelineIR esegue il parsing di un file YAML Kubeflow Pipeline IR
func (p *Parser) ParsePipelineIR(yamlData []byte) (*PipelineIR, error) {
	var pipeline PipelineIR

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

// CalculateTotalResources calcola le risorse totali richieste dalla pipeline
func (p *Parser) CalculateTotalResources(pipeline *PipelineIR) (cpu, memory int64, gpu int) {
	if pipeline == nil || pipeline.DeploymentSpec.Executors == nil {
		return 0, 0, 0
	}

	var totalCPU, totalMemory int64
	var totalGPU int

	for _, executor := range pipeline.DeploymentSpec.Executors {
		execCPU, execMem, execGPU := p.parseExecutorResources(executor)
		totalCPU += execCPU
		totalMemory += execMem
		totalGPU += execGPU
	}

	return totalCPU, totalMemory, totalGPU
}

// parseExecutorResources estrae le risorse da un singolo executor
func (p *Parser) parseExecutorResources(executor Executor) (cpu, memory int64, gpu int) {
	// Default values (Kubernetes defaults per container senza limiti)
	defaultCPU := int64(100)          // 100m
	defaultMemory := int64(134217728) // 128Mi

	if executor.Container.Resources == nil {
		return defaultCPU, defaultMemory, 0
	}

	// Parse CPU
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

	// Parse Memory
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

	// Parse GPU
	if executor.Container.Resources.GPULimit != nil {
		switch v := executor.Container.Resources.GPULimit.(type) {
		case int:
			gpu = v
		case float64:
			gpu = int(v)
		case string:
			// GPU è tipicamente un numero intero
			var parsed int
			fmt.Sscanf(v, "%d", &parsed)
			gpu = parsed
		}
	}

	return cpu, memory, gpu
}

func (p *Parser) ParseExecutorResources(executor Executor) (cpu, memory int64, gpu int) {
	return p.parseExecutorResources(executor)
}

// parseResourceValue converte un valore di risorsa in int64
// Supporta: float64, int, string (Kubernetes quantity format)
func (p *Parser) parseResourceValue(value interface{}, isCPU bool) (int64, error) {
	switch v := value.(type) {
	case float64:
		if isCPU {
			// CPU: float rappresenta cores, converti in millicores
			return int64(v * 1000), nil
		}
		// Memory: float rappresenta gigabytes decimali (×10^9), NON gibibytes
		// Esempio: 0.536870912 = 536,870,912 bytes (esattamente 512 MiB)
		return int64(v * 1_000_000_000), nil

	case int:
		if isCPU {
			return int64(v * 1000), nil // Assumi cores
		}
		return int64(v), nil

	case string:
		// Usa Kubernetes resource.Quantity per parsing
		quantity, err := resource.ParseQuantity(v)
		if err != nil {
			return 0, fmt.Errorf("invalid resource quantity: %w", err)
		}

		if isCPU {
			return quantity.MilliValue(), nil
		}
		return quantity.Value(), nil

	default:
		return 0, fmt.Errorf("unsupported resource value type: %T", value)
	}
}

// GetTaskDependencies estrae la mappa delle dipendenze tra task
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

// GetExecutorNames ritorna la lista dei nomi degli executor
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

// GetTaskNames ritorna la lista dei nomi delle task
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
