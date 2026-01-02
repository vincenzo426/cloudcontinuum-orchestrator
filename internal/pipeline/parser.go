package pipeline

import (
	"fmt"

	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Costanti di configurazione
const (
	supportedSchemaVersion = "2.1.0"

	// Default Kubernetes per container senza resource spec
	defaultCPUMillicores = int64(100)       // 100m = 0.1 core
	defaultMemoryBytes   = int64(134217728) // 128 MiB

	// Conversione unità risorse
	millicoresPerCore = 1000
	bytesPerGigabyte  = 1_000_000_000
)

// Parser gestisce il parsing dei file YAML Kubeflow Pipeline IR v2.1.0.
type Parser struct{}

// NewParser crea una nuova istanza del parser.
func NewParser() *Parser {
	return &Parser{}
}

// ParsePipelineIR esegue il parsing completo di un file YAML Kubeflow Pipeline IR.
func (p *Parser) ParsePipelineIR(yamlData []byte) (*PipelineIR, error) {
	var pipeline PipelineIR

	if err := yaml.Unmarshal(yamlData, &pipeline); err != nil {
		return nil, fmt.Errorf("failed to parse pipeline YAML: %w", err)
	}

	if pipeline.SchemaVersion != supportedSchemaVersion {
		return nil, fmt.Errorf("unsupported schema version: %s (expected %s)",
			pipeline.SchemaVersion, supportedSchemaVersion)
	}

	return &pipeline, nil
}

// CalculateTotalResources calcola le risorse totali richieste dalla pipeline.
// Ritorna CPU (millicores), memoria (bytes), GPU.
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

// parseExecutorResources estrae le risorse da un singolo executor.
func (p *Parser) parseExecutorResources(executor Executor) (cpu, memory int64, gpu int) {
	if executor.Container.Resources == nil {
		return defaultCPUMillicores, defaultMemoryBytes, 0
	}

	resources := executor.Container.Resources

	// Parse CPU
	cpu = defaultCPUMillicores
	if resources.CPULimit != nil {
		if parsedCPU, err := p.parseResourceValue(resources.CPULimit, true); err == nil {
			cpu = parsedCPU
		}
	}

	// Parse Memory
	memory = defaultMemoryBytes
	if resources.MemoryLimit != nil {
		if parsedMem, err := p.parseResourceValue(resources.MemoryLimit, false); err == nil {
			memory = parsedMem
		}
	}

	// Parse GPU
	if resources.GPULimit != nil {
		gpu = p.parseGPUValue(resources.GPULimit)
	}

	return cpu, memory, gpu
}

// ParseExecutorResources è l'API pubblica per parseExecutorResources.
func (p *Parser) ParseExecutorResources(executor Executor) (cpu, memory int64, gpu int) {
	return p.parseExecutorResources(executor)
}

// parseResourceValue converte un valore di risorsa in int64.
func (p *Parser) parseResourceValue(value interface{}, isCPU bool) (int64, error) {
	switch v := value.(type) {
	case float64:
		if isCPU {
			return int64(v * millicoresPerCore), nil
		}
		return int64(v * bytesPerGigabyte), nil

	case int:
		if isCPU {
			return int64(v * millicoresPerCore), nil
		}
		return int64(v), nil

	case string:
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

// parseGPUValue estrae il numero di GPU richieste.
func (p *Parser) parseGPUValue(value interface{}) int {
	switch v := value.(type) {
	case int:
		return v
	case float64:
		return int(v)
	case string:
		var gpu int
		fmt.Sscanf(v, "%d", &gpu)
		return gpu
	default:
		return 0
	}
}

// GetTaskDependencies estrae la mappa delle dipendenze tra task.
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
