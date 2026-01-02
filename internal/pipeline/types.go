package pipeline

// Strutture dati - Kubeflow Pipeline IR v2.1.0

// PipelineIR rappresenta la struttura completa di una Kubeflow Pipeline v2.1.0.
type PipelineIR struct {
	SchemaVersion  string               `yaml:"schemaVersion"`
	SDKVersion     string               `yaml:"sdkVersion"`
	PipelineInfo   PipelineInfo         `yaml:"pipelineInfo"`
	Components     map[string]Component `yaml:"components"`
	DeploymentSpec DeploymentSpec       `yaml:"deploymentSpec"`
	Root           Root                 `yaml:"root"`
}

// PipelineInfo contiene i metadata della pipeline.
type PipelineInfo struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description,omitempty"`
}

// Component rappresenta un componente riutilizzabile.
type Component struct {
	ExecutorLabel     string       `yaml:"executorLabel"`
	InputDefinitions  *Definitions `yaml:"inputDefinitions,omitempty"`
	OutputDefinitions *Definitions `yaml:"outputDefinitions,omitempty"`
}

// Definitions contiene definizioni di input/output.
type Definitions struct {
	Artifacts  map[string]interface{} `yaml:"artifacts,omitempty"`
	Parameters map[string]interface{} `yaml:"parameters,omitempty"`
}

// DeploymentSpec contiene tutti gli executor della pipeline.
type DeploymentSpec struct {
	Executors map[string]Executor `yaml:"executors"`
}

// Executor rappresenta un container executor.
type Executor struct {
	Container Container `yaml:"container"`
}

// Container contiene la configurazione del container.
type Container struct {
	Image     string              `yaml:"image"`
	Command   []string            `yaml:"command,omitempty"`
	Args      []string            `yaml:"args,omitempty"`
	Resources *ContainerResources `yaml:"resources,omitempty"`
	Env       []map[string]string `yaml:"env,omitempty"`
}

// ContainerResources specifica i requisiti di risorse.
type ContainerResources struct {
	CPULimit    interface{} `yaml:"cpuLimit,omitempty"`
	MemoryLimit interface{} `yaml:"memoryLimit,omitempty"`
	GPULimit    interface{} `yaml:"gpuLimit,omitempty"`
}

// Root contiene il DAG della pipeline.
type Root struct {
	DAG              DAG          `yaml:"dag"`
	InputDefinitions *Definitions `yaml:"inputDefinitions,omitempty"`
}

// DAG rappresenta il grafo delle task.
type DAG struct {
	Tasks map[string]Task `yaml:"tasks"`
}

// Task rappresenta una singola task nel DAG.
type Task struct {
	ComponentRef   ComponentRef           `yaml:"componentRef"`
	DependentTasks []string               `yaml:"dependentTasks,omitempty"`
	Inputs         *TaskInputs            `yaml:"inputs,omitempty"`
	TaskInfo       TaskInfo               `yaml:"taskInfo"`
	CachingOptions map[string]interface{} `yaml:"cachingOptions,omitempty"`
}

// ComponentRef referenzia un componente.
type ComponentRef struct {
	Name string `yaml:"name"`
}

// TaskInputs contiene gli input di una task.
type TaskInputs struct {
	Artifacts  map[string]interface{} `yaml:"artifacts,omitempty"`
	Parameters map[string]interface{} `yaml:"parameters,omitempty"`
}

// TaskInfo contiene metadata della task.
type TaskInfo struct {
	Name string `yaml:"name"`
}
