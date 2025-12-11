package pipeline

import (
	"os"
	"path/filepath"
	"testing"
)

// TestParsePipelineConRisorseCustom testa il parsing della pipeline con risorse custom
func TestParsePipelineConRisorseCustom(t *testing.T) {
	yamlPath := filepath.Join("../../testdata", "pipeline_resources.yaml")
	yamlData, err := os.ReadFile(yamlPath)
	if err != nil {
		t.Skipf("File non trovato: %v", err)
		return
	}

	parser := NewParser()
	pipeline, err := parser.ParsePipelineIR(yamlData)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Verifica struttura base
	if pipeline.PipelineInfo.Name != "pipeline-con-risorse-custom" {
		t.Errorf("Expected name 'pipeline-con-risorse-custom', got '%s'",
			pipeline.PipelineInfo.Name)
	}

	expectedDescription := "Esempio di assegnazione CPU/RAM differenziata per componente"
	if pipeline.PipelineInfo.Description != expectedDescription {
		t.Errorf("Expected description '%s', got '%s'",
			expectedDescription, pipeline.PipelineInfo.Description)
	}

	// Verifica executors
	executors := parser.GetExecutorNames(pipeline)
	expectedExecutors := []string{
		"exec-preprocess-data-op",
		"exec-train-model-op",
	}

	if len(executors) != len(expectedExecutors) {
		t.Errorf("Expected %d executors, got %d", len(expectedExecutors), len(executors))
	}

	for _, expectedExec := range expectedExecutors {
		found := false
		for _, exec := range executors {
			if exec == expectedExec {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected executor '%s' not found", expectedExec)
		}
	}

	// Verifica tasks
	tasks := parser.GetTaskNames(pipeline)
	expectedTasks := []string{
		"preprocess-data-op",
		"train-model-op",
	}

	if len(tasks) != len(expectedTasks) {
		t.Errorf("Expected %d tasks, got %d", len(expectedTasks), len(tasks))
	}

	for _, expectedTask := range expectedTasks {
		found := false
		for _, task := range tasks {
			if task == expectedTask {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected task '%s' not found", expectedTask)
		}
	}

	// Verifica dipendenze
	deps := parser.GetTaskDependencies(pipeline)
	expectedDeps := map[string][]string{
		"train-model-op": {"preprocess-data-op"},
	}

	if len(deps) != len(expectedDeps) {
		t.Errorf("Expected %d tasks with dependencies, got %d",
			len(expectedDeps), len(deps))
	}

	for taskName, expectedTaskDeps := range expectedDeps {
		actualDeps, exists := deps[taskName]
		if !exists {
			t.Errorf("Expected task '%s' to have dependencies", taskName)
			continue
		}

		if len(actualDeps) != len(expectedTaskDeps) {
			t.Errorf("Task '%s': expected %d dependencies, got %d",
				taskName, len(expectedTaskDeps), len(actualDeps))
		}

		for _, expectedDep := range expectedTaskDeps {
			found := false
			for _, actualDep := range actualDeps {
				if actualDep == expectedDep {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Task '%s': expected dependency '%s' not found",
					taskName, expectedDep)
			}
		}
	}

	// Verifica risorse (valori espliciti in float)
	totalCPU, totalMemory, totalGPU := parser.CalculateTotalResources(pipeline)

	// exec-preprocess-data-op: cpuLimit=0.5 (500m)
	// exec-train-model-op: cpuLimit=2.0 (2000m)
	// Totale: 2500m
	expectedCPU := int64(2500)

	// exec-preprocess-data-op: memoryLimit=0.536870912 GB = 536870912 bytes
	// exec-train-model-op: memoryLimit=4.294967296 GB = 4294967296 bytes
	// Totale: 4831838208 bytes (4.83 GB)
	expectedMemory := int64(4831838208)

	expectedGPU := 0

	if totalCPU != expectedCPU {
		t.Errorf("Expected total CPU %d mCores, got %d", expectedCPU, totalCPU)
	}

	if totalMemory != expectedMemory {
		t.Errorf("Expected total Memory %d bytes, got %d", expectedMemory, totalMemory)
	}

	if totalGPU != expectedGPU {
		t.Errorf("Expected total GPU %d, got %d", expectedGPU, totalGPU)
	}

	// Verifica risorse individuali per ogni executor
	preprocessExec := pipeline.DeploymentSpec.Executors["exec-preprocess-data-op"]
	if preprocessExec.Container.Resources == nil {
		t.Fatal("Expected preprocess executor to have resources")
	}

	trainExec := pipeline.DeploymentSpec.Executors["exec-train-model-op"]
	if trainExec.Container.Resources == nil {
		t.Fatal("Expected train executor to have resources")
	}

	// Verifica CPU preprocess (0.5 = 500m)
	preprocessCPU, preprocessMem, preprocessGPU := parser.parseExecutorResources(preprocessExec)
	if preprocessCPU != 500 {
		t.Errorf("Expected preprocess CPU 500 mCores, got %d", preprocessCPU)
	}
	if preprocessMem != 536870912 {
		t.Errorf("Expected preprocess Memory 536870912 bytes, got %d", preprocessMem)
	}
	if preprocessGPU != 0 {
		t.Errorf("Expected preprocess GPU 0, got %d", preprocessGPU)
	}

	// Verifica CPU train (2.0 = 2000m)
	trainCPU, trainMem, trainGPU := parser.parseExecutorResources(trainExec)
	if trainCPU != 2000 {
		t.Errorf("Expected train CPU 2000 mCores, got %d", trainCPU)
	}
	if trainMem != 4294967296 {
		t.Errorf("Expected train Memory 4294967296 bytes, got %d", trainMem)
	}
	if trainGPU != 0 {
		t.Errorf("Expected train GPU 0, got %d", trainGPU)
	}

	// Log dei risultati
	t.Logf("✓ Pipeline con risorse custom parsed successfully")
	t.Logf("  Name: %s", pipeline.PipelineInfo.Name)
	t.Logf("  Description: %s", pipeline.PipelineInfo.Description)
	t.Logf("  Executors: %d", len(executors))
	t.Logf("  Tasks: %d", len(tasks))
	t.Logf("  Dependencies: %v", deps)
	t.Logf("  ")
	t.Logf("  Preprocess Executor:")
	t.Logf("    CPU: %d mCores (%.2f cores)", preprocessCPU, float64(preprocessCPU)/1000)
	t.Logf("    Memory: %d bytes (%.2f GB)", preprocessMem, float64(preprocessMem)/1_000_000_000)
	t.Logf("    GPU: %d", preprocessGPU)
	t.Logf("  ")
	t.Logf("  Train Executor:")
	t.Logf("    CPU: %d mCores (%.2f cores)", trainCPU, float64(trainCPU)/1000)
	t.Logf("    Memory: %d bytes (%.2f GB)", trainMem, float64(trainMem)/1_000_000_000)
	t.Logf("    GPU: %d", trainGPU)
	t.Logf("  ")
	t.Logf("  Total Resources:")
	t.Logf("    CPU: %d mCores (%.2f cores)", totalCPU, float64(totalCPU)/1000)
	t.Logf("    Memory: %d bytes (%.2f GB)", totalMemory, float64(totalMemory)/1_000_000_000)
	t.Logf("    GPU: %d", totalGPU)
}

// TestParseDocumentProcessingPipeline testa il parsing della pipeline reale
func TestParseDocumentProcessingPipeline(t *testing.T) {
	yamlPath := filepath.Join("../../testdata", "document-processing-pipeline.yaml")
	yamlData, err := os.ReadFile(yamlPath)
	if err != nil {
		t.Skipf("File non trovato: %v", err)
		return
	}

	parser := NewParser()
	pipeline, err := parser.ParsePipelineIR(yamlData)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Verifica struttura base
	if pipeline.PipelineInfo.Name != "document-processing-pipeline" {
		t.Errorf("Expected name 'document-processing-pipeline', got '%s'",
			pipeline.PipelineInfo.Name)
	}

	// Verifica executors
	executors := parser.GetExecutorNames(pipeline)
	expectedExecutors := []string{
		"exec-chunk-documents",
		"exec-create-embeddings",
		"exec-download-from-minio",
		"exec-upload-to-qdrant",
	}

	if len(executors) != len(expectedExecutors) {
		t.Errorf("Expected %d executors, got %d", len(expectedExecutors), len(executors))
	}

	for _, expectedExec := range expectedExecutors {
		found := false
		for _, exec := range executors {
			if exec == expectedExec {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected executor '%s' not found", expectedExec)
		}
	}

	// Verifica tasks
	tasks := parser.GetTaskNames(pipeline)
	if len(tasks) != 4 {
		t.Errorf("Expected 4 tasks, got %d", len(tasks))
	}

	// Verifica dipendenze
	deps := parser.GetTaskDependencies(pipeline)
	expectedDeps := map[string][]string{
		"chunk-documents":   {"download-from-minio"},
		"create-embeddings": {"chunk-documents"},
		"upload-to-qdrant":  {"create-embeddings"},
	}

	if len(deps) != len(expectedDeps) {
		t.Errorf("Expected %d tasks with dependencies, got %d",
			len(expectedDeps), len(deps))
	}

	for taskName, expectedTaskDeps := range expectedDeps {
		actualDeps, exists := deps[taskName]
		if !exists {
			t.Errorf("Expected task '%s' to have dependencies", taskName)
			continue
		}

		if len(actualDeps) != len(expectedTaskDeps) {
			t.Errorf("Task '%s': expected %d dependencies, got %d",
				taskName, len(expectedTaskDeps), len(actualDeps))
		}

		for _, expectedDep := range expectedTaskDeps {
			found := false
			for _, actualDep := range actualDeps {
				if actualDep == expectedDep {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Task '%s': expected dependency '%s' not found",
					taskName, expectedDep)
			}
		}
	}

	// Calcola risorse (tutti gli executor non hanno resources, usano default)
	totalCPU, totalMemory, totalGPU := parser.CalculateTotalResources(pipeline)

	// 4 executors × 100m = 400m CPU
	expectedCPU := int64(400)
	// 4 executors × 128Mi = 536870912 bytes
	expectedMemory := int64(536870912)

	if totalCPU != expectedCPU {
		t.Errorf("Expected total CPU %d mCores, got %d", expectedCPU, totalCPU)
	}

	if totalMemory != expectedMemory {
		t.Errorf("Expected total Memory %d bytes, got %d", expectedMemory, totalMemory)
	}

	if totalGPU != 0 {
		t.Errorf("Expected total GPU 0, got %d", totalGPU)
	}

	t.Logf("✓ Document processing pipeline parsed successfully")
	t.Logf("  Name: %s", pipeline.PipelineInfo.Name)
	t.Logf("  Executors: %d", len(executors))
	t.Logf("  Tasks: %d", len(tasks))
	t.Logf("  Tasks with dependencies: %d", len(deps))
	t.Logf("  Total CPU: %d mCores", totalCPU)
	t.Logf("  Total Memory: %d bytes (%.2f GB)",
		totalMemory, float64(totalMemory)/1_000_000_000)
	t.Logf("  Total GPU: %d", totalGPU)
}

// TestParsePipelineWithResources testa il parsing con risorse esplicite
func TestParsePipelineWithResources(t *testing.T) {
	yamlData := []byte(`
schemaVersion: 2.1.0
sdkVersion: kfp-2.5.0
pipelineInfo:
  name: test-pipeline-with-resources
deploymentSpec:
  executors:
    exec-preprocess:
      container:
        image: python:3.10
        resources:
          cpuLimit: 0.5
          memoryLimit: 0.536870912
    exec-train:
      container:
        image: python:3.10
        resources:
          cpuLimit: 2.0
          memoryLimit: 4.294967296
          gpuLimit: 1
root:
  dag:
    tasks:
      preprocess:
        componentRef:
          name: comp-preprocess
        taskInfo:
          name: preprocess
      train:
        componentRef:
          name: comp-train
        dependentTasks:
          - preprocess
        taskInfo:
          name: train
`)

	parser := NewParser()
	pipeline, err := parser.ParsePipelineIR(yamlData)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if pipeline.PipelineInfo.Name != "test-pipeline-with-resources" {
		t.Errorf("Expected name 'test-pipeline-with-resources', got '%s'",
			pipeline.PipelineInfo.Name)
	}

	// Verifica risorse
	totalCPU, totalMemory, totalGPU := parser.CalculateTotalResources(pipeline)

	// 500m + 2000m = 2500m
	expectedCPU := int64(2500)
	// 536870912 + 4294967296 = 4831838208 bytes
	expectedMemory := int64(4831838208)
	expectedGPU := 1

	if totalCPU != expectedCPU {
		t.Errorf("Expected total CPU %d mCores, got %d", expectedCPU, totalCPU)
	}

	if totalMemory != expectedMemory {
		t.Errorf("Expected total Memory %d bytes, got %d", expectedMemory, totalMemory)
	}

	if totalGPU != expectedGPU {
		t.Errorf("Expected total GPU %d, got %d", expectedGPU, totalGPU)
	}

	// Verifica dipendenze
	deps := parser.GetTaskDependencies(pipeline)
	if len(deps) != 1 {
		t.Errorf("Expected 1 task with dependencies, got %d", len(deps))
	}

	trainDeps, exists := deps["train"]
	if !exists {
		t.Fatalf("Expected task 'train' to have dependencies")
	}

	if len(trainDeps) != 1 || trainDeps[0] != "preprocess" {
		t.Errorf("Expected 'train' to depend on 'preprocess', got %v", trainDeps)
	}

	t.Logf("✓ Pipeline with resources parsed successfully")
	t.Logf("  Total CPU: %d mCores", totalCPU)
	t.Logf("  Total Memory: %d bytes (%.2f GB)",
		totalMemory, float64(totalMemory)/1_000_000_000)
	t.Logf("  Total GPU: %d", totalGPU)
}

// TestParsePipelineWithDefaults testa il parsing con risorse di default
func TestParsePipelineWithDefaults(t *testing.T) {
	yamlData := []byte(`
schemaVersion: 2.1.0
pipelineInfo:
  name: test-defaults
deploymentSpec:
  executors:
    exec-task1:
      container:
        image: python:3.10
    exec-task2:
      container:
        image: python:3.10
root:
  dag:
    tasks:
      task1:
        componentRef:
          name: comp-task1
        taskInfo:
          name: task1
      task2:
        componentRef:
          name: comp-task2
        taskInfo:
          name: task2
`)

	parser := NewParser()
	pipeline, err := parser.ParsePipelineIR(yamlData)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	totalCPU, totalMemory, totalGPU := parser.CalculateTotalResources(pipeline)

	// 2 executors × 100m = 200m
	expectedCPU := int64(200)
	// 2 executors × 128Mi = 268435456 bytes
	expectedMemory := int64(268435456)

	if totalCPU != expectedCPU {
		t.Errorf("Expected default CPU %d mCores, got %d", expectedCPU, totalCPU)
	}

	if totalMemory != expectedMemory {
		t.Errorf("Expected default Memory %d bytes, got %d", expectedMemory, totalMemory)
	}

	if totalGPU != 0 {
		t.Errorf("Expected default GPU 0, got %d", totalGPU)
	}

	t.Logf("✓ Default resources: CPU=%d mCores, Memory=%d bytes, GPU=%d",
		totalCPU, totalMemory, totalGPU)
}

// TestParseStringResources testa il parsing con risorse in formato string
func TestParseStringResources(t *testing.T) {
	yamlData := []byte(`
schemaVersion: 2.1.0
pipelineInfo:
  name: test-string-resources
deploymentSpec:
  executors:
    exec-task1:
      container:
        image: python:3.10
        resources:
          cpuLimit: "500m"
          memoryLimit: "1Gi"
root:
  dag:
    tasks:
      task1:
        componentRef:
          name: comp-task1
        taskInfo:
          name: task1
`)

	parser := NewParser()
	pipeline, err := parser.ParsePipelineIR(yamlData)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	totalCPU, totalMemory, totalGPU := parser.CalculateTotalResources(pipeline)

	expectedCPU := int64(500)           // 500m
	expectedMemory := int64(1073741824) // 1Gi

	if totalCPU != expectedCPU {
		t.Errorf("Expected CPU %d mCores, got %d", expectedCPU, totalCPU)
	}

	if totalMemory != expectedMemory {
		t.Errorf("Expected Memory %d bytes, got %d", expectedMemory, totalMemory)
	}

	t.Logf("✓ String resources: CPU=%d mCores, Memory=%d bytes, GPU=%d",
		totalCPU, totalMemory, totalGPU)
}

// TestParseInvalidSchemaVersion testa il rigetto di versioni schema non supportate
func TestParseInvalidSchemaVersion(t *testing.T) {
	yamlData := []byte(`
schemaVersion: 1.0.0
pipelineInfo:
  name: invalid
`)

	parser := NewParser()
	_, err := parser.ParsePipelineIR(yamlData)
	if err == nil {
		t.Fatal("Expected error for invalid schema version")
	}

	if err.Error() != "unsupported schema version: 1.0.0 (expected 2.1.0)" {
		t.Errorf("Unexpected error message: %v", err)
	}

	t.Logf("✓ Correctly rejected invalid schema version: %v", err)
}

// TestParseInvalidYAML testa il rigetto di YAML malformato
func TestParseInvalidYAML(t *testing.T) {
	yamlData := []byte(`invalid: yaml: syntax`)

	parser := NewParser()
	_, err := parser.ParsePipelineIR(yamlData)
	if err == nil {
		t.Fatal("Expected error for invalid YAML")
	}

	t.Logf("✓ Correctly rejected invalid YAML: %v", err)
}

// TestNilPipelineHandling testa la gestione di pipeline nil
func TestNilPipelineHandling(t *testing.T) {
	parser := NewParser()

	totalCPU, totalMemory, totalGPU := parser.CalculateTotalResources(nil)
	if totalCPU != 0 || totalMemory != 0 || totalGPU != 0 {
		t.Errorf("Expected zero resources for nil pipeline, got CPU=%d, Mem=%d, GPU=%d",
			totalCPU, totalMemory, totalGPU)
	}

	deps := parser.GetTaskDependencies(nil)
	if len(deps) != 0 {
		t.Errorf("Expected empty dependencies for nil pipeline, got %d", len(deps))
	}

	executors := parser.GetExecutorNames(nil)
	if len(executors) != 0 {
		t.Errorf("Expected no executors for nil pipeline, got %d", len(executors))
	}

	tasks := parser.GetTaskNames(nil)
	if len(tasks) != 0 {
		t.Errorf("Expected no tasks for nil pipeline, got %d", len(tasks))
	}

	t.Logf("✓ Nil handling works correctly")
}

// TestComplexDependencyChain testa catene di dipendenze complesse
func TestComplexDependencyChain(t *testing.T) {
	yamlData := []byte(`
schemaVersion: 2.1.0
pipelineInfo:
  name: test-complex-deps
root:
  dag:
    tasks:
      task-a:
        componentRef:
          name: comp-a
        taskInfo:
          name: task-a
      task-b:
        componentRef:
          name: comp-b
        dependentTasks:
          - task-a
        taskInfo:
          name: task-b
      task-c:
        componentRef:
          name: comp-c
        dependentTasks:
          - task-a
        taskInfo:
          name: task-c
      task-d:
        componentRef:
          name: comp-d
        dependentTasks:
          - task-b
          - task-c
        taskInfo:
          name: task-d
`)

	parser := NewParser()
	pipeline, err := parser.ParsePipelineIR(yamlData)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	deps := parser.GetTaskDependencies(pipeline)

	// Verifica struttura dipendenze
	expectedDepsCount := 3 // task-b, task-c, task-d hanno dipendenze
	if len(deps) != expectedDepsCount {
		t.Errorf("Expected %d tasks with dependencies, got %d",
			expectedDepsCount, len(deps))
	}

	// Verifica task-d ha 2 dipendenze
	taskDDeps, exists := deps["task-d"]
	if !exists {
		t.Fatal("Expected task-d to have dependencies")
	}

	if len(taskDDeps) != 2 {
		t.Errorf("Expected task-d to have 2 dependencies, got %d", len(taskDDeps))
	}

	t.Logf("✓ Complex dependencies: %v", deps)
}
