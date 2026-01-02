package kubeflow

import "time"

// ============================================================================
// STRUTTURE PIPELINE
// ============================================================================

// PipelineUploadResponse rappresenta la risposta dell'API dopo l'upload di una pipeline.
type PipelineUploadResponse struct {
	ID          string    `json:"id"`
	CreatedAt   time.Time `json:"created_at"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Error       string    `json:"error,omitempty"`
}

// PipelineListResponse per listare pipeline esistenti.
type PipelineListResponse struct {
	Pipelines     []PipelineUploadResponse `json:"pipelines"`
	TotalSize     int                      `json:"total_size"`
	NextPageToken string                   `json:"next_page_token"`
}

// ============================================================================
// STRUTTURE EXPERIMENT
// ============================================================================

// ExperimentResponse rappresenta un singolo experiment Kubeflow.
type ExperimentResponse struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Namespace   string    `json:"namespace"`
	CreatedAt   time.Time `json:"created_at"`
	Error       string    `json:"error,omitempty"`
}

// ExperimentListResponse rappresenta la lista di experiment.
type ExperimentListResponse struct {
	Experiments   []ExperimentResponse `json:"experiments"`
	TotalSize     int                  `json:"total_size"`
	NextPageToken string               `json:"next_page_token"`
}

// ============================================================================
// STRUTTURE RUN
// ============================================================================

// ResourceKey identifica una risorsa Kubeflow.
type ResourceKey struct {
	Type string `json:"type"` // es. "EXPERIMENT", "PIPELINE"
	ID   string `json:"id"`
}

// ResourceReference collega una run ad altre risorse (es. Experiment).
type ResourceReference struct {
	Key          ResourceKey `json:"key"`
	Relationship string      `json:"relationship"` // es. "OWNER"
}

// Parameter rappresenta un singolo parametro della pipeline.
type Parameter struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// PipelineSpec specifica quale pipeline eseguire.
type PipelineSpec struct {
	PipelineID        string      `json:"pipeline_id,omitempty"`
	PipelineVersionID string      `json:"pipeline_version_id,omitempty"`
	Parameters        []Parameter `json:"parameters,omitempty"`
}

// RunRequest è il body per creare una run.
type RunRequest struct {
	Name               string              `json:"name"`
	Description        string              `json:"description,omitempty"`
	PipelineSpec       PipelineSpec        `json:"pipeline_spec"`
	ResourceReferences []ResourceReference `json:"resource_references"`
}

// Run contiene i dettagli di un'esecuzione.
type Run struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
	FinishedAt  time.Time `json:"finished_at"`
	Description string    `json:"description"`
}

// RunResponse è la risposta alla creazione di una run.
type RunResponse struct {
	Run   Run    `json:"run"`
	Error string `json:"error,omitempty"`
}
