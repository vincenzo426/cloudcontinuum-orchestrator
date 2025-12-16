package kubeflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"time"
)

// ============================================================================
// STRUTTURE DATI - CLIENT
// ============================================================================

// Client gestisce le chiamate HTTP alle API di Kubeflow Pipelines.
// Ogni Client è configurato per comunicare con un singolo cluster Kubeflow.
type Client struct {
	BaseURL    string       // URL base del cluster Kubeflow (es. http://cluster.svc.clusterset.local.:8888)
	Namespace  string       // Namespace Kubernetes target per le risorse
	Token      string       // Token di autenticazione (non usato in standalone mode)
	HTTPClient *http.Client // Client HTTP configurato con timeout
}

// ============================================================================
// COSTRUTTORE
// ============================================================================

// NewClient crea un nuovo client Kubeflow configurato.
// Parametri:
//   - baseURL: endpoint del cluster Kubeflow (formato Submariner ClusterSet DNS)
//   - namespace: namespace Kubernetes dove creare le risorse
//   - token: token di autenticazione (usa "" per standalone mode)
//
// Ritorna un Client pronto per comunicare con le API Kubeflow.
func NewClient(baseURL, namespace, token string) *Client {
	return &Client{
		BaseURL:   baseURL,
		Namespace: namespace,
		Token:     token,
		HTTPClient: &http.Client{
			Timeout: 60 * time.Second, // Timeout di 60 secondi per le richieste HTTP
		},
	}
}

// ============================================================================
// AUTENTICAZIONE
// ============================================================================

// addAuth aggiunge gli header di autenticazione alle richieste HTTP.
// Nota: attualmente non utilizzato perché Kubeflow è in standalone mode.
// In modalità multi-user, questa funzione aggiungerebbe:
//   - Authorization: Bearer token
//   - kubeflow-userid: identificativo utente
func (c *Client) addAuth(req *http.Request) {
	// Autenticazione disabilitata per standalone mode
	// Se necessario in futuro, decommentare e configurare token + userid
}

// ============================================================================
// STRUTTURE DATI - PIPELINE
// ============================================================================

// PipelineUploadResponse rappresenta la risposta dell'API dopo l'upload di una pipeline.
type PipelineUploadResponse struct {
	ID          string    `json:"id"`          // ID univoco della pipeline creata
	CreatedAt   time.Time `json:"created_at"`  // Timestamp di creazione
	Name        string    `json:"name"`        // Nome della pipeline
	Description string    `json:"description"` // Descrizione della pipeline
	Error       string    `json:"error"`       // Messaggio di errore (se presente)
}

// ============================================================================
// UPLOAD PIPELINE
// ============================================================================

// UploadPipeline carica un file YAML di pipeline Kubeflow al cluster target.
// Utilizza multipart/form-data per inviare il file YAML come upload.
//
// Parametri:
//   - name: nome da assegnare alla pipeline
//   - pipelineYAML: contenuto del file YAML della pipeline (Kubeflow IR v2.1.0)
//
// Ritorna:
//   - string: ID univoco della pipeline caricata
//   - error: errore in caso di fallimento
//
// Endpoint API: POST /apis/v1beta1/pipelines/upload
func (c *Client) UploadPipeline(name string, pipelineYAML []byte) (string, error) {
	// Prepara il body multipart
	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)

	// Crea il campo "uploadfile" con il contenuto YAML
	part, err := writer.CreateFormFile("uploadfile", name+".yaml")
	if err != nil {
		return "", fmt.Errorf("failed to create form file: %w", err)
	}

	if _, err := part.Write(pipelineYAML); err != nil {
		return "", fmt.Errorf("failed to write pipeline yaml: %w", err)
	}

	// Aggiungi il campo "name" al form
	if err := writer.WriteField("name", name); err != nil {
		return "", fmt.Errorf("failed to write name field: %w", err)
	}

	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("failed to close writer: %w", err)
	}

	// Crea la richiesta HTTP POST
	url := fmt.Sprintf("%s/apis/v1beta1/pipelines/upload", c.BaseURL)
	req, err := http.NewRequest("POST", url, &requestBody)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	// Imposta gli header necessari
	req.Header.Set("Content-Type", writer.FormDataContentType())
	c.addAuth(req)

	// Esegui la richiesta
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to upload pipeline: %w", err)
	}
	defer resp.Body.Close()

	// Leggi la risposta
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parsa la risposta JSON
	var uploadResp PipelineUploadResponse
	if err := json.Unmarshal(body, &uploadResp); err != nil {
		return "", fmt.Errorf("failed to parse upload response: %w", err)
	}

	if uploadResp.Error != "" {
		return "", fmt.Errorf("upload error: %s", uploadResp.Error)
	}

	return uploadResp.ID, nil
}

// ============================================================================
// STRUTTURE DATI - EXPERIMENT
// ============================================================================

// ExperimentResponse rappresenta un singolo experiment Kubeflow.
type ExperimentResponse struct {
	ID          string    `json:"id"`          // ID univoco dell'experiment
	Name        string    `json:"name"`        // Nome dell'experiment
	Description string    `json:"description"` // Descrizione dell'experiment
	Namespace   string    `json:"namespace"`   // Namespace dove risiede l'experiment
	CreatedAt   time.Time `json:"created_at"`  // Timestamp di creazione
}

// ExperimentListResponse rappresenta la lista di experiment restituita dall'API.
type ExperimentListResponse struct {
	Experiments   []ExperimentResponse `json:"experiments"`     // Array di experiment
	TotalSize     int                  `json:"total_size"`      // Numero totale di experiment
	NextPageToken string               `json:"next_page_token"` // Token per paginazione
}

// ============================================================================
// GESTIONE EXPERIMENT
// ============================================================================

// GetOrCreateExperiment cerca un experiment esistente per nome, o ne crea uno nuovo.
// Questa funzione implementa il pattern "get-or-create" per evitare duplicati.
//
// Parametri:
//   - experimentName: nome dell'experiment da cercare o creare
//
// Ritorna:
//   - string: ID dell'experiment (esistente o appena creato)
//   - error: errore in caso di fallimento
//
// Flusso:
//  1. Cerca experiment esistente con lo stesso nome
//  2. Se trovato, ritorna il suo ID
//  3. Se non trovato, crea un nuovo experiment
func (c *Client) GetOrCreateExperiment(experimentName string) (string, error) {
	// Prova a trovare l'experiment esistente
	experimentID, err := c.FindExperimentByName(experimentName)
	if err == nil && experimentID != "" {
		return experimentID, nil
	}

	// Se non esiste, creane uno nuovo
	return c.CreateExperiment(experimentName, fmt.Sprintf("Default experiment for %s", experimentName))
}

// FindExperimentByName cerca un experiment per nome esatto nel namespace corrente.
//
// Parametri:
//   - name: nome esatto dell'experiment da cercare
//
// Ritorna:
//   - string: ID dell'experiment se trovato
//   - error: errore se non trovato o in caso di fallimento
//
// Endpoint API: GET /apis/v1beta1/experiments?resource_reference_key.type=NAMESPACE&resource_reference_key.id={namespace}
func (c *Client) FindExperimentByName(name string) (string, error) {
	// Costruisci URL con filtro per namespace
	url := fmt.Sprintf("%s/apis/v1beta1/experiments", c.BaseURL)

	if c.Namespace != "" {
		url = fmt.Sprintf("%s?resource_reference_key.type=NAMESPACE&resource_reference_key.id=%s",
			url, c.Namespace)
	}

	// Crea la richiesta HTTP GET
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	c.addAuth(req)

	// Esegui la richiesta
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to list experiments: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("list experiments failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parsa la risposta JSON
	var listResp ExperimentListResponse
	if err := json.Unmarshal(body, &listResp); err != nil {
		return "", fmt.Errorf("failed to parse experiment list response: %w", err)
	}

	// Cerca l'experiment con nome esatto
	for _, exp := range listResp.Experiments {
		if exp.Name == name {
			return exp.ID, nil
		}
	}

	return "", fmt.Errorf("experiment %s not found", name)
}

// CreateExperiment crea un nuovo experiment nel namespace corrente.
//
// Parametri:
//   - name: nome del nuovo experiment
//   - description: descrizione del nuovo experiment
//
// Ritorna:
//   - string: ID dell'experiment creato
//   - error: errore in caso di fallimento
//
// Endpoint API: POST /apis/v1beta1/experiments
//
// Nota: L'experiment viene automaticamente associato al namespace tramite resource_references.
func (c *Client) CreateExperiment(name, description string) (string, error) {

	fmt.Printf("\n")
	fmt.Printf("═══════════════════════════════════════════════════════\n")
	fmt.Printf("🔍 CreateExperiment DEBUG\n")
	fmt.Printf("───────────────────────────────────────────────────────\n")
	fmt.Printf("  Client.BaseURL:   %s\n", c.BaseURL)
	fmt.Printf("  Client.Namespace: '%s'\n", c.Namespace)
	fmt.Printf("  Experiment Name:  '%s'\n", name)
	fmt.Printf("═══════════════════════════════════════════════════════\n")
	fmt.Printf("\n")

	// Costruisci il body della richiesta con resource reference al namespace
	reqBody := map[string]interface{}{
		"name":        name,
		"description": description,
		"resource_references": []map[string]interface{}{
			{
				"key": map[string]string{
					"type": "NAMESPACE",
					"id":   c.Namespace,
				},
				"relationship": "OWNER",
			},
		},
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	// Log per debug
	fmt.Printf("DEBUG CreateExperiment - Namespace: %s, Body: %s\n", c.Namespace, string(jsonData))

	// Crea la richiesta HTTP POST
	url := fmt.Sprintf("%s/apis/v1beta1/experiments", c.BaseURL)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	c.addAuth(req)

	// Log per debug
	fmt.Printf("DEBUG CreateExperiment - URL: %s, Headers: %v\n", url, req.Header)

	// Esegui la richiesta
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to create experiment: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	// Log per debug
	fmt.Printf("DEBUG CreateExperiment - Status: %d, Response: %s\n", resp.StatusCode, string(body))

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("create experiment failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parsa la risposta JSON
	var expResp ExperimentResponse
	if err := json.Unmarshal(body, &expResp); err != nil {
		return "", fmt.Errorf("failed to parse experiment response: %w", err)
	}

	return expResp.ID, nil
}

// ============================================================================
// STRUTTURE DATI - RUN
// ============================================================================

// ResourceKey identifica una risorsa Kubeflow (experiment, pipeline, etc.).
type ResourceKey struct {
	Type string `json:"type"` // Tipo di risorsa (es. "EXPERIMENT", "PIPELINE")
	ID   string `json:"id"`   // ID univoco della risorsa
}

// ResourceReference collega una run ad altre risorse Kubeflow.
// Utilizzato per associare una run a un experiment (relazione OWNER).
type ResourceReference struct {
	Key          ResourceKey `json:"key"`          // Chiave della risorsa referenziata
	Relationship string      `json:"relationship"` // Tipo di relazione (es. "OWNER")
}

// Parameter rappresenta un singolo parametro della pipeline
type Parameter struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// PipelineSpec specifica quale pipeline eseguire e con quali parametri.
type PipelineSpec struct {
	PipelineID string      `json:"pipeline_id,omitempty"`
	Parameters []Parameter `json:"parameters,omitempty"` // ARRAY invece di map
}

// RunRequest rappresenta il body della richiesta per creare una run.
type RunRequest struct {
	Name               string              `json:"name"`                  // Nome della run
	Description        string              `json:"description,omitempty"` // Descrizione della run
	PipelineSpec       PipelineSpec        `json:"pipeline_spec"`         // Specifica della pipeline
	ResourceReferences []ResourceReference `json:"resource_references"`   // Collegamenti ad altre risorse
}

// Run contiene i dettagli di un'esecuzione pipeline.
type Run struct {
	ID          string    `json:"id"`          // ID univoco della run
	Name        string    `json:"name"`        // Nome della run
	Status      string    `json:"status"`      // Stato attuale (es. "Running", "Succeeded", "Failed")
	CreatedAt   time.Time `json:"created_at"`  // Timestamp di creazione
	FinishedAt  time.Time `json:"finished_at"` // Timestamp di completamento
	Description string    `json:"description"` // Descrizione della run
}

// RunResponse rappresenta la risposta dell'API dopo la creazione di una run.
type RunResponse struct {
	Run   Run    `json:"run"`   // Dettagli della run creata
	Error string `json:"error"` // Messaggio di errore (se presente)
}

// ============================================================================
// CREAZIONE RUN
// ============================================================================

// CreateRun crea ed esegue una nuova pipeline run.
//
// Parametri:
//   - pipelineID: ID della pipeline da eseguire
//   - runName: nome da assegnare alla run
//   - experimentID: ID dell'experiment a cui associare la run
//   - parameters: mappa di parametri runtime da passare alla pipeline
//
// Ritorna:
//   - runID: ID univoco della run creata
//   - runURL: URL per accedere alla run nella UI Kubeflow
//   - error: errore in caso di fallimento
//
// Endpoint API: POST /apis/v1beta1/runs
//
// Nota: La run viene automaticamente associata all'experiment tramite resource_references.
func (c *Client) CreateRun(pipelineID, runName, experimentID string, parameters map[string]interface{}) (string, string, error) {

	fmt.Printf("\n")
	fmt.Printf("═══════════════════════════════════════════════════════\n")
	fmt.Printf("🔍 CreateRun DEBUG\n")
	fmt.Printf("───────────────────────────────────────────────────────\n")
	fmt.Printf("  Client.Namespace: '%s'\n", c.Namespace)
	fmt.Printf("  Pipeline ID:      %s\n", pipelineID)
	fmt.Printf("  Run Name:         %s\n", runName)
	fmt.Printf("  Experiment ID:    %s\n", experimentID)
	fmt.Printf("═══════════════════════════════════════════════════════\n")
	fmt.Printf("\n")

	// Conversione map[string]interface{} in []Parameter
	var params []Parameter
	for name, value := range parameters {
		params = append(params, Parameter{
			Name:  name,
			Value: fmt.Sprintf("%v", value), // Converti a stringa
		})
	}

	// Costruisci il body della richiesta
	runReq := RunRequest{
		Name:        runName,
		Description: "Run created by Cloud Continuum Orchestrator",
		PipelineSpec: PipelineSpec{
			PipelineID: pipelineID,
			Parameters: params,
		},
		ResourceReferences: []ResourceReference{
			{
				Key: ResourceKey{
					Type: "EXPERIMENT",
					ID:   experimentID,
				},
				Relationship: "OWNER", // La run appartiene a questo experiment
			},
		},
	}

	jsonData, err := json.Marshal(runReq)
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal run request: %w", err)
	}

	// Crea la richiesta HTTP POST
	url := fmt.Sprintf("%s/apis/v1beta1/runs", c.BaseURL)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	c.addAuth(req)

	// Esegui la richiesta
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("failed to create run: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", "", fmt.Errorf("run creation failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parsa la risposta JSON
	var runResp RunResponse
	if err := json.Unmarshal(body, &runResp); err != nil {
		return "", "", fmt.Errorf("failed to parse run response: %w", err)
	}

	if runResp.Error != "" {
		return "", "", fmt.Errorf("run creation error: %s", runResp.Error)
	}

	// Costruisci l'URL per accedere alla run nella UI Kubeflow
	runURL := fmt.Sprintf("%s/_/pipeline/#/runs/details/%s", c.BaseURL, runResp.Run.ID)

	return runResp.Run.ID, runURL, nil
}
