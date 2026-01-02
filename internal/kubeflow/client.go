package kubeflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"
)

// Costanti per gli endpoint API
const (
	apiBase        = "/apis/v1beta1"
	endpointUpload = apiBase + "/pipelines/upload"
	endpointPipes  = apiBase + "/pipelines"
	endpointExps   = apiBase + "/experiments"
	endpointRuns   = apiBase + "/runs"
)

// Client gestisce le chiamate HTTP alle API di Kubeflow Pipelines.
type Client struct {
	BaseURL    string
	Namespace  string
	Token      string
	HTTPClient *http.Client
}

// NewClient crea un nuovo client configurato.
func NewClient(baseURL, namespace, token string) *Client {
	return &Client{
		BaseURL:   baseURL,
		Namespace: namespace,
		Token:     token,
		HTTPClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

// addAuth aggiunge gli header di autenticazione (placeholder per implementazioni future).
func (c *Client) addAuth(req *http.Request) {
	// In modalità multi-user, qui andrebbe aggiunto il Bearer token
}

// doRequest gestisce la logica comune delle chiamate HTTP JSON.
func (c *Client) doRequest(method, endpoint string, bodyData interface{}, result interface{}) error {
	var bodyReader io.Reader

	// Gestione del body: se presente, lo serializza in JSON
	if bodyData != nil {
		jsonData, err := json.Marshal(bodyData)
		if err != nil {
			return fmt.Errorf("failed to marshal request body: %w", err)
		}
		bodyReader = bytes.NewBuffer(jsonData)
	}

	url := c.BaseURL + endpoint
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	if bodyData != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	c.addAuth(req)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	// Controllo generico dello status code
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("api error (status %d): %s", resp.StatusCode, string(respBody))
	}

	// Se ci aspettiamo una risposta, deserializziamo
	if result != nil {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}
	}

	return nil
}

// ============================================================================
// GESTIONE PIPELINE
// ============================================================================

// UploadPipeline carica un file YAML via multipart/form-data.
// Nota: Non usa doRequest perché richiede Content-Type multipart.
func (c *Client) UploadPipeline(name string, pipelineYAML []byte) (string, error) {
	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)

	// Crea part per il file
	part, err := writer.CreateFormFile("uploadfile", name+".yaml")
	if err != nil {
		return "", fmt.Errorf("create form file error: %w", err)
	}
	if _, err := part.Write(pipelineYAML); err != nil {
		return "", fmt.Errorf("write yaml error: %w", err)
	}

	// Aggiungi campo nome
	if err := writer.WriteField("name", name); err != nil {
		return "", fmt.Errorf("write name field error: %w", err)
	}
	if err := writer.Close(); err != nil {
		return "", err
	}

	url := c.BaseURL + endpointUpload
	req, err := http.NewRequest("POST", url, &requestBody)
	if err != nil {
		return "", err
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())
	c.addAuth(req)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("upload request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("upload failed status %d: %s", resp.StatusCode, string(body))
	}

	var uploadResp PipelineUploadResponse
	if err := json.Unmarshal(body, &uploadResp); err != nil {
		return "", fmt.Errorf("parse error: %w", err)
	}

	return uploadResp.ID, nil
}

// GetPipelineByName cerca una pipeline per nome.
func (c *Client) GetPipelineByName(name string) (string, error) {
	var listResp PipelineListResponse
	if err := c.doRequest("GET", endpointPipes, nil, &listResp); err != nil {
		return "", err
	}

	for _, pipeline := range listResp.Pipelines {
		if pipeline.Name == name || pipeline.Name == name+".yaml" {
			return pipeline.ID, nil
		}
	}
	return "", fmt.Errorf("pipeline %s not found", name)
}

// UploadOrVersionPipeline gestisce la logica di creazione o versionamento.
func (c *Client) UploadOrVersionPipeline(pipelineName string, pipelineYAML []byte) (pipelineID, versionID string, err error) {
	existingID, err := c.GetPipelineByName(pipelineName)

	if err != nil {
		// Pipeline non trovata -> Creazione nuova
		fmt.Printf("Creating new pipeline: %s\n", pipelineName)
		pid, err := c.UploadPipeline(pipelineName, pipelineYAML)
		if err != nil {
			return "", "", err
		}
		return pid, "", nil
	}

	// Pipeline trovata -> Creazione nuova versione (nome con timestamp)
	fmt.Printf("Pipeline found (%s). Creating versioned pipeline.\n", existingID)

	baseName := strings.TrimSuffix(pipelineName, ".yaml")
	versionedName := fmt.Sprintf("%s-v%s.yaml", baseName, time.Now().Format("20060102-150405"))

	newID, err := c.UploadPipeline(versionedName, pipelineYAML)
	if err != nil {
		return "", "", fmt.Errorf("failed to upload versioned pipeline: %w", err)
	}

	return newID, newID, nil
}

// ============================================================================
// GESTIONE EXPERIMENT
// ============================================================================

// GetOrCreateExperiment cerca o crea un experiment.
func (c *Client) GetOrCreateExperiment(experimentName string) (string, error) {
	id, err := c.FindExperimentByName(experimentName)
	if err == nil && id != "" {
		return id, nil
	}
	return c.CreateExperiment(experimentName, fmt.Sprintf("Default experiment for %s", experimentName))
}

// FindExperimentByName cerca un experiment nel namespace configurato.
func (c *Client) FindExperimentByName(name string) (string, error) {
	endpoint := endpointExps
	if c.Namespace != "" {
		endpoint = fmt.Sprintf("%s?resource_reference_key.type=NAMESPACE&resource_reference_key.id=%s", endpointExps, c.Namespace)
	}

	var listResp ExperimentListResponse
	if err := c.doRequest("GET", endpoint, nil, &listResp); err != nil {
		return "", err
	}

	for _, exp := range listResp.Experiments {
		if exp.Name == name {
			return exp.ID, nil
		}
	}
	return "", fmt.Errorf("experiment %s not found", name)
}

// CreateExperiment crea un nuovo experiment.
func (c *Client) CreateExperiment(name, description string) (string, error) {
	reqBody := map[string]interface{}{
		"name":        name,
		"description": description,
		"resource_references": []map[string]interface{}{
			{
				"key":          map[string]string{"type": "NAMESPACE", "id": c.Namespace},
				"relationship": "OWNER",
			},
		},
	}

	var expResp ExperimentResponse
	if err := c.doRequest("POST", endpointExps, reqBody, &expResp); err != nil {
		return "", err
	}
	return expResp.ID, nil
}

// ============================================================================
// GESTIONE RUN
// ============================================================================

// CreateRun crea ed esegue una pipeline run.
func (c *Client) CreateRun(pipelineID, versionID, runName, experimentID string, parameters map[string]interface{}) (string, string, error) {
	// Conversione parametri
	var params []Parameter
	for name, value := range parameters {
		params = append(params, Parameter{
			Name:  name,
			Value: fmt.Sprintf("%v", value),
		})
	}

	pipelineSpec := PipelineSpec{
		PipelineID: pipelineID,
		Parameters: params,
	}
	if versionID != "" {
		pipelineSpec.PipelineVersionID = versionID
	}

	runReq := RunRequest{
		Name:         runName,
		Description:  "Run created by Cloud Continuum Orchestrator",
		PipelineSpec: pipelineSpec,
		ResourceReferences: []ResourceReference{
			{
				Key:          ResourceKey{Type: "EXPERIMENT", ID: experimentID},
				Relationship: "OWNER",
			},
		},
	}

	var runResp RunResponse
	if err := c.doRequest("POST", endpointRuns, runReq, &runResp); err != nil {
		return "", "", err
	}

	if runResp.Error != "" {
		return "", "", fmt.Errorf("API returned run error: %s", runResp.Error)
	}

	runURL := fmt.Sprintf("%s/_/pipeline/#/runs/details/%s", c.BaseURL, runResp.Run.ID)
	return runResp.Run.ID, runURL, nil
}
