package kubeflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"time"
)

// Client wraps HTTP calls to Kubeflow Pipelines API
type Client struct {
	BaseURL             string
	Namespace           string
	UserEmail           string // User email for multi-user authentication
	ServiceAccountToken string // Kubernetes ServiceAccount token for auth
	HTTPClient          *http.Client
}

// NewClient creates a new Kubeflow client
func NewClient(baseURL, namespace string) *Client {
	// Derive user email from namespace
	userEmail := namespaceToEmail(namespace)

	// Read ServiceAccount token from pod filesystem
	token := readServiceAccountToken()

	return &Client{
		BaseURL:             baseURL,
		Namespace:           namespace,
		UserEmail:           userEmail,
		ServiceAccountToken: token,
		HTTPClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}

	fmt.Printf("WARN: No ServiceAccount token found, running without authentication\n")
	return ""
}

// readServiceAccountToken reads authentication token from the pod filesystem
// Priority 1: Kubeflow-specific token (mounted as Secret)
// Priority 2: Standard Kubernetes ServiceAccount token
// Priority 3: Empty string (for local development)
func readServiceAccountToken() string {
	// Try Kubeflow-specific token first (mounted from kubeflow-api-token Secret)
	tokenPath := "/var/run/secrets/kubeflow/token"
	tokenBytes, err := os.ReadFile(tokenPath)
	if err == nil {
		fmt.Printf("INFO: Using Kubeflow-specific token from %s\n", tokenPath)
		return strings.TrimSpace(string(tokenBytes))
	}

	// Fallback to standard ServiceAccount token
	tokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	tokenBytes, err = os.ReadFile(tokenPath)
	if err == nil {
		fmt.Printf("INFO: Using standard ServiceAccount token from %s\n", tokenPath)
		return strings.TrimSpace(string(tokenBytes))
	}

	// No token found - will work locally but auth will fail
	fmt.Printf("WARN: No ServiceAccount token found, running without authentication\n")
	return ""
}

// namespaceToEmail converts Kubeflow namespace to user email
func namespaceToEmail(namespace string) string {
	// Remove "kubeflow-" prefix if present
	email := strings.TrimPrefix(namespace, "kubeflow-")

	// Replace hyphens with @ and dots
	// Example: "user-example-com" -> "user@example.com"
	parts := strings.Split(email, "-")
	if len(parts) < 3 {
		// Fallback: if format doesn't match, use namespace as-is
		return email + "@example.com"
	}

	// Reconstruct email: first part + @ + rest with dots
	return parts[0] + "@" + strings.Replace(strings.Join(parts[1:], "-"), "-", ".", -1)
}

// addAuthHeaders adds authentication headers required by Kubeflow multi-user mode
func (c *Client) addAuthHeaders(req *http.Request) {
	// Add user identity header
	req.Header.Set("kubeflow-userid", c.UserEmail)

	// Add Kubernetes ServiceAccount token for authentication
	if c.ServiceAccountToken != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.ServiceAccountToken))
	}
}

// ============================================================================
// UPLOAD PIPELINE
// ============================================================================

// PipelineUploadResponse is the response from pipeline upload
type PipelineUploadResponse struct {
	ID          string    `json:"id"`
	CreatedAt   time.Time `json:"created_at"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Error       string    `json:"error"`
}

// UploadPipeline uploads a pipeline YAML to Kubeflow
func (c *Client) UploadPipeline(name string, pipelineYAML []byte) (string, error) {
	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)

	// Add uploadfile field with YAML content
	part, err := writer.CreateFormFile("uploadfile", name+".yaml")
	if err != nil {
		return "", fmt.Errorf("failed to create form file: %w", err)
	}

	if _, err := part.Write(pipelineYAML); err != nil {
		return "", fmt.Errorf("failed to write pipeline yaml: %w", err)
	}

	// Add name field
	if err := writer.WriteField("name", name); err != nil {
		return "", fmt.Errorf("failed to write name field: %w", err)
	}

	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("failed to close writer: %w", err)
	}

	// Create request
	url := fmt.Sprintf("%s/apis/v1beta1/pipelines/upload", c.BaseURL)
	req, err := http.NewRequest("POST", url, &requestBody)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())

	c.addAuthHeaders(req) // Add authentication headers

	// Execute request
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to upload pipeline: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(body))
	}

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
// EXPERIMENT MANAGEMENT
// ============================================================================

// ExperimentResponse from API
type ExperimentResponse struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Namespace   string    `json:"namespace"`
	CreatedAt   time.Time `json:"created_at"`
}

// ExperimentListResponse for list experiments
type ExperimentListResponse struct {
	Experiments   []ExperimentResponse `json:"experiments"`
	TotalSize     int                  `json:"total_size"`
	NextPageToken string               `json:"next_page_token"`
}

// GetOrCreateExperiment finds an existing experiment by name or creates a new one
func (c *Client) GetOrCreateExperiment(experimentName string) (string, error) {
	// Step 1: Try to find existing experiment
	experimentID, err := c.FindExperimentByName(experimentName)
	if err == nil && experimentID != "" {
		return experimentID, nil
	}

	// Step 2: If not exists, create it
	return c.CreateExperiment(experimentName, fmt.Sprintf("Default experiment for %s", experimentName))
}

// FindExperimentByName searches for an experiment by name with namespace filtering
func (c *Client) FindExperimentByName(name string) (string, error) {
	// In multi-user mode, must use resource_reference_key for namespace filtering
	url := fmt.Sprintf("%s/apis/v1beta1/experiments", c.BaseURL)

	// Add namespace filtering via resource_reference_key
	if c.Namespace != "" {
		url = fmt.Sprintf("%s?resource_reference_key.type=NAMESPACE&resource_reference_key.id=%s",
			url, c.Namespace)
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	c.addAuthHeaders(req) // Add authentication headers

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to list experiments: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("list experiments failed with status %d: %s", resp.StatusCode, string(body))
	}

	var listResp ExperimentListResponse
	if err := json.Unmarshal(body, &listResp); err != nil {
		return "", fmt.Errorf("failed to parse experiment list response: %w", err)
	}

	// Search for experiment with exact name
	for _, exp := range listResp.Experiments {
		if exp.Name == name {
			return exp.ID, nil
		}
	}

	return "", fmt.Errorf("experiment %s not found", name)
}

// CreateExperiment creates a new experiment with namespace support for multi-user mode
func (c *Client) CreateExperiment(name, description string) (string, error) {
	// CRITICAL: In multi-user mode, Kubeflow requires resource_references with NAMESPACE
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

	url := fmt.Sprintf("%s/apis/v1beta1/experiments", c.BaseURL)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	c.addAuthHeaders(req) // Add authentication headers

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to create experiment: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("create experiment failed with status %d: %s", resp.StatusCode, string(body))
	}

	var expResp ExperimentResponse
	if err := json.Unmarshal(body, &expResp); err != nil {
		return "", fmt.Errorf("failed to parse experiment response: %w", err)
	}

	return expResp.ID, nil
}

// ============================================================================
// RUN CREATION
// ============================================================================

// ResourceKey identifies a Kubeflow resource
type ResourceKey struct {
	Type string `json:"type"` // e.g., "EXPERIMENT", "PIPELINE"
	ID   string `json:"id"`
}

// ResourceReference links a run to other resources
type ResourceReference struct {
	Key          ResourceKey `json:"key"`
	Relationship string      `json:"relationship"` // e.g., "OWNER"
}

// PipelineSpec specifies which pipeline to run
type PipelineSpec struct {
	PipelineID string                 `json:"pipeline_id,omitempty"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
}

// RunRequest is the request body for creating a run
type RunRequest struct {
	Name               string              `json:"name"`
	Description        string              `json:"description,omitempty"`
	PipelineSpec       PipelineSpec        `json:"pipeline_spec"`
	ResourceReferences []ResourceReference `json:"resource_references"`
}

// Run contains run details
type Run struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
	FinishedAt  time.Time `json:"finished_at"`
	Description string    `json:"description"`
}

// RunResponse is the response from creating a run
type RunResponse struct {
	Run   Run    `json:"run"`
	Error string `json:"error"`
}

// CreateRun creates a new pipeline run
func (c *Client) CreateRun(pipelineID, runName, experimentID string, parameters map[string]interface{}) (string, string, error) {
	runReq := RunRequest{
		Name:        runName,
		Description: "Run created by Cloud Continuum Orchestrator",
		PipelineSpec: PipelineSpec{
			PipelineID: pipelineID,
			Parameters: parameters,
		},
		ResourceReferences: []ResourceReference{
			{
				Key: ResourceKey{
					Type: "EXPERIMENT",
					ID:   experimentID,
				},
				Relationship: "OWNER",
			},
		},
	}

	jsonData, err := json.Marshal(runReq)
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal run request: %w", err)
	}

	url := fmt.Sprintf("%s/apis/v1beta1/runs", c.BaseURL)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	c.addAuthHeaders(req) // Add authentication headers

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("failed to create run: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", "", fmt.Errorf("run creation failed with status %d: %s", resp.StatusCode, string(body))
	}

	var runResp RunResponse
	if err := json.Unmarshal(body, &runResp); err != nil {
		return "", "", fmt.Errorf("failed to parse run response: %w", err)
	}

	if runResp.Error != "" {
		return "", "", fmt.Errorf("run creation error: %s", runResp.Error)
	}

	// Build run URL in Kubeflow UI
	runURL := fmt.Sprintf("%s/_/pipeline/#/runs/details/%s", c.BaseURL, runResp.Run.ID)

	return runResp.Run.ID, runURL, nil
}
