// internal/pipeline/rl_strategy.go
package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

const (
	defaultRLTimeout = 10 * time.Second
)

// RLPipelineStrategy uses an RL model for placement decisions.
type RLPipelineStrategy struct {
	parser       *Parser
	inferenceURL string
	httpClient   *http.Client
}

// NewRLPipelineStrategy creates a new RL-based strategy.
func NewRLPipelineStrategy(inferenceURL string) *RLPipelineStrategy {
	return &RLPipelineStrategy{
		parser:       NewParser(),
		inferenceURL: inferenceURL,
		httpClient: &http.Client{
			Timeout: defaultRLTimeout,
		},
	}
}

// Name returns the strategy name.
func (s *RLPipelineStrategy) Name() string {
	return "rl-based-pipeline"
}

// SelectCluster uses the RL model to select the target cluster.
// dataSize is the size of data to transfer (e.g., "500MB", "2GB").
func (s *RLPipelineStrategy) SelectCluster(
	ctx context.Context,
	pipeline *PipelineIR,
	dataLocation string,
	dataSize string,
	metrics *placement.ClusterMetrics,
) (string, string, error) {

	// 1. Build RL request with data size
	requestPayload := s.buildRLRequest(pipeline, dataLocation, dataSize, metrics)

	// 2. Call inference service
	response, err := s.callRLInference(ctx, requestPayload)
	if err != nil {
		return "", "", fmt.Errorf("RL inference failed: %w", err)
	}

	// 3. Validate response
	if !response.IsValid {
		return "", "", fmt.Errorf("RL model returned invalid prediction: %s", response.Reason)
	}

	// 4. Check cluster was specified
	if response.TargetCluster == "" {
		return "", "", fmt.Errorf("RL model returned empty cluster")
	}

	// 5. Verify cluster exists in metrics
	if _, exists := metrics.Clusters[response.TargetCluster]; !exists {
		return "", "", fmt.Errorf("RL model returned unknown cluster: %s", response.TargetCluster)
	}

	// 6. Build decision rationale
	decision := response.Reason
	if decision == "" {
		decision = s.buildDecisionRationale(response, pipeline, dataLocation, dataSize)
	}

	return response.TargetCluster, decision, nil
}

// ============================================================================
// JSON STRUCTURES
// ============================================================================

// RLRequest represents the JSON request sent to the RL service.
type RLRequest struct {
	Pipeline map[string]interface{}  `json:"pipeline"`
	Clusters map[string]ClusterState `json:"clusters"`
}

// ClusterState represents the state of a single cluster.
type ClusterState struct {
	CPUCapacity     int64 `json:"cpu_capacity"`
	CPUAvailable    int64 `json:"cpu_available"`
	MemoryCapacity  int64 `json:"memory_capacity"`
	MemoryAvailable int64 `json:"memory_available"`
	CPUUsed         int64 `json:"cpu_used,omitempty"`
	MemoryUsed      int64 `json:"memory_used,omitempty"`
}

// RLResponse represents the JSON response from the RL service.
type RLResponse struct {
	TargetCluster       string             `json:"target_cluster"`
	Confidence          float64            `json:"confidence"`
	Reason              string             `json:"reason"`
	IsValid             bool               `json:"is_valid"`
	ActionProbabilities map[string]float64 `json:"action_probabilities,omitempty"`
}

// ============================================================================
// REQUEST BUILDING
// ============================================================================

// parseDataSize converts a human-readable data size string to bytes.
// Supports formats like "100MB", "1.5GB", "500KB", etc.
func parseDataSize(dataSize string) int64 {
	if dataSize == "" {
		return 0
	}

	var value float64
	var unit string

	// Try to parse with unit
	_, err := fmt.Sscanf(dataSize, "%f%s", &value, &unit)
	if err != nil {
		// Try without decimal
		var intValue int64
		_, err = fmt.Sscanf(dataSize, "%d%s", &intValue, &unit)
		if err != nil {
			return 0
		}
		value = float64(intValue)
	}

	// Convert to bytes based on unit
	switch unit {
	case "B", "b":
		return int64(value)
	case "KB", "kb", "K", "k":
		return int64(value * 1024)
	case "MB", "mb", "M", "m":
		return int64(value * 1024 * 1024)
	case "GB", "gb", "G", "g":
		return int64(value * 1024 * 1024 * 1024)
	case "TB", "tb", "T", "t":
		return int64(value * 1024 * 1024 * 1024 * 1024)
	default:
		// Assume bytes if no unit recognized
		return int64(value)
	}
}

func (s *RLPipelineStrategy) buildRLRequest(
	pipeline *PipelineIR,
	dataLocation string,
	dataSize string,
	metrics *placement.ClusterMetrics,
) RLRequest {

	totalResources := CalculatePipelineResources(pipeline, s.parser)

	// Parse data size to bytes
	dataSizeBytes := parseDataSize(dataSize)
	if dataSizeBytes == 0 {
		// Default: 100MB if not specified
		dataSizeBytes = 100 * 1024 * 1024
	}

	// Pipeline requirements (millicores, bytes, and data_size)
	pipelineReq := map[string]interface{}{
		"cpu_required":    totalResources.TotalCPU,
		"memory_required": totalResources.TotalMemory,
		"data_location":   dataLocation,
		"data_size":       dataSizeBytes,
	}

	// Clusters state
	clustersState := make(map[string]ClusterState)
	for clusterName, clusterMetric := range metrics.Clusters {
		if !clusterMetric.Available {
			continue
		}

		clustersState[clusterName] = ClusterState{
			CPUCapacity:     clusterMetric.CPUCapacity,
			CPUAvailable:    clusterMetric.CPUAvailable,
			MemoryCapacity:  clusterMetric.MemoryCapacity,
			MemoryAvailable: clusterMetric.MemoryAvailable,
		}
	}

	return RLRequest{
		Pipeline: pipelineReq,
		Clusters: clustersState,
	}
}

// ============================================================================
// HTTP CALL
// ============================================================================

func (s *RLPipelineStrategy) callRLInference(ctx context.Context, payload RLRequest) (*RLResponse, error) {
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	url := fmt.Sprintf("%s/predict", s.inferenceURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("RL service returned status %d: %s", resp.StatusCode, string(body))
	}

	var rlResponse RLResponse
	if err := json.Unmarshal(body, &rlResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &rlResponse, nil
}

// ============================================================================
// DECISION RATIONALE (FALLBACK)
// ============================================================================

func (s *RLPipelineStrategy) buildDecisionRationale(
	response *RLResponse,
	pipeline *PipelineIR,
	dataLocation string,
	dataSize string,
) string {

	totalResources := CalculatePipelineResources(pipeline, s.parser)

	cpuCores := float64(totalResources.TotalCPU) / 1000.0
	memoryGB := float64(totalResources.TotalMemory) / (1024 * 1024 * 1024)

	decision := fmt.Sprintf("RL placement: %s (conf: %.0f%%, cpu: %.2f cores, mem: %.2f GB)",
		response.TargetCluster, response.Confidence*100, cpuCores, memoryGB)

	// Add data locality info
	if dataLocation != "" && dataLocation != "none" && dataLocation != "distributed" {
		if response.TargetCluster == dataLocation {
			decision += " [data local]"
		} else {
			decision += fmt.Sprintf(" [data at %s", dataLocation)
			if dataSize != "" {
				decision += fmt.Sprintf(", size: %s", dataSize)
			}
			decision += "]"
		}
	}

	return decision
}
