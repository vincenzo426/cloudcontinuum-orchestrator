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

// RLPipelineStrategy usa un modello RL per il placement.
type RLPipelineStrategy struct {
	parser       *Parser
	inferenceURL string
	httpClient   *http.Client
}

// NewRLPipelineStrategy crea una nuova strategia RL.
func NewRLPipelineStrategy(inferenceURL string) *RLPipelineStrategy {
	return &RLPipelineStrategy{
		parser:       NewParser(),
		inferenceURL: inferenceURL,
		httpClient: &http.Client{
			Timeout: defaultRLTimeout,
		},
	}
}

// Name ritorna il nome della strategia.
func (s *RLPipelineStrategy) Name() string {
	return "rl-based-pipeline"
}

// SelectCluster usa il modello RL per selezionare il cluster.
func (s *RLPipelineStrategy) SelectCluster(
	ctx context.Context,
	pipeline *PipelineIR,
	dataLocation string,
	metrics *placement.ClusterMetrics,
) (string, string, error) {

	// 1. Costruisci lo stato per il modello RL
	requestPayload := s.buildRLRequest(pipeline, dataLocation, metrics)

	// 2. Chiamata al servizio di inference
	response, err := s.callRLInference(ctx, requestPayload)
	if err != nil {
		return "", "", fmt.Errorf("RL inference failed: %w", err)
	}

	// 3. Verifica che la risposta sia valida (NUOVO CHECK!)
	if !response.IsValid {
		return "", "", fmt.Errorf("RL model returned invalid prediction: %s", response.Reason)
	}

	// 4. Verifica che il cluster sia stato specificato
	if response.TargetCluster == "" {
		return "", "", fmt.Errorf("RL model returned empty cluster")
	}

	// 5. Verifica che il cluster esista nelle metriche
	if _, exists := metrics.Clusters[response.TargetCluster]; !exists {
		return "", "", fmt.Errorf("RL model returned unknown cluster: %s", response.TargetCluster)
	}

	// 6. Usa la decisione motivata dal modello RL
	decision := response.Reason
	if decision == "" {
		decision = s.buildDecisionRationale(response, pipeline, dataLocation)
	}

	return response.TargetCluster, decision, nil
}

// ============================================================================
// STRUTTURE DATI JSON
// ============================================================================

// RLRequest rappresenta la richiesta JSON inviata al servizio RL.
type RLRequest struct {
	Pipeline map[string]interface{}  `json:"pipeline"`
	Clusters map[string]ClusterState `json:"clusters"`
}

// ClusterState rappresenta lo stato di un singolo cluster.
type ClusterState struct {
	CPUCapacity     int64 `json:"cpu_capacity"`
	CPUAvailable    int64 `json:"cpu_available"`
	MemoryCapacity  int64 `json:"memory_capacity"`
	MemoryAvailable int64 `json:"memory_available"`
	CPUUsed         int64 `json:"cpu_used,omitempty"`
	MemoryUsed      int64 `json:"memory_used,omitempty"`
}

// RLResponse rappresenta la risposta JSON del servizio RL.
type RLResponse struct {
	TargetCluster       string             `json:"target_cluster"`
	Confidence          float64            `json:"confidence"`
	Reason              string             `json:"reason"`
	IsValid             bool               `json:"is_valid"` // IMPORTANTE: gestisce casi di errore
	ActionProbabilities map[string]float64 `json:"action_probabilities,omitempty"`
}

// ============================================================================
// COSTRUZIONE RICHIESTA
// ============================================================================

func (s *RLPipelineStrategy) buildRLRequest(
	pipeline *PipelineIR,
	dataLocation string,
	metrics *placement.ClusterMetrics,
) RLRequest {

	totalResources := CalculatePipelineResources(pipeline, s.parser)

	// Pipeline requirements (millicores e bytes)
	pipelineReq := map[string]interface{}{
		"cpu_required":    totalResources.TotalCPU,
		"memory_required": totalResources.TotalMemory,
		"data_location":   dataLocation,
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
// CHIAMATA HTTP
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
// COSTRUZIONE DECISIONE (FALLBACK)
// ============================================================================

func (s *RLPipelineStrategy) buildDecisionRationale(
	response *RLResponse,
	pipeline *PipelineIR,
	dataLocation string,
) string {

	totalResources := CalculatePipelineResources(pipeline, s.parser)

	cpuCores := float64(totalResources.TotalCPU) / 1000.0
	memoryGB := float64(totalResources.TotalMemory) / (1024 * 1024 * 1024)

	decision := fmt.Sprintf("RL placement: %s (conf: %.0f%%, cpu: %.2f cores, mem: %.2f GB)",
		response.TargetCluster, response.Confidence*100, cpuCores, memoryGB)

	if dataLocation != "" && dataLocation != "none" && dataLocation != "distributed" {
		if response.TargetCluster == dataLocation {
			decision += " [data local]"
		} else {
			decision += fmt.Sprintf(" [data at %s]", dataLocation)
		}
	}

	return decision
}
