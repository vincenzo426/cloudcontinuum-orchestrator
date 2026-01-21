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

	// 3. Valida la risposta
	if response.TargetCluster == "" {
		return "", "", fmt.Errorf("RL model returned empty cluster")
	}

	// 4. Verifica che il cluster sia valido
	if _, exists := metrics.Clusters[response.TargetCluster]; !exists {
		return "", "", fmt.Errorf("RL model returned invalid cluster: %s", response.TargetCluster)
	}

	// 5. Usa la decisione motivata dal modello RL
	decision := response.Reason
	if decision == "" {
		// Fallback se reason non è presente
		decision = s.buildDecisionRationale(response, pipeline, dataLocation)
	}

	return response.TargetCluster, decision, nil
}

// ============================================================================
// COSTRUZIONE RICHIESTA RL
// ============================================================================

// RLRequest rappresenta la richiesta JSON inviata al servizio RL.
// Deve matchare esattamente il formato atteso da inference.py
type RLRequest struct {
	Pipeline map[string]interface{}   `json:"pipeline"`
	Clusters map[string]ClusterState `json:"clusters"`
}

// ClusterState rappresenta lo stato di un singolo cluster.
// Deve matchare esattamente il formato atteso da inference.py
type ClusterState struct {
	CPUCapacity     int64 `json:"cpu_capacity"`      // millicores
	CPUAvailable    int64 `json:"cpu_available"`     // millicores
	MemoryCapacity  int64 `json:"memory_capacity"`   // bytes
	MemoryAvailable int64 `json:"memory_available"`  // bytes
	CPUUsed         int64 `json:"cpu_used"`          // millicores
	MemoryUsed      int64 `json:"memory_used"`       // bytes
}

// RLResponse rappresenta la risposta JSON del servizio RL.
// Deve matchare esattamente il formato ritornato da inference.py
type RLResponse struct {
	TargetCluster       string             `json:"target_cluster"`
	Confidence          float64            `json:"confidence"`
	Reason              string             `json:"reason"`
	ActionProbabilities map[string]float64 `json:"action_probabilities,omitempty"`
}

// buildRLRequest costruisce il payload JSON per il servizio RL.
func (s *RLPipelineStrategy) buildRLRequest(
	pipeline *PipelineIR,
	dataLocation string,
	metrics *placement.ClusterMetrics,
) RLRequest {

	totalResources := CalculatePipelineResources(pipeline, s.parser)

	// Pipeline requirements (MILLICORES e BYTES come atteso da Python)
	pipelineReq := map[string]interface{}{
		"cpu_required":    totalResources.TotalCPU,    // millicores
		"memory_required": totalResources.TotalMemory, // bytes
		"data_location":   dataLocation,
		"pipeline_name":   pipeline.PipelineInfo.Name,
	}

	// Clusters state (MILLICORES e BYTES)
	clustersState := make(map[string]ClusterState)
	for clusterName, clusterMetric := range metrics.Clusters {
		if !clusterMetric.Available {
			continue // Salta cluster non disponibili
		}

		clustersState[clusterName] = ClusterState{
			CPUCapacity:     clusterMetric.CPUCapacity,
			CPUAvailable:    clusterMetric.CPUAvailable,
			MemoryCapacity:  clusterMetric.MemoryCapacity,
			MemoryAvailable: clusterMetric.MemoryAvailable,
			CPUUsed:         clusterMetric.CPUUsed,
			MemoryUsed:      clusterMetric.MemoryUsed,
		}
	}

	return RLRequest{
		Pipeline: pipelineReq,
		Clusters: clustersState,
	}
}

// ============================================================================
// CHIAMATA HTTP AL SERVIZIO RL
// ============================================================================

// callRLInference effettua la chiamata HTTP al servizio Flask.
func (s *RLPipelineStrategy) callRLInference(ctx context.Context, payload RLRequest) (*RLResponse, error) {
	// Serializza payload
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Crea richiesta HTTP
	url := fmt.Sprintf("%s/predict", s.inferenceURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Esegui richiesta
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	// Leggi body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Controlla status code
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("RL service returned status %d: %s", resp.StatusCode, string(body))
	}

	// Deserializza risposta
	var rlResponse RLResponse
	if err := json.Unmarshal(body, &rlResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &rlResponse, nil
}

// ============================================================================
// COSTRUZIONE DECISIONE (FALLBACK)
// ============================================================================

// buildDecisionRationale costruisce una spiegazione della decisione RL.
// Usato solo come fallback se il servizio RL non ritorna 'reason'.
func (s *RLPipelineStrategy) buildDecisionRationale(
	response *RLResponse,
	pipeline *PipelineIR,
	dataLocation string,
) string {

	totalResources := CalculatePipelineResources(pipeline, s.parser)

	decision := fmt.Sprintf("RL-based placement: Selected cluster '%s' with confidence %.2f%%",
		response.TargetCluster, response.Confidence*100)

	// Aggiungi info su risorse (converti per leggibilità)
	cpuCores := float64(totalResources.TotalCPU) / 1000.0
	memoryGB := float64(totalResources.TotalMemory) / (1024 * 1024 * 1024)

	decision += fmt.Sprintf("\nPipeline requirements: %.2f cores, %.2f GB memory",
		cpuCores, memoryGB)

	if totalResources.TotalGPU > 0 {
		decision += fmt.Sprintf(", %d GPU", totalResources.TotalGPU)
	}

	// Aggiungi info su data locality
	if dataLocation != "" && dataLocation != "none" {
		if response.TargetCluster == dataLocation {
			decision += fmt.Sprintf("\nData locality: Co-located with data at '%s'", dataLocation)
		} else {
			decision += fmt.Sprintf("\nData locality: Data at '%s', transfer required", dataLocation)
		}
	}

	// Aggiungi probabilità azioni se disponibili
	if len(response.ActionProbabilities) > 0 {
		decision += "\nAction probabilities:"
		for cluster, prob := range response.ActionProbabilities {
			decision += fmt.Sprintf("\n  - %s: %.2f%%", cluster, prob*100)
		}
	}

	return decision
}