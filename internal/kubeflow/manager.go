package kubeflow

import (
	"context"
	"fmt"
	"os"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Manager manages Kubeflow clients for multiple clusters
type Manager struct {
	clients   map[string]*Client
	namespace string
}

// NewManager creates a new Kubeflow manager
func NewManager(namespace string) *Manager {
	clients := make(map[string]*Client)
	endpoints := getKubeflowEndpoints()

	// 1. Leggi il token dal file montato
	token, err := readTokenFromFile("/var/run/secrets/kubeflow/token")
	if err != nil {
		// Logga errore ma continua (magari siamo in locale senza token)
		fmt.Printf("WARNING: Could not read Kubeflow token: %v\n", err)
	} else {
		fmt.Println("INFO: Kubeflow token loaded successfully")
	}

	for clusterName, endpoint := range endpoints {
		// 2. Passa il token al client
		clients[clusterName] = NewClient(endpoint, namespace, token)
	}

	return &Manager{
		clients:   clients,
		namespace: namespace,
	}
}

// readTokenFromFile legge e pulisce il token dal file
func readTokenFromFile(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(content)), nil
}

// getKubeflowEndpoints returns endpoints based on environment
func getKubeflowEndpoints() map[string]string {
	// Production - Submariner clusterset DNS puntando al GATEWAY
    // Nota: Aggiungiamo "/pipeline" alla fine perché il Gateway usa questo prefisso
    // per instradare le richieste al servizio ml-pipeline
    return map[string]string{
        "cloud_cluster":  "http://cloud-cluster.ml-pipeline.kubeflow.svc.clusterset.local.:8888",
		"edge_cluster_1": "http://edge-cluster-1.ml-pipeline.kubeflow.svc.clusterset.local.:8888",
		"edge_cluster_2": "http://edge-cluster-2.ml-pipeline.kubeflow.svc.clusterset.local.:8888",
		"edge_cluster_3": "http://edge-cluster-3.ml-pipeline.kubeflow.svc.clusterset.local.:8888",
    }
}

// GetClient returns the Kubeflow client for a specific cluster
func (m *Manager) GetClient(clusterName string) (*Client, error) {
	client, ok := m.clients[clusterName]
	if !ok {
		return nil, fmt.Errorf("no Kubeflow client for cluster: %s", clusterName)
	}
	return client, nil
}

// UploadAndRunPipeline uploads a pipeline and creates a run with experiment support
func (m *Manager) UploadAndRunPipeline(
	ctx context.Context,
	clusterName string,
	pipelineName string,
	pipelineYAML []byte,
	runName string,
	experimentID string,
	experimentName string,
	parameters map[string]interface{},
) (runID, runURL string, err error) {

	logger := log.FromContext(ctx)

	// Get client for target cluster
	client, err := m.GetClient(clusterName)
	if err != nil {
		return "", "", fmt.Errorf("failed to get client for cluster %s: %w", clusterName, err)
	}

	// Step 1: Upload pipeline
	logger.Info("Uploading pipeline to Kubeflow",
		"cluster", clusterName,
		"baseURL", client.BaseURL,
		"name", pipelineName)

	pipelineID, err := client.UploadPipeline(pipelineName, pipelineYAML)
	if err != nil {
		return "", "", fmt.Errorf("failed to upload pipeline: %w", err)
	}

	logger.Info("Pipeline uploaded successfully",
		"pipelineID", pipelineID,
		"cluster", clusterName)

	// Step 2: Get/Create Experiment
	var finalExperimentID string

	if experimentID != "" {
		// Use user-specified experiment
		logger.Info("Using user-specified experiment", "experimentID", experimentID)
		finalExperimentID = experimentID
	} else {
		// Create/find default experiment
		defaultExpName := experimentName
		if defaultExpName == "" {
			defaultExpName = "cloudcontinuum-default"
		}

		logger.Info("Getting or creating default experiment", "name", defaultExpName)
		finalExperimentID, err = client.GetOrCreateExperiment(defaultExpName)
		if err != nil {
			return "", "", fmt.Errorf("failed to get/create experiment: %w", err)
		}

		logger.Info("Experiment ready",
			"experimentID", finalExperimentID,
			"name", defaultExpName)
	}

	// Step 3: Create Run
	logger.Info("Creating pipeline run",
		"pipelineID", pipelineID,
		"experimentID", finalExperimentID,
		"name", runName)

	runID, runURL, err = client.CreateRun(pipelineID, runName, finalExperimentID, parameters)
	if err != nil {
		return "", "", fmt.Errorf("failed to create run: %w", err)
	}

	logger.Info("Pipeline run created successfully",
		"runID", runID,
		"runURL", runURL,
		"cluster", clusterName)

	return runID, runURL, nil
}
