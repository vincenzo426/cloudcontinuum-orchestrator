package kubeflow

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	defaultExperimentName = "cloudcontinuum-default"
)

// Manager gestisce i client Kubeflow per cluster multipli nel CloudContinuum.
type Manager struct {
	clients   map[string]*Client
	namespace string
}

// NewManager crea e inizializza un nuovo Manager configurato con i client per tutti i cluster.
func NewManager(namespace string) *Manager {
	clients := make(map[string]*Client)
	endpoints := getKubeflowEndpoints()

	// Inizializza i client per ogni endpoint disponibile
	for name, url := range endpoints {
		// Nota: Token vuoto per modalità standalone
		clients[name] = NewClient(url, namespace, "")
	}

	return &Manager{
		clients:   clients,
		namespace: namespace,
	}
}

// getKubeflowEndpoints restituisce la mappa statica dei cluster e i loro endpoint.
// TODO: In futuro, spostare questa configurazione in una ConfigMap o flag.
func getKubeflowEndpoints() map[string]string {
	// Porta 8888: Gateway Submariner verso il servizio ml-pipeline
	return map[string]string{
		"cloud_cluster":  "http://cloud-cluster.ml-pipeline.kubeflow.svc.clusterset.local.:8888",
		"edge_cluster_1": "http://edge-cluster-1.ml-pipeline.kubeflow.svc.clusterset.local.:8888",
		"edge_cluster_2": "http://edge-cluster-2.ml-pipeline.kubeflow.svc.clusterset.local.:8888",
		"edge_cluster_3": "http://edge-cluster-3.ml-pipeline.kubeflow.svc.clusterset.local.:8888",
	}
}

// GetClient restituisce il client per un cluster specifico.
func (m *Manager) GetClient(clusterName string) (*Client, error) {
	client, ok := m.clients[clusterName]
	if !ok {
		return nil, fmt.Errorf("client not found for cluster: %s", clusterName)
	}
	return client, nil
}

// UploadAndRunPipeline orchestra il deploy completo: Upload -> Experiment -> Run.
func (m *Manager) UploadAndRunPipeline(
	ctx context.Context,
	clusterName string,
	pipelineName string,
	pipelineYAML []byte,
	runName string,
	experimentID string,
	experimentName string,
	parameters map[string]interface{},
) (string, string, error) {

	logger := log.FromContext(ctx)

	// 1. Ottieni il client
	client, err := m.GetClient(clusterName)
	if err != nil {
		return "", "", err
	}

	// 2. Upload o Versionamento Pipeline
	pipelineID, versionID, err := client.UploadOrVersionPipeline(pipelineName, pipelineYAML)
	if err != nil {
		return "", "", fmt.Errorf("pipeline upload failed: %w", err)
	}

	logger.Info("Pipeline prepared", "cluster", clusterName, "pipelineID", pipelineID, "versionID", versionID)

	// 3. Risoluzione Experiment
	finalExpID := experimentID
	if finalExpID == "" {
		targetName := experimentName
		if targetName == "" {
			targetName = defaultExperimentName
		}

		// Get or Create
		finalExpID, err = client.GetOrCreateExperiment(targetName)
		if err != nil {
			return "", "", fmt.Errorf("experiment setup failed: %w", err)
		}
	}

	// 4. Creazione Run
	logger.Info("Starting run", "name", runName, "experimentID", finalExpID)

	runID, runURL, err := client.CreateRun(pipelineID, versionID, runName, finalExpID, parameters)
	if err != nil {
		return "", "", fmt.Errorf("run creation failed: %w", err)
	}

	logger.Info("Run created successfully", "runID", runID, "url", runURL)

	return runID, runURL, nil
}
