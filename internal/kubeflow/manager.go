package kubeflow

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ============================================================================
// STRUTTURE DATI
// ============================================================================

// Manager gestisce i client Kubeflow per cluster multipli nel CloudContinuum.
// Mantiene una mappa di client (uno per ogni cluster) e il namespace di default.
type Manager struct {
	clients   map[string]*Client // Mappa clusterName -> Client Kubeflow
	namespace string             // Namespace di default per le operazioni
}

// ============================================================================
// COSTRUTTORE
// ============================================================================

// NewManager crea e inizializza un nuovo Manager Kubeflow.
// Parametri:
//   - namespace: namespace Kubernetes dove verranno create le risorse
//
// Ritorna un Manager configurato con client per tutti i cluster disponibili.
func NewManager(namespace string) *Manager {

	fmt.Printf("═══════════════════════════════════════════════════════\n")
	fmt.Printf("✓ Kubeflow Manager initialized\n")
	fmt.Printf("  Namespace: '%s'\n", namespace)
	fmt.Printf("═══════════════════════════════════════════════════════\n")
	fmt.Printf("\n")

	// Inizializza la mappa dei client
	clients := make(map[string]*Client)

	// Ottiene gli endpoint di tutti i cluster Kubeflow
	endpoints := getKubeflowEndpoints()

	// Crea un client per ogni cluster disponibile
	// Nota: il token è vuoto ("") perché usiamo Kubeflow in standalone mode
	for clusterName, endpoint := range endpoints {
		clients[clusterName] = NewClient(endpoint, namespace, "")

		// LOG DEBUG: Client creato
		fmt.Printf("✓ Client created for cluster: %s\n", clusterName)
		fmt.Printf("  - Endpoint: %s\n", endpoint)
		fmt.Printf("  - Namespace: %s\n", namespace)
		fmt.Printf("\n")
	}

	fmt.Printf("═══════════════════════════════════════════════════════\n")
	fmt.Printf("✓ Kubeflow Manager initialized\n")
	fmt.Printf("  Total clients: %d\n", len(clients))
	fmt.Printf("  Namespace: '%s'\n", namespace)
	fmt.Printf("═══════════════════════════════════════════════════════\n")
	fmt.Printf("\n")

	return &Manager{
		clients:   clients,
		namespace: namespace,
	}
}

// ============================================================================
// CONFIGURAZIONE CLUSTER
// ============================================================================

// getKubeflowEndpoints restituisce gli endpoint dei cluster Kubeflow.
// Utilizza i DNS Submariner ClusterSet per comunicazione cross-cluster.
//
// Formato endpoint: http://<cluster-name>.<service>.<namespace>.svc.clusterset.local.:8888
// Porta 8888: Gateway Submariner che instrada le richieste al servizio ml-pipeline
func getKubeflowEndpoints() map[string]string {
	return map[string]string{
		"cloud_cluster":  "http://cloud-cluster.ml-pipeline.kubeflow.svc.clusterset.local.:8888",
		"edge_cluster_1": "http://edge-cluster-1.ml-pipeline.kubeflow.svc.clusterset.local.:8888",
		"edge_cluster_2": "http://edge-cluster-2.ml-pipeline.kubeflow.svc.clusterset.local.:8888",
		"edge_cluster_3": "http://edge-cluster-3.ml-pipeline.kubeflow.svc.clusterset.local.:8888",
	}
}

// ============================================================================
// METODI PUBBLICI
// ============================================================================

// GetClient restituisce il client Kubeflow per un cluster specifico.
// Parametri:
//   - clusterName: nome del cluster (es. "cloud_cluster", "edge_cluster_1")
//
// Ritorna:
//   - *Client: puntatore al client Kubeflow
//   - error: errore se il cluster non esiste
func (m *Manager) GetClient(clusterName string) (*Client, error) {
	client, ok := m.clients[clusterName]
	if !ok {
		return nil, fmt.Errorf("no Kubeflow client for cluster: %s", clusterName)
	}
	return client, nil
}

// UploadAndRunPipeline orchestra il processo completo di deploy di una pipeline:
// 1. Upload della pipeline YAML al cluster target
// 2. Creazione/recupero dell'experiment
// 3. Creazione ed esecuzione della run
//
// Parametri:
//   - ctx: context per logging e cancellazione
//   - clusterName: cluster dove eseguire la pipeline
//   - pipelineName: nome da assegnare alla pipeline
//   - pipelineYAML: contenuto YAML della pipeline (Kubeflow IR v2.1.0)
//   - runName: nome da assegnare all'esecuzione
//   - experimentID: ID experiment esistente (opzionale, usa "" per auto-creazione)
//   - experimentName: nome experiment da creare (opzionale, default: "cloudcontinuum-default")
//   - parameters: parametri runtime da passare alla pipeline
//
// Ritorna:
//   - runID: ID univoco dell'esecuzione creata
//   - runURL: URL per accedere alla run nella UI Kubeflow
//   - error: errore in caso di fallimento
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

	// ========================================================================
	// FASE 1: Recupero del client per il cluster target
	// ========================================================================
	client, err := m.GetClient(clusterName)
	if err != nil {
		return "", "", fmt.Errorf("failed to get client for cluster %s: %w", clusterName, err)
	}

	// ========================================================================
	// FASE 2: Upload della pipeline al cluster
	// ========================================================================
	// Usa la nuova funzione che gestisce versioning automaticamente
	pipelineID, versionID, err := client.UploadOrVersionPipeline(pipelineName, pipelineYAML)
	if err != nil {
		return "", "", fmt.Errorf("failed to upload/version pipeline: %w", err)
	}

	if versionID != "" {
		logger.Info("Pipeline version created",
			"pipelineID", pipelineID,
			"versionID", versionID,
			"cluster", clusterName)
	} else {
		logger.Info("New pipeline created",
			"pipelineID", pipelineID,
			"cluster", clusterName)
	}

	// ========================================================================
	// FASE 3: Gestione dell'Experiment
	// ========================================================================
	var finalExperimentID string

	if experimentID != "" {
		// Usa l'experiment specificato dall'utente
		logger.Info("Using user-specified experiment", "experimentID", experimentID)
		finalExperimentID = experimentID
	} else {
		// Crea o recupera l'experiment di default
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

	// ========================================================================
	// FASE 4: Creazione ed esecuzione della Run
	// ========================================================================
	logger.Info("Creating pipeline run",
		"pipelineID", pipelineID,
		"experimentID", finalExperimentID,
		"name", runName)

	// Crea run passando anche versionID
	runID, runURL, err = client.CreateRun(pipelineID, versionID, runName, finalExperimentID, parameters)
	if err != nil {
		return "", "", fmt.Errorf("failed to create run: %w", err)
	}

	logger.Info("Pipeline run created successfully",
		"runID", runID,
		"runURL", runURL,
		"cluster", clusterName)

	return runID, runURL, nil
}
