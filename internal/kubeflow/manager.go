package kubeflow

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	defaultProfileNamespace = "kubeflow-user-example-com"
	defaultExperimentName   = "cloudcontinuum-default"
	authSecretName          = "kubeflow-auth-tokens"
	authSecretNamespace     = "cloudcontinuum-orchestrator-system"
)

// Manager gestisce i client Kubeflow per cluster multipli nel CloudContinuum.
type Manager struct {
	clients   map[string]*Client
	namespace string
}

// NewManager crea e inizializza un nuovo Manager configurato con i client per tutti i cluster.
func NewManager(ctx context.Context, config *rest.Config, scheme *runtime.Scheme) (*Manager, error) {
	logger := log.FromContext(ctx)

	// Crea un DIRECT client (non-cached) per leggere il Secret
	directClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("failed to create direct client: %w", err)
	}

	// Carica i token dal Secret usando il direct client
	tokens, err := loadAuthTokens(ctx, directClient)
	if err != nil {
		logger.Error(err, "Failed to load auth tokens, using standalone mode")
		// Fallback a standalone mode
		tokens = getEmptyTokens()
	}

	clients := make(map[string]*Client)
	endpoints := getKubeflowEndpoints()

	// Inizializza i client per ogni endpoint disponibile
	for name, url := range endpoints {
		token := tokens[name]
		clients[name] = NewClient(url, defaultProfileNamespace, token)

		if token != "" {
			logger.Info("Kubeflow client initialized with authentication",
				"cluster", name,
				"profile", defaultProfileNamespace,
				"mode", "multi-user")
		} else {
			logger.Info("Kubeflow client initialized without authentication",
				"cluster", name,
				"mode", "standalone")
		}
	}

	return &Manager{
		clients:   clients,
		namespace: defaultProfileNamespace,
	}, nil
}

// loadAuthTokens carica i token dal Secret Kubernetes usando un direct client
func loadAuthTokens(ctx context.Context, k8sClient client.Client) (map[string]string, error) {
	logger := log.FromContext(ctx)

	secret := &corev1.Secret{}
	err := k8sClient.Get(ctx, types.NamespacedName{
		Name:      authSecretName,
		Namespace: authSecretNamespace,
	}, secret)

	if err != nil {
		return nil, fmt.Errorf("failed to get auth secret: %w", err)
	}

	tokens := make(map[string]string)
	for key, value := range secret.Data {
		tokenStr := string(value)
		if tokenStr != "" {
			tokens[key] = tokenStr
			logger.Info("Token loaded from secret",
				"cluster", key,
				"tokenLength", len(tokenStr))
		}
	}

	if len(tokens) == 0 {
		logger.Info("Secret found but contains no valid tokens")
		return getEmptyTokens(), nil
	}

	return tokens, nil
}

// getEmptyTokens ritorna una mappa di token vuoti (modalità standalone)
func getEmptyTokens() map[string]string {
	return map[string]string{
		"cloud_cluster":  "",
		"edge_cluster_1": "",
		"edge_cluster_2": "",
		"edge_cluster_3": "",
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
