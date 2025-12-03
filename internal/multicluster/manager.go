package multicluster

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClusterManager manages connections to multiple clusters
type ClusterManager struct {
	// ClusterClients maps cluster names to their respective clients
	ClusterClients map[string]client.Client
}

// NewClusterManager creates a new ClusterManager by loading kubeconfigs from a Secret
// NewClusterManager creates a new ClusterManager by loading kubeconfigs from a Secret
// NewClusterManager creates a new ClusterManager by loading kubeconfigs from a Secret
func NewClusterManager(ctx context.Context, config *rest.Config, secretName, secretNamespace string, scheme *runtime.Scheme) (*ClusterManager, error) {
	cm := &ClusterManager{
		ClusterClients: make(map[string]client.Client),
	}

	// Create a direct (non-cached) client to read the Secret
	directClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("failed to create direct client: %w", err)
	}

	// Fetch the Secret containing kubeconfigs
	secret := &corev1.Secret{}
	if err := directClient.Get(ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: secretNamespace,
	}, secret); err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig secret: %w", err)
	}

	// For each kubeconfig in the Secret, create a client
	for clusterName, kubeconfigData := range secret.Data {
		// Load the kubeconfig
		kubeconfig, err := clientcmd.Load(kubeconfigData)
		if err != nil {
			return nil, fmt.Errorf("failed to load kubeconfig for cluster %s: %w", clusterName, err)
		}

		// Determine which context to use
		contextName := kubeconfig.CurrentContext
		if contextName == "" {
			// If no current context, use the first available
			for name := range kubeconfig.Contexts {
				contextName = name
				break
			}
		}

		if contextName == "" {
			return nil, fmt.Errorf("no context found in kubeconfig for cluster %s", clusterName)
		}

		// Build client config from the kubeconfig
		clientConfig := clientcmd.NewNonInteractiveClientConfig(
			*kubeconfig,
			contextName,
			&clientcmd.ConfigOverrides{},
			nil,
		)

		// Get the rest.Config
		clusterConfig, err := clientConfig.ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to create client config for cluster %s (context: %s): %w", clusterName, contextName, err)
		}

		// Create a new scheme with all necessary types
		clusterScheme := runtime.NewScheme()

		// Register standard Kubernetes types (Pod, Namespace, etc.)
		if err := clientgoscheme.AddToScheme(clusterScheme); err != nil {
			return nil, fmt.Errorf("failed to add Kubernetes scheme for cluster %s: %w", clusterName, err)
		}

		// Register metrics types (NodeMetrics, PodMetrics)
		if err := metricsv1beta1.AddToScheme(clusterScheme); err != nil {
			return nil, fmt.Errorf("failed to add metrics scheme for cluster %s: %w", clusterName, err)
		}

		// Create Kubernetes client with complete scheme
		clusterClient, err := client.New(clusterConfig, client.Options{Scheme: clusterScheme})
		if err != nil {
			return nil, fmt.Errorf("failed to create client for cluster %s: %w", clusterName, err)
		}

		cm.ClusterClients[clusterName] = clusterClient

	}

	return cm, nil
}

// GetClient returns the client for a specific cluster
func (cm *ClusterManager) GetClient(clusterName string) (client.Client, error) {
	clusterClient, ok := cm.ClusterClients[clusterName]
	if !ok {
		return nil, fmt.Errorf("no client found for cluster: %s", clusterName)
	}
	return clusterClient, nil
}

// ListClusters returns the names of all configured clusters
func (cm *ClusterManager) ListClusters() []string {
	clusters := make([]string, 0, len(cm.ClusterClients))
	for name := range cm.ClusterClients {
		clusters = append(clusters, name)
	}
	return clusters
}
