package multicluster

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClusterManager manages connections to multiple clusters
type ClusterManager struct {
	// LocalClient is the client for the cluster where controller runs
	LocalClient client.Client

	// ClusterClients maps cluster names to their respective clients
	ClusterClients map[string]client.Client
}

// NewClusterManager creates a new ClusterManager by loading kubeconfigs from a Secret
func NewClusterManager(ctx context.Context, localClient client.Client, secretName, secretNamespace string, scheme *rest.Scheme) (*ClusterManager, error) {
	cm := &ClusterManager{
		LocalClient:    localClient,
		ClusterClients: make(map[string]client.Client),
	}

	// Fetch the Secret containing kubeconfigs
	secret := &corev1.Secret{}
	if err := localClient.Get(ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: secretNamespace,
	}, secret); err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig secret: %w", err)
	}

	// For each kubeconfig in the Secret, create a client
	for clusterName, kubeconfigData := range secret.Data {
		// Parse kubeconfig YAML into rest.Config
		config, err := clientcmd.RESTConfigFromKubeConfig(kubeconfigData)
		if err != nil {
			return nil, fmt.Errorf("failed to parse kubeconfig for cluster %s: %w", clusterName, err)
		}

		// Create Kubernetes client
		clusterClient, err := client.New(config, client.Options{Scheme: scheme})
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
