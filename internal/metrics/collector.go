package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	//"k8s.io/apimachinery/pkg/api/resource"
	//metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

// RealMetricsCollector implementa Collector usando metriche reali
type RealMetricsCollector struct {
	config  *Config
	clients map[string]client.Client

	// Cache delle metriche
	cache          *placement.ClusterMetrics
	cacheTimestamp time.Time
	cacheMutex     sync.RWMutex

	// Background refresh
	stopChan chan struct{}
	stopWg   sync.WaitGroup
}

// NewRealMetricsCollector crea un nuovo collector
func NewRealMetricsCollector(clients map[string]client.Client, config *Config) *RealMetricsCollector {
	if config == nil {
		config = DefaultConfig()
	}

	return &RealMetricsCollector{
		config:   config,
		clients:  clients,
		cache:    placement.NewClusterMetrics(),
		stopChan: make(chan struct{}),
	}
}

// Start avvia il refresh periodico in background
func (c *RealMetricsCollector) Start(ctx context.Context) {
	logger := log.FromContext(ctx)
	logger.Info("Starting real metrics collector",
		"refreshInterval", c.config.RefreshInterval,
		"clusters", len(c.clients))

	// Primo refresh immediato
	if err := c.refresh(ctx); err != nil {
		logger.Error(err, "Initial metrics refresh failed")
	}

	// Avvia goroutine per refresh periodico
	c.stopWg.Add(1)
	go c.backgroundRefresh(ctx)
}

// Stop ferma il refresh in background
func (c *RealMetricsCollector) Stop() {
	close(c.stopChan)
	c.stopWg.Wait()
}

// CollectMetrics ritorna le metriche correnti (dalla cache)
func (c *RealMetricsCollector) CollectMetrics(ctx context.Context) (*placement.ClusterMetrics, error) {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()

	// Verifica se cache è ancora valida
	if time.Since(c.cacheTimestamp) > c.config.CacheTTL {
		return nil, fmt.Errorf("metrics cache expired (age: %v)", time.Since(c.cacheTimestamp))
	}

	return c.cache, nil
}

// backgroundRefresh esegue refresh periodici
func (c *RealMetricsCollector) backgroundRefresh(ctx context.Context) {
	defer c.stopWg.Done()

	logger := log.FromContext(ctx)
	ticker := time.NewTicker(c.config.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.refresh(ctx); err != nil {
				logger.Error(err, "Background metrics refresh failed")
			}
		case <-c.stopChan:
			logger.Info("Stopping metrics collector")
			return
		}
	}
}

// refresh raccoglie metriche fresche da tutti i cluster
func (c *RealMetricsCollector) refresh(ctx context.Context) error {
	logger := log.FromContext(ctx)
	logger.V(1).Info("Refreshing cluster metrics")

	newMetrics := placement.NewClusterMetrics()

	// Raccolta parallela da tutti i cluster
	var wg sync.WaitGroup
	var mutex sync.Mutex

	for clusterName, clusterClient := range c.clients {
		wg.Add(1)
		go func(name string, cli client.Client) {
			defer wg.Done()

			metric, err := c.collectClusterMetrics(ctx, name, cli)
			if err != nil {
				logger.Error(err, "Failed to collect metrics from cluster", "cluster", name)
				// Opzione A: Se errore, non aggiungere il cluster (viene escluso)
				return
			}

			mutex.Lock()
			newMetrics.SetCluster(name, metric)
			mutex.Unlock()
		}(clusterName, clusterClient)
	}

	wg.Wait()

	// Misura latenze cross-cluster
	c.measureLatencies(ctx, newMetrics)

	// Aggiorna cache
	c.cacheMutex.Lock()
	c.cache = newMetrics
	c.cacheTimestamp = time.Now()
	c.cacheMutex.Unlock()

	logger.Info("Metrics refresh completed",
		"clusters", len(newMetrics.Clusters),
		"timestamp", c.cacheTimestamp)

	return nil
}

// collectClusterMetrics raccoglie metriche da un singolo cluster
// collectClusterMetrics raccoglie metriche da un singolo cluster
func (c *RealMetricsCollector) collectClusterMetrics(ctx context.Context, clusterName string, cli client.Client) (*placement.ClusterMetric, error) {
	// Timeout per questa operazione
	timeoutCtx, cancel := context.WithTimeout(ctx, c.config.MetricsTimeout)
	defer cancel()

	logger := log.FromContext(ctx)

	// 1. Ottieni lista nodi
	nodeList := &corev1.NodeList{}
	if err := cli.List(timeoutCtx, nodeList); err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}

	if len(nodeList.Items) == 0 {
		return nil, fmt.Errorf("no nodes found in cluster")
	}

	// 2. Calcola capacità totale dai nodi
	var totalCPUCapacity, totalMemoryCapacity int64

	for _, node := range nodeList.Items {
		cpuCap := node.Status.Capacity[corev1.ResourceCPU]
		memCap := node.Status.Capacity[corev1.ResourceMemory]

		totalCPUCapacity += cpuCap.MilliValue()
		totalMemoryCapacity += memCap.Value()
	}

	// 3. Ottieni lista di TUTTI i pod nel cluster per calcolare requests
	podList := &corev1.PodList{}
	if err := cli.List(timeoutCtx, podList); err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	// 4. Somma i resource REQUESTS di tutti i pod (non usage!)
	var totalCPURequested, totalMemoryRequested int64

	for _, pod := range podList.Items {
		// Skippa pod terminated
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}

		for _, container := range pod.Spec.Containers {
			if container.Resources.Requests != nil {
				if cpuReq, ok := container.Resources.Requests[corev1.ResourceCPU]; ok {
					totalCPURequested += cpuReq.MilliValue()
				}
				if memReq, ok := container.Resources.Requests[corev1.ResourceMemory]; ok {
					totalMemoryRequested += memReq.Value()
				}
			}
		}
	}

	// 5. Ottieni anche metriche usage per logging (opzionale)
	var totalCPUUsed, totalMemoryUsed int64
	nodeMetricsList := &metricsv1beta1.NodeMetricsList{}
	if err := cli.List(timeoutCtx, nodeMetricsList); err != nil {
		logger.V(1).Info("Could not get node metrics (usage), using requests only", "cluster", clusterName, "error", err)
	} else {
		for _, nodeMetrics := range nodeMetricsList.Items {
			cpuUsage := nodeMetrics.Usage[corev1.ResourceCPU]
			memUsage := nodeMetrics.Usage[corev1.ResourceMemory]
			totalCPUUsed += cpuUsage.MilliValue()
			totalMemoryUsed += memUsage.Value()
		}
	}

	// 6. Calcola disponibilità basandosi su REQUESTS (come fa Kubernetes scheduler!)
	totalCPUAvailable := totalCPUCapacity - totalCPURequested
	totalMemoryAvailable := totalMemoryCapacity - totalMemoryRequested

	// Log dettagliato
	logger.V(1).Info("Cluster metrics collected",
		"cluster", clusterName,
		"cpuCapacity", totalCPUCapacity,
		"cpuRequested", totalCPURequested,
		"cpuUsed", totalCPUUsed,
		"cpuAvailable", totalCPUAvailable,
		"memoryCapacity", totalMemoryCapacity,
		"memoryRequested", totalMemoryRequested,
		"memoryAvailable", totalMemoryAvailable)

	return &placement.ClusterMetric{
		Name:            clusterName,
		CPUCapacity:     totalCPUCapacity,
		CPUUsed:         totalCPURequested, // ✅ Ora usa REQUESTS invece di usage
		CPUAvailable:    totalCPUAvailable,
		MemoryCapacity:  totalMemoryCapacity,
		MemoryUsed:      totalMemoryRequested, // ✅ Ora usa REQUESTS
		MemoryAvailable: totalMemoryAvailable,
		Available:       true,
	}, nil
}

// measureLatencies misura latenza tra cluster
func (c *RealMetricsCollector) measureLatencies(ctx context.Context, metrics *placement.ClusterMetrics) {
	logger := log.FromContext(ctx)

	// Per ogni coppia di cluster, misura latenza
	clusterNames := []string{"cloud_cluster", "edge_cluster_1", "edge_cluster_2,", "edge_cluster_3"}

	for _, fromCluster := range clusterNames {
		fromMetric := metrics.GetCluster(fromCluster)
		if fromMetric == nil {
			continue
		}

		for _, toCluster := range clusterNames {
			if fromCluster == toCluster {
				// Latenza verso se stesso = 0
				continue
			}

			toClient, ok := c.clients[toCluster]
			if !ok {
				continue
			}

			// Misura latency con HTTP ping all'API server
			latency := c.measureLatency(ctx, toClient)

			// Salva nella metrica appropriata
			switch toCluster {
			case "edge_cluster_1":
				fromMetric.LatencyToEdge1 = latency
			case "edge_cluster_2":
				fromMetric.LatencyToEdge2 = latency
			case "cloud_cluster":
				fromMetric.LatencyToCloud = latency
			}
		}
	}

	logger.V(1).Info("Latency measurements completed")
}

// measureLatency misura latenza verso un cluster specifico
func (c *RealMetricsCollector) measureLatency(ctx context.Context, cli client.Client) int64 {
	timeoutCtx, cancel := context.WithTimeout(ctx, c.config.LatencyTimeout)
	defer cancel()

	// Usa una query leggera all'API server come ping
	start := time.Now()

	// Query a un namespace che sicuramente esiste
	namespace := &corev1.Namespace{}
	err := cli.Get(timeoutCtx, client.ObjectKey{Name: "kube-system"}, namespace)

	elapsed := time.Since(start)

	if err != nil {
		// Se errore, ritorna un valore alto per indicare problemi
		return 9999
	}

	return elapsed.Milliseconds()
}
