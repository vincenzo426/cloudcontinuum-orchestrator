package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

// Costanti di configurazione
const (
	unreachableLatency = 9999 // Latenza default per cluster unreachable (ms)
	kubeSystemNS       = "kube-system"

	// Nomi cluster standard
	cloudCluster = "cloud_cluster"
	edgeCluster1 = "edge_cluster_1"
	edgeCluster2 = "edge_cluster_2"
	edgeCluster3 = "edge_cluster_3"
)

var clusterNames = []string{cloudCluster, edgeCluster1, edgeCluster2, edgeCluster3}

// RealMetricsCollector raccoglie metriche reali dai cluster Kubernetes.
type RealMetricsCollector struct {
	config  *Config
	clients map[string]client.Client

	cache          *placement.ClusterMetrics
	cacheTimestamp time.Time
	cacheMutex     sync.RWMutex

	stopChan chan struct{}
	stopWg   sync.WaitGroup
}

// NewRealMetricsCollector crea un nuovo collector di metriche.
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

// Start avvia il refresh periodico delle metriche in background.
func (c *RealMetricsCollector) Start(ctx context.Context) {
	logger := log.FromContext(ctx)
	logger.Info("[METRICS] Starting collector", "refreshInterval", c.config.RefreshInterval, "clusters", len(c.clients))

	// Primo refresh sincrono
	if err := c.refresh(ctx); err != nil {
		logger.Error(err, "[WARN] Initial metrics refresh failed")
	}

	// Avvia background refresh
	c.stopWg.Add(1)
	go c.backgroundRefresh(ctx)
}

// Stop ferma il refresh in background.
func (c *RealMetricsCollector) Stop() {
	close(c.stopChan)
	c.stopWg.Wait()
}

// CollectMetrics ritorna le metriche correnti dalla cache.
func (c *RealMetricsCollector) CollectMetrics(ctx context.Context) (*placement.ClusterMetrics, error) {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()

	cacheAge := time.Since(c.cacheTimestamp)
	if cacheAge > c.config.CacheTTL {
		return nil, fmt.Errorf("metrics cache expired (age: %v, TTL: %v)", cacheAge, c.config.CacheTTL)
	}

	return c.cache, nil
}

// backgroundRefresh loop infinito per aggiornamento periodico metriche.
func (c *RealMetricsCollector) backgroundRefresh(ctx context.Context) {
	defer c.stopWg.Done()

	logger := log.FromContext(ctx)
	ticker := time.NewTicker(c.config.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.refresh(ctx); err != nil {
				logger.Error(err, "[WARN] Background metrics refresh failed")
			}
		case <-c.stopChan:
			logger.Info("[SHUTDOWN] Metrics collector stopped")
			return
		}
	}
}

// refresh raccoglie metriche fresche da tutti i cluster.
func (c *RealMetricsCollector) refresh(ctx context.Context) error {
	logger := log.FromContext(ctx)
	logger.V(1).Info("[DEBUG] Refreshing cluster metrics")

	newMetrics := placement.NewClusterMetrics()
	var wg sync.WaitGroup
	var mutex sync.Mutex

	// Raccolta parallela
	for clusterName, clusterClient := range c.clients {
		wg.Add(1)
		go func(name string, cli client.Client) {
			defer wg.Done()

			metric, err := c.collectClusterMetrics(ctx, name, cli)
			if err != nil {
				logger.Error(err, "[ERROR] Failed to collect metrics", "cluster", name)
				return
			}

			mutex.Lock()
			newMetrics.SetCluster(name, metric)
			mutex.Unlock()
		}(clusterName, clusterClient)
	}

	wg.Wait()

	// Misurazione latenze
	c.measureLatencies(ctx, newMetrics)

	// Aggiornamento atomico cache
	c.cacheMutex.Lock()
	c.cache = newMetrics
	c.cacheTimestamp = time.Now()
	c.cacheMutex.Unlock()

	logger.Info("[METRICS] Refreshed", "clusters", len(newMetrics.Clusters), "timestamp", c.cacheTimestamp.Format("15:04:05"))

	return nil
}

// collectClusterMetrics raccoglie metriche da un singolo cluster.
func (c *RealMetricsCollector) collectClusterMetrics(ctx context.Context, clusterName string, cli client.Client) (*placement.ClusterMetric, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, c.config.MetricsTimeout)
	defer cancel()

	logger := log.FromContext(ctx)

	// Lista nodi
	nodeList := &corev1.NodeList{}
	if err := cli.List(timeoutCtx, nodeList); err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}
	if len(nodeList.Items) == 0 {
		return nil, fmt.Errorf("no nodes found")
	}

	// Calcola capacità totale (inline)
	var totalCPUCapacity, totalMemoryCapacity int64
	for _, node := range nodeList.Items {
		cpuCap := node.Status.Capacity[corev1.ResourceCPU]
		memCap := node.Status.Capacity[corev1.ResourceMemory]
		totalCPUCapacity += cpuCap.MilliValue()
		totalMemoryCapacity += memCap.Value()
	}

	// Lista pod
	podList := &corev1.PodList{}
	if err := cli.List(timeoutCtx, podList); err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	// Calcola requests totali (inline, solo pod attivi)
	var totalCPURequested, totalMemoryRequested int64
	for _, pod := range podList.Items {
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

	// Calcola disponibilità
	totalCPUAvailable := totalCPUCapacity - totalCPURequested
	totalMemoryAvailable := totalMemoryCapacity - totalMemoryRequested

	// Opzionale: ottieni usage metrics per logging
	totalCPUUsed, totalMemoryUsed := c.getUsageMetrics(timeoutCtx, cli, clusterName, logger)

	logger.V(1).Info("[DEBUG] Cluster metrics",
		"cluster", clusterName,
		"nodes", len(nodeList.Items),
		"pods", len(podList.Items),
		"cpu", fmt.Sprintf("cap=%d req=%d used=%d avail=%d", totalCPUCapacity, totalCPURequested, totalCPUUsed, totalCPUAvailable),
		"mem", fmt.Sprintf("cap=%d req=%d used=%d avail=%d", totalMemoryCapacity, totalMemoryRequested, totalMemoryUsed, totalMemoryAvailable))

	return &placement.ClusterMetric{
		Name:            clusterName,
		CPUCapacity:     totalCPUCapacity,
		CPUUsed:         totalCPURequested,
		CPUAvailable:    totalCPUAvailable,
		MemoryCapacity:  totalMemoryCapacity,
		MemoryUsed:      totalMemoryRequested,
		MemoryAvailable: totalMemoryAvailable,
		Available:       true,
	}, nil
}

// getUsageMetrics raccoglie usage metrics (opzionale, solo per logging).
func (c *RealMetricsCollector) getUsageMetrics(ctx context.Context, cli client.Client, clusterName string, logger logr.Logger) (int64, int64) {
	nodeMetricsList := &metricsv1beta1.NodeMetricsList{}
	if err := cli.List(ctx, nodeMetricsList); err != nil {
		logger.V(1).Info("[DEBUG] Could not get usage metrics", "cluster", clusterName, "error", err)
		return 0, 0
	}

	var totalCPU, totalMemory int64
	for _, nodeMetrics := range nodeMetricsList.Items {
		cpuUsage := nodeMetrics.Usage[corev1.ResourceCPU]
		memUsage := nodeMetrics.Usage[corev1.ResourceMemory]
		totalCPU += cpuUsage.MilliValue()
		totalMemory += memUsage.Value()
	}

	return totalCPU, totalMemory
}

// measureLatencies misura la latenza di rete tra tutti i cluster.
func (c *RealMetricsCollector) measureLatencies(ctx context.Context, metrics *placement.ClusterMetrics) {
	logger := log.FromContext(ctx)

	for _, fromCluster := range clusterNames {
		fromMetric := metrics.GetCluster(fromCluster)
		if fromMetric == nil {
			continue
		}

		for _, toCluster := range clusterNames {
			if fromCluster == toCluster {
				continue
			}

			toClient, ok := c.clients[toCluster]
			if !ok {
				continue
			}

			latency := c.measureLatency(ctx, toClient)

			// --- AGGIUNGI QUESTA RIGA ---
			logger.Info("[LATENCY CHECK]", "From", fromCluster, "To", toCluster, "Latency(ms)", latency)
			// -----------------------------

			// Imposta il campo latenza appropriato (inline switch)
			switch toCluster {
			case edgeCluster1:
				fromMetric.LatencyToEdge1 = latency
			case edgeCluster2:
				fromMetric.LatencyToEdge2 = latency
			case edgeCluster3:
				fromMetric.LatencyToEdge3 = latency
			case cloudCluster:
				fromMetric.LatencyToCloud = latency
			}
		}
	}

	logger.V(1).Info("[DEBUG] Latency measurements completed")
}

// measureLatency misura la latenza di rete verso un cluster.
func (c *RealMetricsCollector) measureLatency(ctx context.Context, cli client.Client) int64 {
	timeoutCtx, cancel := context.WithTimeout(ctx, c.config.LatencyTimeout)
	defer cancel()

	start := time.Now()

	// Query leggera all'API server
	namespace := &corev1.Namespace{}
	err := cli.Get(timeoutCtx, client.ObjectKey{Name: kubeSystemNS}, namespace)

	if err != nil {
		return unreachableLatency
	}

	return time.Since(start).Milliseconds()
}
