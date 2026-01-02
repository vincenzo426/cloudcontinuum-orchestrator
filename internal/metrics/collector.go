package metrics

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sync"
	"time"

	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

// Costanti di configurazione
const (
	// Latenza default per cluster unreachable (millisecondi)
	unreachableLatency = 9999

	// Nomi cluster standard
	cloudCluster = "cloud_cluster"
	edgeCluster1 = "edge_cluster_1"
	edgeCluster2 = "edge_cluster_2"
	edgeCluster3 = "edge_cluster_3"
	kubeSystemNS = "kube-system"
)

// RealMetricsCollector raccoglie metriche reali dai cluster Kubernetes.
// Mantiene una cache aggiornata periodicamente in background.
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
	logger.Info("🚀 Starting metrics collector",
		"refreshInterval", c.config.RefreshInterval,
		"clusters", len(c.clients))

	// Primo refresh sincrono per popolare cache iniziale
	if err := c.refresh(ctx); err != nil {
		logger.Error(err, "⚠️  Initial metrics refresh failed")
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
				logger.Error(err, "⚠️  Background metrics refresh failed")
			}

		case <-c.stopChan:
			logger.Info("✅ Stopping metrics collector")
			return
		}
	}
}

// refresh raccoglie metriche fresche da tutti i cluster.
func (c *RealMetricsCollector) refresh(ctx context.Context) error {
	logger := log.FromContext(ctx)
	logger.V(1).Info("📊 Refreshing cluster metrics")

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
				logger.Error(err, "❌ Failed to collect metrics", "cluster", name)
				return
			}

			mutex.Lock()
			newMetrics.SetCluster(name, metric)
			mutex.Unlock()
		}(clusterName, clusterClient)
	}

	wg.Wait()

	// Misurazione latenze cross-cluster
	c.measureLatencies(ctx, newMetrics)

	// Aggiornamento atomico cache
	c.cacheMutex.Lock()
	c.cache = newMetrics
	c.cacheTimestamp = time.Now()
	c.cacheMutex.Unlock()

	logger.Info("✅ Metrics refreshed", "clusters", len(newMetrics.Clusters), "timestamp", c.cacheTimestamp)

	return nil
}

// collectClusterMetrics raccoglie metriche da un singolo cluster.
// Calcola disponibilità basandosi su resource requests (non usage).
func (c *RealMetricsCollector) collectClusterMetrics(
	ctx context.Context,
	clusterName string,
	cli client.Client,
) (*placement.ClusterMetric, error) {

	timeoutCtx, cancel := context.WithTimeout(ctx, c.config.MetricsTimeout)
	defer cancel()

	logger := log.FromContext(ctx)

	// Lista nodi
	nodeList := &corev1.NodeList{}
	if err := cli.List(timeoutCtx, nodeList); err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}

	if len(nodeList.Items) == 0 {
		return nil, fmt.Errorf("no nodes found in cluster")
	}

	// Calcola capacità totale
	totalCPUCapacity, totalMemoryCapacity := c.calculateTotalCapacity(nodeList)

	// Lista tutti i pod
	podList := &corev1.PodList{}
	if err := cli.List(timeoutCtx, podList); err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	// Calcola requests totali (solo pod attivi)
	totalCPURequested, totalMemoryRequested := c.calculateTotalRequests(podList)

	// Opzionale: ottieni usage metrics per logging
	totalCPUUsed, totalMemoryUsed := c.getTotalUsage(timeoutCtx, cli, clusterName, logger)

	// Calcola disponibilità (capacity - requests)
	totalCPUAvailable := totalCPUCapacity - totalCPURequested
	totalMemoryAvailable := totalMemoryCapacity - totalMemoryRequested

	logger.V(1).Info("📊 Cluster metrics",
		"cluster", clusterName,
		"nodes", len(nodeList.Items),
		"pods", len(podList.Items),
		"cpu", fmt.Sprintf("cap=%d req=%d used=%d avail=%d",
			totalCPUCapacity, totalCPURequested, totalCPUUsed, totalCPUAvailable),
		"mem", fmt.Sprintf("cap=%d req=%d used=%d avail=%d",
			totalMemoryCapacity, totalMemoryRequested, totalMemoryUsed, totalMemoryAvailable))

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

// calculateTotalCapacity somma la capacità di tutti i nodi.
func (c *RealMetricsCollector) calculateTotalCapacity(nodeList *corev1.NodeList) (int64, int64) {
	var totalCPU, totalMemory int64

	for _, node := range nodeList.Items {
		cpuCap := node.Status.Capacity[corev1.ResourceCPU]
		memCap := node.Status.Capacity[corev1.ResourceMemory]

		totalCPU += cpuCap.MilliValue()
		totalMemory += memCap.Value()
	}

	return totalCPU, totalMemory
}

// calculateTotalRequests somma i resource requests di tutti i pod attivi.
func (c *RealMetricsCollector) calculateTotalRequests(podList *corev1.PodList) (int64, int64) {
	var totalCPU, totalMemory int64

	for _, pod := range podList.Items {
		// Skip pod terminati
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}

		for _, container := range pod.Spec.Containers {
			if container.Resources.Requests != nil {
				if cpuReq, ok := container.Resources.Requests[corev1.ResourceCPU]; ok {
					totalCPU += cpuReq.MilliValue()
				}
				if memReq, ok := container.Resources.Requests[corev1.ResourceMemory]; ok {
					totalMemory += memReq.Value()
				}
			}
		}
	}

	return totalCPU, totalMemory
}

// getTotalUsage raccoglie usage metrics (opzionale, solo per logging).
func (c *RealMetricsCollector) getTotalUsage(
	ctx context.Context,
	cli client.Client,
	clusterName string,
	logger logr.Logger,
) (int64, int64) {
	var totalCPU, totalMemory int64

	nodeMetricsList := &metricsv1beta1.NodeMetricsList{}
	if err := cli.List(ctx, nodeMetricsList); err != nil {
		logger.V(1).Info("⚠️  Could not get usage metrics", "cluster", clusterName, "error", err)
		return 0, 0
	}

	for _, nodeMetrics := range nodeMetricsList.Items {
		cpuUsage := nodeMetrics.Usage[corev1.ResourceCPU]
		memUsage := nodeMetrics.Usage[corev1.ResourceMemory]
		totalCPU += cpuUsage.MilliValue()
		totalMemory += memUsage.Value()
	}

	return totalCPU, totalMemory
}

// measureLatencies misura la latenza di rete tra tutti i cluster.
func (c *RealMetricsCollector) measureLatencies(
	ctx context.Context,
	metrics *placement.ClusterMetrics,
) {
	logger := log.FromContext(ctx)

	clusterNames := []string{cloudCluster, edgeCluster1, edgeCluster2, edgeCluster3}

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
			c.setLatencyField(fromMetric, toCluster, latency)
		}
	}

	logger.V(1).Info("📡 Latency measurements completed")
}

// setLatencyField imposta il campo latenza appropriato nella metrica.
func (c *RealMetricsCollector) setLatencyField(metric *placement.ClusterMetric, toCluster string, latency int64) {
	switch toCluster {
	case edgeCluster1:
		metric.LatencyToEdge1 = latency
	case edgeCluster2:
		metric.LatencyToEdge2 = latency
	case edgeCluster3:
		metric.LatencyToEdge3 = latency
	case cloudCluster:
		metric.LatencyToCloud = latency
	}
}

// measureLatency misura la latenza di rete verso un cluster.
// Ritorna 9999ms se unreachable o errore.
func (c *RealMetricsCollector) measureLatency(
	ctx context.Context,
	cli client.Client,
) int64 {

	timeoutCtx, cancel := context.WithTimeout(ctx, c.config.LatencyTimeout)
	defer cancel()

	start := time.Now()

	// Query leggera all'API server
	namespace := &corev1.Namespace{}
	err := cli.Get(timeoutCtx, client.ObjectKey{Name: kubeSystemNS}, namespace)

	elapsed := time.Since(start)

	if err != nil {
		return unreachableLatency
	}

	return elapsed.Milliseconds()
}
