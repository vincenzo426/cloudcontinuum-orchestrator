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

	// === RESERVATION SETTINGS ===
	// Quanto dura una reservation prima di scadere automaticamente.
	// Deve essere abbastanza lungo da coprire il tempo necessario affinché
	// i Pod diventino visibili nelle metriche K8s (tipicamente 10-30s).
	defaultReservationTTL = 60 * time.Second

	// Ogni quanto pulire le reservation scadute
	cleanupInterval = 15 * time.Second
)

var clusterNames = []string{cloudCluster, edgeCluster1, edgeCluster2, edgeCluster3}

// =============================================================================
// IN-FLIGHT RESERVATION
// =============================================================================

// InFlightReservation rappresenta risorse riservate per una pipeline
// che è stata schedulata ma i cui Pod non sono ancora visibili nelle metriche K8s.
//
// Questo risolve il problema dell'over-commit: quando l'agente RL decide di
// piazzare una pipeline, i Pod non sono immediatamente visibili (possono volerci
// 5-30 secondi). Senza reservation, l'agente potrebbe schedulare altre pipeline
// sullo stesso cluster pensando che abbia ancora risorse disponibili.
type InFlightReservation struct {
	PipelineName string    // Nome della pipeline (per logging/debugging)
	ClusterName  string    // Cluster su cui è stata schedulata
	CPURequested int64     // millicores riservati
	MemRequested int64     // bytes riservati
	CreatedAt    time.Time // Quando è stata creata la reservation
	ExpiresAt    time.Time // Quando scade (auto-cleanup)
}

// =============================================================================
// REAL METRICS COLLECTOR
// =============================================================================

// RealMetricsCollector raccoglie metriche reali dai cluster Kubernetes.
type RealMetricsCollector struct {
	config  *Config
	clients map[string]client.Client

	cache          *placement.ClusterMetrics
	cacheTimestamp time.Time
	cacheMutex     sync.RWMutex

	// === IN-FLIGHT RESERVATIONS ===
	// Mappa: pipelineName -> reservation
	inFlightReservations map[string]*InFlightReservation
	reservationsMutex    sync.RWMutex

	stopChan chan struct{}
	stopWg   sync.WaitGroup
}

// NewRealMetricsCollector crea un nuovo collector di metriche.
func NewRealMetricsCollector(clients map[string]client.Client, config *Config) *RealMetricsCollector {
	if config == nil {
		config = DefaultConfig()
	}

	return &RealMetricsCollector{
		config:               config,
		clients:              clients,
		cache:                placement.NewClusterMetrics(),
		inFlightReservations: make(map[string]*InFlightReservation),
		stopChan:             make(chan struct{}),
	}
}

// Start avvia il refresh periodico delle metriche in background.
func (c *RealMetricsCollector) Start(ctx context.Context) {
	logger := log.FromContext(ctx)
	logger.Info("[METRICS] Starting collector",
		"refreshInterval", c.config.RefreshInterval,
		"clusters", len(c.clients),
		"reservationTTL", defaultReservationTTL)

	// Primo refresh sincrono
	if err := c.refresh(ctx); err != nil {
		logger.Error(err, "[WARN] Initial metrics refresh failed")
	}

	// Avvia background refresh
	c.stopWg.Add(1)
	go c.backgroundRefresh(ctx)

	// Avvia cleanup periodico delle reservation scadute
	c.stopWg.Add(1)
	go c.reservationCleanupLoop(ctx)
}

// Stop ferma il refresh in background.
func (c *RealMetricsCollector) Stop() {
	close(c.stopChan)
	c.stopWg.Wait()
}

// =============================================================================
// IN-FLIGHT RESERVATIONS - PUBLIC API
// =============================================================================

// AddReservation registra una reservation per una pipeline appena schedulata.
//
// QUANDO CHIAMARLA: Subito dopo che il modello RL decide il cluster target,
// PRIMA che i Pod vengano creati su Kubeflow.
//
// PERCHÉ: Previene l'over-commit. Le prossime decisioni vedranno queste risorse
// come già utilizzate, anche se i Pod non sono ancora visibili in K8s.
//
// Esempio di utilizzo nel controller:
//
//	targetCluster, decision, err := strategy.SelectCluster(...)
//	if err == nil {
//	    metricsCollector.AddReservation(ppr.Name, targetCluster, totalResources.TotalCPU, totalResources.TotalMemory)
//	}
func (c *RealMetricsCollector) AddReservation(pipelineName, clusterName string, cpuMillicores, memoryBytes int64) {
	c.reservationsMutex.Lock()
	defer c.reservationsMutex.Unlock()

	now := time.Now()
	reservation := &InFlightReservation{
		PipelineName: pipelineName,
		ClusterName:  clusterName,
		CPURequested: cpuMillicores,
		MemRequested: memoryBytes,
		CreatedAt:    now,
		ExpiresAt:    now.Add(defaultReservationTTL),
	}

	c.inFlightReservations[pipelineName] = reservation

	// Log per debugging/monitoring
	log.Log.Info("[RESERVATION] Added",
		"pipeline", pipelineName,
		"cluster", clusterName,
		"cpu", fmt.Sprintf("%dm", cpuMillicores),
		"memory", fmt.Sprintf("%dMB", memoryBytes/(1024*1024)),
		"expiresIn", defaultReservationTTL.String(),
		"totalActiveReservations", len(c.inFlightReservations))
}

// RemoveReservation rimuove una reservation.
//
// QUANDO CHIAMARLA:
// - Quando i Pod diventano visibili nelle metriche K8s
// - Quando la pipeline fallisce o viene cancellata
// - Quando la pipeline completa l'esecuzione
//
// NOTA: Le reservation scadono automaticamente dopo defaultReservationTTL,
// quindi chiamare RemoveReservation è opzionale ma consigliato per
// liberare risorse appena possibile.
func (c *RealMetricsCollector) RemoveReservation(pipelineName string) {
	c.reservationsMutex.Lock()
	defer c.reservationsMutex.Unlock()

	if res, exists := c.inFlightReservations[pipelineName]; exists {
		age := time.Since(res.CreatedAt)
		delete(c.inFlightReservations, pipelineName)

		log.Log.Info("[RESERVATION] Removed",
			"pipeline", pipelineName,
			"cluster", res.ClusterName,
			"age", age.String(),
			"remainingReservations", len(c.inFlightReservations))
	}
}

// GetReservationsForCluster ritorna le risorse totali riservate per un cluster.
// Utile per debugging/monitoring.
func (c *RealMetricsCollector) GetReservationsForCluster(clusterName string) (cpuMillicores, memoryBytes int64) {
	c.reservationsMutex.RLock()
	defer c.reservationsMutex.RUnlock()

	now := time.Now()
	for _, res := range c.inFlightReservations {
		if now.After(res.ExpiresAt) {
			continue // Scaduta, ignora
		}
		if res.ClusterName == clusterName {
			cpuMillicores += res.CPURequested
			memoryBytes += res.MemRequested
		}
	}
	return
}

// GetActiveReservationsCount ritorna il numero di reservation attive (non scadute).
func (c *RealMetricsCollector) GetActiveReservationsCount() int {
	c.reservationsMutex.RLock()
	defer c.reservationsMutex.RUnlock()

	count := 0
	now := time.Now()
	for _, res := range c.inFlightReservations {
		if now.Before(res.ExpiresAt) {
			count++
		}
	}
	return count
}

// =============================================================================
// IN-FLIGHT RESERVATIONS - INTERNAL
// =============================================================================

// reservationCleanupLoop pulisce periodicamente le reservation scadute.
func (c *RealMetricsCollector) reservationCleanupLoop(ctx context.Context) {
	defer c.stopWg.Done()

	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanupExpiredReservations()
		case <-c.stopChan:
			return
		}
	}
}

// cleanupExpiredReservations rimuove le reservation scadute.
func (c *RealMetricsCollector) cleanupExpiredReservations() {
	c.reservationsMutex.Lock()
	defer c.reservationsMutex.Unlock()

	now := time.Now()
	expired := 0

	for name, res := range c.inFlightReservations {
		if now.After(res.ExpiresAt) {
			delete(c.inFlightReservations, name)
			expired++

			log.Log.Info("[RESERVATION] Expired and auto-removed",
				"pipeline", name,
				"cluster", res.ClusterName,
				"totalAge", now.Sub(res.CreatedAt).String())
		}
	}

	if expired > 0 {
		log.Log.Info("[RESERVATION] Cleanup completed",
			"expiredCount", expired,
			"remainingCount", len(c.inFlightReservations))
	}
}

// =============================================================================
// METRICS COLLECTION
// =============================================================================

// CollectMetrics ritorna le metriche correnti dalla cache (SENZA reservation).
// Per le decisioni di placement, usare GetAdjustedMetrics() che include le reservation.
func (c *RealMetricsCollector) CollectMetrics(ctx context.Context) (*placement.ClusterMetrics, error) {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()

	cacheAge := time.Since(c.cacheTimestamp)

	// Log per debugging
	logger := log.FromContext(ctx)
	logger.Info("[METRICS CACHE]",
		"age", cacheAge.String(),
		"ttl", c.config.CacheTTL.String(),
		"isExpired", cacheAge > c.config.CacheTTL,
		"activeReservations", c.GetActiveReservationsCount())

	if cacheAge > c.config.CacheTTL {
		return nil, fmt.Errorf("metrics cache expired (age: %v, TTL: %v)", cacheAge, c.config.CacheTTL)
	}

	return c.cache, nil
}

// GetAdjustedMetrics ritorna le metriche CON le reservation in-flight sottratte.
//
// QUESTA È LA FUNZIONE DA USARE PER LE DECISIONI DI PLACEMENT!
//
// Le metriche ritornate hanno già le risorse delle pipeline "in-flight"
// sottratte, quindi l'agente RL vede lo stato "reale" del cluster
// includendo le pipeline già schedulate ma non ancora visibili in K8s.
func (c *RealMetricsCollector) GetAdjustedMetrics(ctx context.Context) (*placement.ClusterMetrics, error) {
	// Prima ottieni le metriche raw dalla cache
	rawMetrics, err := c.CollectMetrics(ctx)
	if err != nil {
		return nil, err
	}

	// Crea una copia profonda per non modificare la cache originale
	adjustedMetrics := placement.NewClusterMetrics()

	c.reservationsMutex.RLock()
	defer c.reservationsMutex.RUnlock()

	now := time.Now()
	logger := log.FromContext(ctx)

	// Copia ogni cluster e sottrai le reservation attive
	for clusterName, rawMetric := range rawMetrics.Clusters {
		// Copia tutti i campi del metric
		adjustedMetric := &placement.ClusterMetric{
			Name:            rawMetric.Name,
			CPUCapacity:     rawMetric.CPUCapacity,
			CPUUsed:         rawMetric.CPUUsed,
			CPUAvailable:    rawMetric.CPUAvailable,
			MemoryCapacity:  rawMetric.MemoryCapacity,
			MemoryUsed:      rawMetric.MemoryUsed,
			MemoryAvailable: rawMetric.MemoryAvailable,
			Available:       rawMetric.Available,
			LatencyToCloud:  rawMetric.LatencyToCloud,
			LatencyToEdge1:  rawMetric.LatencyToEdge1,
			LatencyToEdge2:  rawMetric.LatencyToEdge2,
			LatencyToEdge3:  rawMetric.LatencyToEdge3,
		}

		// Calcola reservation totali per questo cluster
		var reservedCPU, reservedMem int64
		var reservationCount int
		var reservationPipelines []string

		for pipelineName, res := range c.inFlightReservations {
			if now.After(res.ExpiresAt) {
				continue // Scaduta, ignora
			}
			if res.ClusterName == clusterName {
				reservedCPU += res.CPURequested
				reservedMem += res.MemRequested
				reservationCount++
				reservationPipelines = append(reservationPipelines, pipelineName)
			}
		}

		// Sottrai le reservation dalle risorse disponibili
		if reservedCPU > 0 || reservedMem > 0 {
			adjustedMetric.CPUAvailable -= reservedCPU
			adjustedMetric.MemoryAvailable -= reservedMem
			adjustedMetric.CPUUsed += reservedCPU
			adjustedMetric.MemoryUsed += reservedMem

			// Assicura che non vadano sotto zero
			if adjustedMetric.CPUAvailable < 0 {
				adjustedMetric.CPUAvailable = 0
			}
			if adjustedMetric.MemoryAvailable < 0 {
				adjustedMetric.MemoryAvailable = 0
			}

			logger.Info("[METRICS] Adjusted for in-flight reservations",
				"cluster", clusterName,
				"reservationCount", reservationCount,
				"reservedCPU", fmt.Sprintf("%dm", reservedCPU),
				"reservedMem", fmt.Sprintf("%dMB", reservedMem/(1024*1024)),
				"pipelines", reservationPipelines,
				"rawCPUAvailable", fmt.Sprintf("%dm", rawMetric.CPUAvailable),
				"adjustedCPUAvailable", fmt.Sprintf("%dm", adjustedMetric.CPUAvailable))
		}

		adjustedMetrics.SetCluster(clusterName, adjustedMetric)
	}

	return adjustedMetrics, nil
}

// GetCacheTimestamp ritorna il timestamp dell'ultima raccolta metriche.
func (c *RealMetricsCollector) GetCacheTimestamp() time.Time {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()
	return c.cacheTimestamp
}

// GetMetricsAge ritorna l'età delle metriche in secondi.
func (c *RealMetricsCollector) GetMetricsAge() float64 {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()
	return time.Since(c.cacheTimestamp).Seconds()
}

// =============================================================================
// BACKGROUND REFRESH
// =============================================================================

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

	// Raccolta parallela da tutti i cluster
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

	logger.Info("[METRICS] Refreshed",
		"clusters", len(newMetrics.Clusters),
		"timestamp", c.cacheTimestamp.Format("15:04:05"),
		"activeReservations", c.GetActiveReservationsCount())

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

	// Calcola capacità totale
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

	// Calcola requests totali (solo pod attivi)
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

			logger.Info("[LATENCY CHECK]", "From", fromCluster, "To", toCluster, "Latency(ms)", latency)

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

	namespace := &corev1.Namespace{}
	err := cli.Get(timeoutCtx, client.ObjectKey{Name: kubeSystemNS}, namespace)

	if err != nil {
		return unreachableLatency
	}

	return time.Since(start).Milliseconds()
}
