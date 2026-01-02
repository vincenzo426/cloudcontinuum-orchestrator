package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
)

// ============================================================================
// STRUTTURA DEL COLLECTOR
// ============================================================================

// RealMetricsCollector implementa il Collector usando metriche reali dai cluster Kubernetes.
// Raccoglie periodicamente dati su CPU, memoria e latenza da tutti i cluster configurati,
// mantenendo una cache aggiornata per evitare query frequenti all'API server.
//
// Caratteristiche principali:
//   - Refresh periodico automatico in background
//   - Cache thread-safe con TTL configurabile
//   - Raccolta parallela da cluster multipli
//   - Calcolo basato su resource requests
//   - Misurazione latenze cross-cluster
type RealMetricsCollector struct {
	config  *Config                  // Configurazione del collector (intervalli, timeout)
	clients map[string]client.Client // Client Kubernetes per ogni cluster

	// Cache delle metriche con protezione concorrenza
	cache          *placement.ClusterMetrics // Ultima snapshot delle metriche
	cacheTimestamp time.Time                 // Timestamp dell'ultimo refresh
	cacheMutex     sync.RWMutex              // Mutex per accesso thread-safe

	// Controllo background refresh
	stopChan chan struct{}  // Canale per segnalare lo stop
	stopWg   sync.WaitGroup // WaitGroup per shutdown graceful
}

// ============================================================================
// COSTRUTTORE
// ============================================================================

// NewRealMetricsCollector crea un nuovo collector di metriche reali.
//
// Parametri:
//   - clients: mappa clusterName -> client Kubernetes per accesso ai cluster
//   - config: configurazione opzionale (usa DefaultConfig se nil)
//
// Ritorna un RealMetricsCollector configurato ma non ancora avviato.
// Chiamare Start() per iniziare la raccolta periodica.
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

// ============================================================================
// LIFECYCLE MANAGEMENT
// ============================================================================

// Start avvia il refresh periodico delle metriche in background.
// Esegue un primo refresh immediato, poi continua periodicamente secondo config.RefreshInterval.
//
// Parametri:
//   - ctx: context per logging (non per cancellazione - usa Stop() per quello)
//
// Note:
//   - Non bloccante: ritorna immediatamente dopo aver avviato la goroutine
//   - Il primo refresh è sincrono per garantire cache iniziale
//   - Errori nel refresh non bloccano l'avvio ma vengono loggati
func (c *RealMetricsCollector) Start(ctx context.Context) {
	logger := log.FromContext(ctx)
	logger.Info("🚀 Starting real metrics collector",
		"refreshInterval", c.config.RefreshInterval,
		"clusters", len(c.clients))

	// Esegui primo refresh immediato per popolare la cache
	if err := c.refresh(ctx); err != nil {
		logger.Error(err, "⚠️  Initial metrics refresh failed")
	}

	// Avvia goroutine per refresh periodico in background
	c.stopWg.Add(1)
	go c.backgroundRefresh(ctx)
}

// Stop ferma il refresh in background e attende il completamento..
//
// Note:
//   - Bloccante: ritorna solo dopo che la goroutine è terminata
//   - Dovrebbe essere chiamato durante il cleanup dell'applicazione
func (c *RealMetricsCollector) Stop() {
	close(c.stopChan)
	c.stopWg.Wait()
}

// ============================================================================
// RACCOLTA METRICHE - API PUBBLICA
// ============================================================================

// CollectMetrics ritorna le metriche correnti dalla cache.
// Questo metodo è thread-safe e molto veloce (no I/O, solo lettura da cache).
//
// Parametri:
//   - ctx: context (attualmente non utilizzato ma mantenuto per interfaccia)
//
// Ritorna:
//   - *placement.ClusterMetrics: snapshot corrente delle metriche
//   - error: errore se la cache è scaduta oltre il TTL configurato
//
// Note:
//   - Non esegue raccolta attiva: ritorna solo dati cached
//   - Cache aggiornata in background da backgroundRefresh()
//   - TTL configurabile via Config.CacheTTL
func (c *RealMetricsCollector) CollectMetrics(ctx context.Context) (*placement.ClusterMetrics, error) {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()

	// Verifica validità della cache
	cacheAge := time.Since(c.cacheTimestamp)
	if cacheAge > c.config.CacheTTL {
		return nil, fmt.Errorf("metrics cache expired (age: %v, TTL: %v)",
			cacheAge, c.config.CacheTTL)
	}

	return c.cache, nil
}

// ============================================================================
// BACKGROUND REFRESH
// ============================================================================

// backgroundRefresh loop infinito che aggiorna periodicamente le metriche.
// Eseguito in goroutine separata, gestisce il refresh automatico della cache.
//
// Parametri:
//   - ctx: context per logging (non per cancellazione)
//
// Comportamento:
//   - Esegue refresh ogni config.RefreshInterval
//   - Termina quando stopChan viene chiuso (via Stop())
//   - Errori nel refresh vengono loggati ma non fermano il loop
func (c *RealMetricsCollector) backgroundRefresh(ctx context.Context) {
	defer c.stopWg.Done()

	logger := log.FromContext(ctx)
	ticker := time.NewTicker(c.config.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Tick periodico: esegui refresh
			if err := c.refresh(ctx); err != nil {
				logger.Error(err, "⚠️  Background metrics refresh failed")
			}

		case <-c.stopChan:
			// Richiesta di stop: termina gracefully
			logger.Info("✅ Stopping metrics collector")
			return
		}
	}
}

// ============================================================================
// REFRESH LOGIC - ORCHESTRAZIONE
// ============================================================================

// refresh raccoglie metriche fresche da tutti i cluster.
// Questo è il cuore del collector: coordina la raccolta parallela e aggiorna la cache.
//
// Parametri:
//   - ctx: context per logging e timeout
//
// Ritorna:
//   - error: sempre nil attualmente (errori per-cluster loggati ma non propagati)
//
// Flusso operativo:
//  1. Crea nuova struttura ClusterMetrics vuota
//  2. Raccoglie metriche da ogni cluster in parallelo (goroutine separate)
//  3. Cluster con errori vengono esclusi dalla snapshot
//  4. Misura latenze cross-cluster tra tutti i cluster disponibili
//  5. Aggiorna cache atomicamente con lock
//
// Note:
//   - Raccolta parallela per minimizzare tempo totale
//   - Errori per-cluster non bloccano gli altri
//   - Cache sempre consistente (aggiornamento atomico)
func (c *RealMetricsCollector) refresh(ctx context.Context) error {
	logger := log.FromContext(ctx)
	logger.V(1).Info("📊 Refreshing cluster metrics")

	newMetrics := placement.NewClusterMetrics()

	// ========================================================================
	// FASE 1: Raccolta parallela da tutti i cluster
	// ========================================================================
	var wg sync.WaitGroup
	var mutex sync.Mutex // Protegge accesso concorrente a newMetrics

	for clusterName, clusterClient := range c.clients {
		wg.Add(1)

		// Lancia goroutine per ogni cluster
		go func(name string, cli client.Client) {
			defer wg.Done()

			// Raccogli metriche da questo cluster
			metric, err := c.collectClusterMetrics(ctx, name, cli)
			if err != nil {
				logger.Error(err, "❌ Failed to collect metrics from cluster",
					"cluster", name)
				// Cluster non disponibile: escluso dalla snapshot
				return
			}

			// Aggiungi metriche alla snapshot (thread-safe)
			mutex.Lock()
			newMetrics.SetCluster(name, metric)
			mutex.Unlock()
		}(clusterName, clusterClient)
	}

	// Attendi completamento di tutte le goroutine
	wg.Wait()

	// ========================================================================
	// FASE 2: Misurazione latenze cross-cluster
	// ========================================================================
	c.measureLatencies(ctx, newMetrics)

	// ========================================================================
	// FASE 3: Aggiornamento atomico della cache
	// ========================================================================
	c.cacheMutex.Lock()
	c.cache = newMetrics
	c.cacheTimestamp = time.Now()
	c.cacheMutex.Unlock()

	logger.Info("✅ Metrics refresh completed",
		"clusters", len(newMetrics.Clusters),
		"timestamp", c.cacheTimestamp)

	return nil
}

// ============================================================================
// RACCOLTA METRICHE - SINGOLO CLUSTER
// ============================================================================

// collectClusterMetrics raccoglie metriche complete da un singolo cluster.
// Implementa la logica di calcolo basata su resource requests, simulando
// il comportamento del Kubernetes scheduler.
//
// Parametri:
//   - ctx: context per timeout e logging
//   - clusterName: nome identificativo del cluster
//   - cli: client Kubernetes per accesso al cluster
//
// Ritorna:
//   - *placement.ClusterMetric: metriche aggregate del cluster
//   - error: errore in caso di fallimento (cluster unreachable, API errors, etc.)
//
// Flusso operativo:
//  1. Lista tutti i nodi del cluster
//  2. Calcola capacità totale (somma di tutti i nodi)
//  3. Lista tutti i pod attivi
//  4. Somma resource REQUESTS di tutti i pod
//  5. Calcola disponibilità: capacity - requests
//  6. Opzionalmente raccoglie usage per logging
//
// Note critiche:
//
//   - Skip pod terminati (Succeeded/Failed)
//   - Timeout configurabile via config.MetricsTimeout
//   - Usage metrics opzionale (richiede metrics-server)
func (c *RealMetricsCollector) collectClusterMetrics(
	ctx context.Context,
	clusterName string,
	cli client.Client,
) (*placement.ClusterMetric, error) {

	// Applica timeout per evitare blocchi su cluster lenti
	timeoutCtx, cancel := context.WithTimeout(ctx, c.config.MetricsTimeout)
	defer cancel()

	logger := log.FromContext(ctx)

	// ========================================================================
	// STEP 1: Ottieni lista di tutti i nodi
	// ========================================================================
	nodeList := &corev1.NodeList{}
	if err := cli.List(timeoutCtx, nodeList); err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}

	if len(nodeList.Items) == 0 {
		return nil, fmt.Errorf("no nodes found in cluster")
	}

	// ========================================================================
	// STEP 2: Calcola capacità totale sommando tutti i nodi
	// ========================================================================
	var totalCPUCapacity, totalMemoryCapacity int64

	for _, node := range nodeList.Items {
		cpuCap := node.Status.Capacity[corev1.ResourceCPU]
		memCap := node.Status.Capacity[corev1.ResourceMemory]

		totalCPUCapacity += cpuCap.MilliValue() // CPU in millicores
		totalMemoryCapacity += memCap.Value()   // Memory in bytes
	}

	// ========================================================================
	// STEP 3: Ottieni lista di TUTTI i pod nel cluster
	// ========================================================================
	podList := &corev1.PodList{}
	if err := cli.List(timeoutCtx, podList); err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	// ========================================================================
	// STEP 4: Somma resource REQUESTS (non usage!) di tutti i pod attivi
	// ========================================================================
	var totalCPURequested, totalMemoryRequested int64

	for _, pod := range podList.Items {
		// Skippa pod terminati (non consumano risorse)
		if pod.Status.Phase == corev1.PodSucceeded ||
			pod.Status.Phase == corev1.PodFailed {
			continue
		}

		// Somma requests di tutti i container nel pod
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

	// ========================================================================
	// STEP 5: Ottieni usage metrics (opzionale, solo per logging)
	// ========================================================================
	// Nota: Usage è diverso da requests! Usage è consumo reale al momento,
	// ma NON è quello che lo scheduler usa per placement decisions.
	var totalCPUUsed, totalMemoryUsed int64
	nodeMetricsList := &metricsv1beta1.NodeMetricsList{}

	if err := cli.List(timeoutCtx, nodeMetricsList); err != nil {
		logger.V(1).Info("⚠️  Could not get node metrics (usage), using requests only",
			"cluster", clusterName, "error", err)
	} else {
		for _, nodeMetrics := range nodeMetricsList.Items {
			cpuUsage := nodeMetrics.Usage[corev1.ResourceCPU]
			memUsage := nodeMetrics.Usage[corev1.ResourceMemory]
			totalCPUUsed += cpuUsage.MilliValue()
			totalMemoryUsed += memUsage.Value()
		}
	}

	// ========================================================================
	// STEP 6: Calcola disponibilità (capacity - requests)
	// ========================================================================
	// FONDAMENTALE: Kubernetes scheduler usa REQUESTS per decidere placement
	totalCPUAvailable := totalCPUCapacity - totalCPURequested
	totalMemoryAvailable := totalMemoryCapacity - totalMemoryRequested

	// Log dettagliato per debugging e monitoring
	logger.V(1).Info("📊 Cluster metrics collected",
		"cluster", clusterName,
		"nodes", len(nodeList.Items),
		"pods", len(podList.Items),
		"cpuCapacity", totalCPUCapacity,
		"cpuRequested", totalCPURequested,
		"cpuUsed", totalCPUUsed,
		"cpuAvailable", totalCPUAvailable,
		"memoryCapacity", totalMemoryCapacity,
		"memoryRequested", totalMemoryRequested,
		"memoryUsed", totalMemoryUsed,
		"memoryAvailable", totalMemoryAvailable)

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

// ============================================================================
// MISURAZIONE LATENZE
// ============================================================================

// measureLatencies misura la latenza di rete tra tutti i cluster.
// Aggiorna le metriche esistenti con informazioni sulla latenza cross-cluster,
// utile per strategie di placement che considerano la località dei dati.
//
// Parametri:
//   - ctx: context per timeout e logging
//   - metrics: struttura ClusterMetrics da arricchire con latenze
//
// Comportamento:
//   - Per ogni coppia di cluster (A, B), misura latenza da A a B
//   - Latenza verso se stesso = 0 (skip)
//   - Usa ping leggero all'API server (query namespace kube-system)
//   - Salva latenza nei campi appropriati (LatencyToEdge1, LatencyToCloud, etc.)
//
// Note:
//   - Timeout configurabile via config.LatencyTimeout
//   - Errori producono latenza molto alta (9999ms) per penalizzare cluster unreachable
func (c *RealMetricsCollector) measureLatencies(
	ctx context.Context,
	metrics *placement.ClusterMetrics,
) {
	logger := log.FromContext(ctx)

	// Lista di tutti i cluster da testare
	clusterNames := []string{
		"cloud_cluster",
		"edge_cluster_1",
		"edge_cluster_2",
		"edge_cluster_3",
	}

	// Per ogni coppia di cluster (from, to)
	for _, fromCluster := range clusterNames {
		fromMetric := metrics.GetCluster(fromCluster)
		if fromMetric == nil {
			continue // Cluster non disponibile in questa snapshot
		}

		for _, toCluster := range clusterNames {
			// Skip latenza verso se stesso
			if fromCluster == toCluster {
				continue
			}

			// Ottieni client per cluster target
			toClient, ok := c.clients[toCluster]
			if !ok {
				continue
			}

			// Misura latency con HTTP ping
			latency := c.measureLatency(ctx, toClient)

			// Salva nella metrica appropriata del cluster source
			switch toCluster {
			case "edge_cluster_1":
				fromMetric.LatencyToEdge1 = latency
			case "edge_cluster_2":
				fromMetric.LatencyToEdge2 = latency
			case "edge_cluster_3":
				fromMetric.LatencyToEdge3 = latency
			case "cloud_cluster":
				fromMetric.LatencyToCloud = latency
			}
		}
	}

	logger.V(1).Info("📡 Latency measurements completed")
}

// measureLatency misura la latenza di rete verso un cluster specifico.
// Implementa un ping leggero via query API server.
//
// Parametri:
//   - ctx: context per timeout
//   - cli: client Kubernetes verso il cluster target
//
// Ritorna:
//   - int64: latenza in millisecondi (9999 se errore/timeout)
//
// Implementazione:
//   - Esegue GET del namespace "kube-system" (leggero, sempre presente)
//   - Misura tempo round-trip con time.Since()
//   - Timeout configurabile via config.LatencyTimeout
//
// Note:
//   - 9999ms indica cluster unreachable o errore
//   - Latenza include: network RTT + API server response time
func (c *RealMetricsCollector) measureLatency(
	ctx context.Context,
	cli client.Client,
) int64 {

	// Applica timeout per evitare blocchi
	timeoutCtx, cancel := context.WithTimeout(ctx, c.config.LatencyTimeout)
	defer cancel()

	// Timestamp di inizio
	start := time.Now()

	// Query leggera all'API server: GET namespace kube-system
	namespace := &corev1.Namespace{}
	err := cli.Get(timeoutCtx, client.ObjectKey{Name: "kube-system"}, namespace)

	// Calcola tempo trascorso
	elapsed := time.Since(start)

	if err != nil {
		// Errore o timeout: ritorna latenza molto alta per penalizzare
		return 9999
	}

	return elapsed.Milliseconds()
}
