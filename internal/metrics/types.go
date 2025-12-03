package metrics

import (
	"context"
	"time"

	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/placement"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Collector definisce l'interfaccia per la raccolta metriche
type Collector interface {
	// CollectMetrics raccoglie metriche aggiornate da tutti i cluster
	CollectMetrics(ctx context.Context) (*placement.ClusterMetrics, error)

	// Start avvia il refresh periodico in background
	Start(ctx context.Context)

	// Stop ferma il refresh in background
	Stop()
}

// ClusterClient rappresenta un client per un cluster specifico
type ClusterClient struct {
	Name   string
	Client client.Client
}

// Config contiene la configurazione del collector
type Config struct {
	// RefreshInterval è l'intervallo tra refresh successivi
	RefreshInterval time.Duration

	// LatencyTimeout è il timeout per le misurazioni di latenza
	LatencyTimeout time.Duration

	// MetricsTimeout è il timeout per le query alle metrics API
	MetricsTimeout time.Duration

	// CacheTTL è il tempo di validità delle metriche cached
	CacheTTL time.Duration
}

// DefaultConfig ritorna una configurazione di default
func DefaultConfig() *Config {
	return &Config{
		RefreshInterval: 30 * time.Second,
		LatencyTimeout:  5 * time.Second,
		MetricsTimeout:  10 * time.Second,
		CacheTTL:        60 * time.Second,
	}
}
