package datatransfer

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	configMapName      = "data-transfer-config"
	configMapNamespace = "cloudcontinuum-system"
	defaultBandwidth   = 100.0 // Mbps
)

// TransferConfig holds bandwidth configuration between clusters
type TransferConfig struct {
	bandwidthMatrix  map[string]float64 // Key: "cluster1-cluster2", Value: Mbps
	defaultBandwidth float64
}

// Calculator handles data transfer time calculations
type Calculator struct {
	config *TransferConfig
}

// NewCalculator creates a new data transfer calculator
func NewCalculator(ctx context.Context, k8sClient client.Client) (*Calculator, error) {
	logger := log.FromContext(ctx)

	config, err := loadConfigFromConfigMap(ctx, k8sClient)
	if err != nil {
		logger.Error(err, "Failed to load data transfer config, using defaults")
		config = getDefaultConfig()
	}

	return &Calculator{
		config: config,
	}, nil
}

// loadConfigFromConfigMap loads bandwidth configuration from ConfigMap
func loadConfigFromConfigMap(ctx context.Context, k8sClient client.Client) (*TransferConfig, error) {
	cm := &corev1.ConfigMap{}
	err := k8sClient.Get(ctx, types.NamespacedName{
		Name:      configMapName,
		Namespace: configMapNamespace,
	}, cm)

	if err != nil {
		return nil, fmt.Errorf("failed to get configmap: %w", err)
	}

	config := &TransferConfig{
		bandwidthMatrix:  make(map[string]float64),
		defaultBandwidth: defaultBandwidth,
	}

	// Parse bandwidth values from ConfigMap
	for key, value := range cm.Data {
		if key == "default-bandwidth" {
			if bw, err := strconv.ParseFloat(value, 64); err == nil {
				config.defaultBandwidth = bw
			}
			continue
		}

		if bw, err := strconv.ParseFloat(value, 64); err == nil {
			config.bandwidthMatrix[key] = bw
		}
	}

	return config, nil
}

// getDefaultConfig returns a default configuration
func getDefaultConfig() *TransferConfig {
	return &TransferConfig{
		bandwidthMatrix: map[string]float64{
			// Cloud ↔ Edge: 100 Mbps
			"cloud_cluster-edge_cluster_1": 100,
			"edge_cluster_1-cloud_cluster": 100,
			"cloud_cluster-edge_cluster_2": 100,
			"edge_cluster_2-cloud_cluster": 100,
			"cloud_cluster-edge_cluster_3": 100,
			"edge_cluster_3-cloud_cluster": 100,

			// Edge ↔ Edge: 1 Gbps
			"edge_cluster_1-edge_cluster_2": 1000,
			"edge_cluster_2-edge_cluster_1": 1000,
			"edge_cluster_1-edge_cluster_3": 1000,
			"edge_cluster_3-edge_cluster_1": 1000,
			"edge_cluster_2-edge_cluster_3": 1000,
			"edge_cluster_3-edge_cluster_2": 1000,
		},
		defaultBandwidth: defaultBandwidth,
	}
}

// TransferResult contains the calculated transfer metrics
type TransferResult struct {
	NetworkLatency   int64   // milliseconds
	DataTransferTime int64   // milliseconds (latency + transfer)
	BandwidthUsed    float64 // Mbps
	DataSizeBytes    int64
	TransferDetails  string
}

// CalculateTransferTime computes the total data transfer time
func (c *Calculator) CalculateTransferTime(
	sourceCluster string,
	targetCluster string,
	dataSize string,
	networkLatency int64,
) (*TransferResult, error) {

	// If no data location or same cluster, no transfer needed
	if sourceCluster == "" || sourceCluster == targetCluster {
		return &TransferResult{
			NetworkLatency:   0,
			DataTransferTime: 0,
			TransferDetails:  "No data transfer required (same cluster or no data location specified)",
		}, nil
	}

	// Parse data size
	dataSizeBytes, err := parseDataSize(dataSize)
	if err != nil {
		return nil, fmt.Errorf("invalid data size format: %w", err)
	}

	// Get bandwidth for this route
	bandwidth := c.getBandwidth(sourceCluster, targetCluster)

	// Calculate transfer time
	// Time (ms) = (DataSize in bytes * 8 bits/byte) / (Bandwidth in Mbps * 1,000,000 bits/sec) * 1000 ms/sec
	dataSizeBits := float64(dataSizeBytes * 8)
	bandwidthBitsPerSec := bandwidth * 1_000_000
	transferTimeSeconds := dataSizeBits / bandwidthBitsPerSec
	transferTimeMs := int64(transferTimeSeconds * 1000)

	// Total time = network latency + actual transfer
	totalTimeMs := networkLatency + transferTimeMs

	details := fmt.Sprintf(
		"Data transfer from %s to %s: %s (%.2f MB) at %.0f Mbps. "+
			"Network latency: %d ms, Transfer time: %d ms, Total: %d ms",
		sourceCluster, targetCluster,
		dataSize, float64(dataSizeBytes)/(1024*1024),
		bandwidth,
		networkLatency, transferTimeMs, totalTimeMs,
	)

	return &TransferResult{
		NetworkLatency:   networkLatency,
		DataTransferTime: totalTimeMs,
		BandwidthUsed:    bandwidth,
		DataSizeBytes:    dataSizeBytes,
		TransferDetails:  details,
	}, nil
}

// getBandwidth returns the bandwidth between two clusters
func (c *Calculator) getBandwidth(source, target string) float64 {
	// Try direct lookup
	key := fmt.Sprintf("%s-%s", source, target)
	if bw, ok := c.config.bandwidthMatrix[key]; ok {
		return bw
	}

	// Try reverse lookup (symmetric)
	reverseKey := fmt.Sprintf("%s-%s", target, source)
	if bw, ok := c.config.bandwidthMatrix[reverseKey]; ok {
		return bw
	}

	// Return default
	return c.config.defaultBandwidth
}

// parseDataSize converts string like "5GB" or "500MB" to bytes
func parseDataSize(size string) (int64, error) {
	if size == "" {
		return 0, fmt.Errorf("empty data size")
	}

	size = strings.ToUpper(strings.TrimSpace(size))

	var multiplier int64
	var numStr string

	if strings.HasSuffix(size, "GB") {
		multiplier = 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(size, "GB")
	} else if strings.HasSuffix(size, "MB") {
		multiplier = 1024 * 1024
		numStr = strings.TrimSuffix(size, "MB")
	} else if strings.HasSuffix(size, "KB") {
		multiplier = 1024
		numStr = strings.TrimSuffix(size, "KB")
	} else if strings.HasSuffix(size, "B") {
		multiplier = 1
		numStr = strings.TrimSuffix(size, "B")
	} else {
		return 0, fmt.Errorf("unsupported size unit (use GB, MB, KB, or B)")
	}

	// Parse the numeric part (supports decimals like "2.5GB")
	num, err := strconv.ParseFloat(strings.TrimSpace(numStr), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid numeric value: %w", err)
	}

	return int64(num * float64(multiplier)), nil
}
