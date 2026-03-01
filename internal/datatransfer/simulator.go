/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package datatransfer

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/vincenzo426/cloudcontinuum-orchestrator/internal/multicluster"
)

const (
	// DataSinkServiceName is the name of the data sink service
	DataSinkServiceName = "data-sink"
	// DataSinkNamespace is the namespace where data sink runs
	DataSinkNamespace = "cloudcontinuum-system"
	// DataSinkPort is the port the data sink listens on
	DataSinkPort = 9999
	// TransferJobPrefix is the prefix for transfer job names
	TransferJobPrefix = "data-transfer"
	// SenderImage is the Docker image for the data sender job
	SenderImage = "alpine:3.19"
	// JobTimeout is the maximum time to wait for a transfer job
	JobTimeout = 30 * time.Minute
	// PollInterval is how often to check job status
	PollInterval = 2 * time.Second
)

// Simulator handles simulated data transfers between clusters
type Simulator struct {
	clusterManager *multicluster.ClusterManager
}

// NewSimulator creates a new data transfer simulator
func NewSimulator(clusterManager *multicluster.ClusterManager) *Simulator {
	return &Simulator{
		clusterManager: clusterManager,
	}
}

// TransferRequest contains all information needed to simulate a data transfer
type TransferRequest struct {
	// SourceCluster is where the data "resides" (where we generate traffic from)
	SourceCluster string
	// TargetCluster is where the pipeline will run (where data sink is)
	TargetCluster string
	// DataSizeBytes is the amount of data to transfer
	DataSizeBytes int64
	// PipelineName is used for naming the transfer job
	PipelineName string
	// Namespace for the job
	Namespace string
}

// TransferStatus represents the result of a data transfer
type TransferStatus struct {
	// Completed indicates if transfer finished successfully
	Completed bool
	// Failed indicates if transfer failed
	Failed bool
	// StartTime when transfer started
	StartTime time.Time
	// EndTime when transfer completed
	EndTime time.Time
	// DurationMs actual transfer duration in milliseconds
	DurationMs int64
	// BytesTransferred actual bytes sent
	BytesTransferred int64
	// ErrorMessage if failed
	ErrorMessage string
	// JobName the name of the transfer job
	JobName string
}

// SimulateTransfer creates a Job on the source cluster that sends data to the target cluster
// through Submariner, simulating real network traffic
func (s *Simulator) SimulateTransfer(ctx context.Context, req *TransferRequest) (*TransferStatus, error) {
	logger := log.FromContext(ctx)

	// Validate request
	if req.SourceCluster == "" || req.TargetCluster == "" {
		return nil, fmt.Errorf("source and target clusters are required")
	}

	if req.SourceCluster == req.TargetCluster {
		logger.Info("[TRANSFER] No transfer needed - same cluster",
			"cluster", req.SourceCluster)
		return &TransferStatus{
			Completed:        true,
			DurationMs:       0,
			BytesTransferred: 0,
		}, nil
	}

	if req.DataSizeBytes <= 0 {
		logger.Info("[TRANSFER] No transfer needed - no data",
			"source", req.SourceCluster,
			"target", req.TargetCluster)
		return &TransferStatus{
			Completed:        true,
			DurationMs:       0,
			BytesTransferred: 0,
		}, nil
	}

	// Get client for source cluster (where we create the sender job)
	sourceClient, err := s.clusterManager.GetClient(req.SourceCluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get source cluster client: %w", err)
	}

	// Build the data sink address using Submariner DNS
	sinkAddress := buildSubmarinerAddress(req.TargetCluster, DataSinkServiceName, DataSinkNamespace)

	// IMPORTANT: Always use DataSinkNamespace (cloudcontinuum-system) for the job
	// This namespace exists on all clusters, unlike the pipeline's namespace
	jobNamespace := DataSinkNamespace

	// Create the transfer job
	job := s.buildTransferJob(req, sinkAddress, jobNamespace)

	logger.Info("[TRANSFER] Creating data transfer job",
		"job", job.Name,
		"namespace", jobNamespace,
		"source", req.SourceCluster,
		"target", req.TargetCluster,
		"sinkAddress", sinkAddress,
		"dataSize", formatBytes(req.DataSizeBytes))

	// Create the job on source cluster
	startTime := time.Now()
	if err := sourceClient.Create(ctx, job); err != nil {
		if !errors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("failed to create transfer job: %w", err)
		}
		logger.Info("[TRANSFER] Job already exists, waiting for completion", "job", job.Name)
	}

	// Wait for job completion
	status, err := s.waitForJobCompletion(ctx, sourceClient, job.Name, jobNamespace, startTime)
	if err != nil {
		return nil, fmt.Errorf("transfer job failed: %w", err)
	}

	status.BytesTransferred = req.DataSizeBytes
	status.JobName = job.Name

	logger.Info("[TRANSFER] Transfer completed",
		"job", job.Name,
		"duration", fmt.Sprintf("%dms", status.DurationMs),
		"bytes", formatBytes(status.BytesTransferred),
		"success", status.Completed)

	return status, nil
}

// buildTransferJob creates the Kubernetes Job specification for data transfer
func (s *Simulator) buildTransferJob(req *TransferRequest, sinkAddress string, namespace string) *batchv1.Job {
	jobName := fmt.Sprintf("%s-%s-%d", TransferJobPrefix, req.PipelineName, time.Now().Unix())

	// Truncate job name if too long (max 63 chars for Kubernetes)
	if len(jobName) > 63 {
		jobName = jobName[:63]
	}

	// Calculate block count for dd (1MB blocks)
	blockSizeMB := int64(1024 * 1024) // 1MB
	blockCount := req.DataSizeBytes / blockSizeMB
	if blockCount == 0 {
		blockCount = 1
	}

	// Build the transfer command
	// Uses dd to generate random data and pipes it to netcat
	transferCmd := fmt.Sprintf(`
set -e
echo "[TRANSFER] Starting data transfer to %s:%d"
echo "[TRANSFER] Data size: %d bytes (%d MB blocks)"
START_TIME=$(date +%%s%%N)

# Generate random data and send via netcat
dd if=/dev/urandom bs=1M count=%d 2>/dev/null | nc -w 60 %s %d

END_TIME=$(date +%%s%%N)
DURATION_NS=$((END_TIME - START_TIME))
DURATION_MS=$((DURATION_NS / 1000000))
echo "[TRANSFER] Transfer completed in ${DURATION_MS}ms"
`,
		sinkAddress, DataSinkPort,
		req.DataSizeBytes, blockCount,
		blockCount, sinkAddress, DataSinkPort)

	backoffLimit := int32(3)
	ttlSeconds := int32(3600)            // Clean up after 1 hour
	activeDeadlineSeconds := int64(1800) // 30 minute timeout

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: namespace,
			Labels: map[string]string{
				"app":                        "data-transfer",
				"cloudcontinuum.io/pipeline": req.PipelineName,
				"cloudcontinuum.io/source":   req.SourceCluster,
				"cloudcontinuum.io/target":   req.TargetCluster,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttlSeconds,
			ActiveDeadlineSeconds:   &activeDeadlineSeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":                        "data-transfer",
						"cloudcontinuum.io/pipeline": req.PipelineName,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{
						{
							Name:    "sender",
							Image:   SenderImage,
							Command: []string{"/bin/sh", "-c"},
							Args:    []string{transferCmd},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    mustParseQuantity("100m"),
									corev1.ResourceMemory: mustParseQuantity("128Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    mustParseQuantity("500m"),
									corev1.ResourceMemory: mustParseQuantity("256Mi"),
								},
							},
						},
					},
				},
			},
		},
	}
}

// waitForJobCompletion polls the job status until it completes or fails
func (s *Simulator) waitForJobCompletion(ctx context.Context, cli client.Client, jobName, namespace string, startTime time.Time) (*TransferStatus, error) {
	logger := log.FromContext(ctx)

	ticker := time.NewTicker(PollInterval)
	defer ticker.Stop()

	timeout := time.After(JobTimeout)

	for {
		select {
		case <-ctx.Done():
			return &TransferStatus{
				Failed:       true,
				ErrorMessage: "context cancelled",
				StartTime:    startTime,
				EndTime:      time.Now(),
			}, ctx.Err()

		case <-timeout:
			return &TransferStatus{
				Failed:       true,
				ErrorMessage: "transfer job timed out",
				StartTime:    startTime,
				EndTime:      time.Now(),
			}, fmt.Errorf("transfer job timed out after %v", JobTimeout)

		case <-ticker.C:
			job := &batchv1.Job{}
			err := cli.Get(ctx, types.NamespacedName{Name: jobName, Namespace: namespace}, job)
			if err != nil {
				logger.Error(err, "Failed to get job status", "job", jobName)
				continue
			}

			// Check for completion
			if job.Status.Succeeded > 0 {
				endTime := time.Now()
				if job.Status.CompletionTime != nil {
					endTime = job.Status.CompletionTime.Time
				}
				return &TransferStatus{
					Completed:  true,
					StartTime:  startTime,
					EndTime:    endTime,
					DurationMs: endTime.Sub(startTime).Milliseconds(),
				}, nil
			}

			// Check for failure
			if job.Status.Failed > 0 {
				return &TransferStatus{
					Failed:       true,
					ErrorMessage: fmt.Sprintf("job failed with %d failures", job.Status.Failed),
					StartTime:    startTime,
					EndTime:      time.Now(),
				}, fmt.Errorf("transfer job failed")
			}

			// Still running
			logger.V(1).Info("[TRANSFER] Job still running",
				"job", jobName,
				"active", job.Status.Active,
				"elapsed", time.Since(startTime).String())
		}
	}
}

// CleanupTransferJob deletes a completed transfer job
func (s *Simulator) CleanupTransferJob(ctx context.Context, sourceCluster, jobName, namespace string) error {
	sourceClient, err := s.clusterManager.GetClient(sourceCluster)
	if err != nil {
		return fmt.Errorf("failed to get source cluster client: %w", err)
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: namespace,
		},
	}

	propagationPolicy := metav1.DeletePropagationBackground
	if err := sourceClient.Delete(ctx, job, &client.DeleteOptions{
		PropagationPolicy: &propagationPolicy,
	}); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete transfer job: %w", err)
	}

	return nil
}

// buildSubmarinerAddress constructs the Submariner clusterset.local address
func buildSubmarinerAddress(clusterName, serviceName, namespace string) string {
	// Convert cluster name to Submariner format
	// e.g., "cloud_cluster" -> "cloud-cluster"
	clusterID := clusterNameToSubmarinerID(clusterName)

	// Format: <cluster-id>.<service>.<namespace>.svc.clusterset.local
	return fmt.Sprintf("%s.%s.%s.svc.clusterset.local", clusterID, serviceName, namespace)
}

// clusterNameToSubmarinerID converts internal cluster names to Submariner cluster IDs
func clusterNameToSubmarinerID(clusterName string) string {
	switch clusterName {
	case "cloud_cluster":
		return "cloud-cluster"
	case "edge_cluster_1":
		return "edge-cluster-1"
	case "edge_cluster_2":
		return "edge-cluster-2"
	case "edge_cluster_3":
		return "edge-cluster-3"
	default:
		return clusterName
	}
}

// formatBytes formats bytes to human-readable string
func formatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// mustParseQuantity parses a quantity string and panics on error
func mustParseQuantity(s string) resource.Quantity {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		panic(fmt.Sprintf("invalid quantity: %s", s))
	}
	return q
}
