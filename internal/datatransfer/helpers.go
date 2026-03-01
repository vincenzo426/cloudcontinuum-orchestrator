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
	"fmt"
	"strconv"
	"strings"
)

// ParseDataSize converts a human-readable data size string to bytes.
// Supports formats like "5GB", "500MB", "100KB", "1024B", "2.5GB"
// This is the exported version of parseDataSize for use by other packages.
func ParseDataSize(size string) (int64, error) {
	if size == "" {
		return 0, fmt.Errorf("empty data size")
	}

	size = strings.ToUpper(strings.TrimSpace(size))

	var multiplier int64
	var numStr string

	switch {
	case strings.HasSuffix(size, "TB"):
		multiplier = 1024 * 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(size, "TB")
	case strings.HasSuffix(size, "GB"):
		multiplier = 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(size, "GB")
	case strings.HasSuffix(size, "MB"):
		multiplier = 1024 * 1024
		numStr = strings.TrimSuffix(size, "MB")
	case strings.HasSuffix(size, "KB"):
		multiplier = 1024
		numStr = strings.TrimSuffix(size, "KB")
	case strings.HasSuffix(size, "B"):
		multiplier = 1
		numStr = strings.TrimSuffix(size, "B")
	default:
		return 0, fmt.Errorf("unsupported size unit (use TB, GB, MB, KB, or B)")
	}

	// Parse the numeric part (supports decimals like "2.5GB")
	num, err := strconv.ParseFloat(strings.TrimSpace(numStr), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid numeric value: %w", err)
	}

	if num < 0 {
		return 0, fmt.Errorf("data size cannot be negative")
	}

	return int64(num * float64(multiplier)), nil
}

// FormatDataSize converts bytes to a human-readable string
func FormatDataSize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/float64(TB))
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

// EstimateTransferTime calculates the estimated transfer time in milliseconds
// given data size in bytes and bandwidth in MB/s
func EstimateTransferTime(dataSizeBytes int64, bandwidthMBps float64, networkLatencyMs int64) int64 {
	if dataSizeBytes <= 0 || bandwidthMBps <= 0 {
		return networkLatencyMs
	}

	// Convert bandwidth to bytes per second
	bandwidthBytesPerSec := bandwidthMBps * 1_000_000

	// Calculate transfer time in seconds, then convert to milliseconds
	transferTimeSeconds := float64(dataSizeBytes) / bandwidthBytesPerSec
	transferTimeMs := int64(transferTimeSeconds * 1000)

	// Total time = network latency + transfer time
	return networkLatencyMs + transferTimeMs
}
