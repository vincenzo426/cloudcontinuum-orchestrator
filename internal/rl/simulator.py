# internal/rl/simulator.py
"""CloudContinuum RL - Simulation models for execution time, latency, and contention."""

import numpy as np
from typing import Dict, Tuple
from dataclasses import dataclass


class NetworkLatencyModel:
    """Network latency model between clusters."""
    
    def __init__(self, clusters_config: list = None):
        self.latency_matrix: Dict[str, Dict[str, float]] = {}
        if clusters_config:
            self._build_latency_matrix(clusters_config)
    
    def _build_latency_matrix(self, clusters: list):
        """Build latency matrix between all clusters."""
        for src in clusters:
            self.latency_matrix[src.name] = {}
            for dst in clusters:
                if src.name == dst.name:
                    self.latency_matrix[src.name][dst.name] = 0.0
                elif src.cluster_type == "cloud":
                    self.latency_matrix[src.name][dst.name] = dst.latency_to_cloud
                elif dst.cluster_type == "cloud":
                    self.latency_matrix[src.name][dst.name] = src.latency_to_cloud
                else:
                    # Edge to edge: goes through cloud
                    self.latency_matrix[src.name][dst.name] = \
                        src.latency_to_cloud + dst.latency_to_cloud
    
    def get_latency(self, source: str, destination: str) -> float:
        """Get latency in ms between two clusters."""
        if source in self.latency_matrix and destination in self.latency_matrix[source]:
            return self.latency_matrix[source][destination]
        return 0.0
    
    def get_transfer_time(self, source: str, destination: str,
                          data_size_bytes: int = 100 * 1024 * 1024) -> float:
        """Estimate data transfer time in seconds (assumes ~100 Mbps bandwidth)."""
        if source == destination:
            return 0.0
        latency_sec = self.get_latency(source, destination) / 1000.0
        bandwidth_bps = 12.5 * 1024 * 1024  # 100 Mbps
        return latency_sec + (data_size_bytes / bandwidth_bps)


class ExecutionTimeSimulator:
    """
    Pipeline execution time simulator.
    
    Time depends on: CPU required, data locality, data size, and resource contention.
    Formula: exec_time = base_time * contention_factor + transfer_time
    """
    
    # Network bandwidth estimates
    BANDWIDTH_CLOUD_EDGE = 100 * 1024 * 1024  # 100 MB/s (fast link)
    BANDWIDTH_EDGE_EDGE = 50 * 1024 * 1024    # 50 MB/s (goes through cloud)
    
    def __init__(self, time_per_cpu_core: float = 30.0, latency_impact_factor: float = 0.5,
                 contention_impact_factor: float = 0.5, baseline_time: float = 60.0):
        self.time_per_cpu_core = time_per_cpu_core
        self.latency_impact_factor = latency_impact_factor
        self.contention_impact_factor = contention_impact_factor
        self.baseline_time = baseline_time
    
    def simulate(self, pipeline_cpu: int, pipeline_memory: int,
                 cluster_cpu_utilization: float, cluster_memory_utilization: float,
                 network_latency_ms: float, is_data_local: bool,
                 data_size_bytes: int = 0, is_edge_to_edge: bool = False) -> float:
        """
        Simulate pipeline execution time in seconds.
        
        Args:
            pipeline_cpu: CPU required in millicores
            pipeline_memory: Memory required in bytes
            cluster_cpu_utilization: Target cluster CPU utilization (0-1)
            cluster_memory_utilization: Target cluster memory utilization (0-1)
            network_latency_ms: Network latency in ms
            is_data_local: True if data is on target cluster
            data_size_bytes: Size of data to transfer (0 if local)
            is_edge_to_edge: True if transfer is between two edge clusters
        
        Returns:
            Execution time in seconds
        """
        # Base time proportional to CPU
        cpu_cores = pipeline_cpu / 1000.0
        base_time = max(10.0, cpu_cores * self.time_per_cpu_core)
        
        # Contention factor (1.0 to 1.5 based on utilization)
        avg_util = (cluster_cpu_utilization + cluster_memory_utilization) / 2.0
        contention_factor = 1.0 + (avg_util * self.contention_impact_factor)
        
        # Transfer time for remote data (based on actual data size)
        transfer_time = 0.0
        if not is_data_local and data_size_bytes > 0:
            # Select bandwidth based on transfer type
            bandwidth = self.BANDWIDTH_EDGE_EDGE if is_edge_to_edge else self.BANDWIDTH_CLOUD_EDGE
            
            # Transfer time = latency + data_size / bandwidth
            latency_sec = network_latency_ms / 1000.0
            transfer_time = latency_sec + (data_size_bytes / bandwidth)
            
            # Minimum 1 second for any transfer
            transfer_time = max(1.0, transfer_time)
        
        # Final time with noise (±10%)
        exec_time = (base_time * contention_factor) + transfer_time
        exec_time *= (1.0 + np.random.uniform(-0.1, 0.1))
        return round(exec_time, 2)
    
    def normalize_time(self, exec_time: float) -> float:
        """Normalize time relative to baseline."""
        return exec_time / self.baseline_time


class ResourceContentionModel:
    """Model for resource contention effects on placement."""
    
    @staticmethod
    def get_contention_score(cpu_utilization: float, memory_utilization: float) -> float:
        """
        Calculate contention score (0-1).
        Returns 0 for low utilization, increases non-linearly above 70%.
        """
        weighted_util = 0.6 * cpu_utilization + 0.4 * memory_utilization
        if weighted_util < 0.5:
            return 0.0
        elif weighted_util < 0.7:
            return (weighted_util - 0.5) * 1.5
        else:
            excess = weighted_util - 0.7
            return 0.35 + (excess * 2.0) ** 1.5
    
    @staticmethod
    def get_failure_probability(cpu_utilization: float, memory_utilization: float,
                                pipeline_cpu_ratio: float, pipeline_memory_ratio: float) -> float:
        """
        Estimate placement failure probability.
        Thresholds account for 20s metrics latency (safety buffer).
        """
        if cpu_utilization < 0.75 and memory_utilization < 0.75:
            return 0.0
        
        max_util = max(cpu_utilization, memory_utilization)
        if max_util < 0.85:
            return 0.05  # Yellow zone
        elif max_util < 0.90:
            return 0.15  # Orange zone
        return 0.30     # Red zone


@dataclass
class SimulatedClusterState:
    """Simulated cluster state during an episode."""
    
    name: str
    cluster_type: str
    cpu_capacity: int
    memory_capacity: int
    cpu_used: int
    memory_used: int
    placements_count: int
    latency_to_cloud: float
    
    @property
    def cpu_available(self) -> int:
        return max(0, self.cpu_capacity - self.cpu_used)
    
    @property
    def memory_available(self) -> int:
        return max(0, self.memory_capacity - self.memory_used)
    
    @property
    def cpu_utilization(self) -> float:
        return self.cpu_used / self.cpu_capacity if self.cpu_capacity > 0 else 0.0
    
    @property
    def memory_utilization(self) -> float:
        return self.memory_used / self.memory_capacity if self.memory_capacity > 0 else 0.0
    
    def can_fit(self, cpu_required: int, memory_required: int) -> bool:
        """Check if pipeline fits in cluster."""
        return self.cpu_available >= cpu_required and self.memory_available >= memory_required
    
    def allocate(self, cpu: int, memory: int) -> bool:
        """Allocate resources. Returns True on success."""
        if not self.can_fit(cpu, memory):
            return False
        self.cpu_used += cpu
        self.memory_used += memory
        self.placements_count += 1
        return True
    
    def release(self, cpu: int, memory: int):
        """Release resources when pipeline completes."""
        self.cpu_used = max(0, self.cpu_used - cpu)
        self.memory_used = max(0, self.memory_used - memory)
        self.placements_count = max(0, self.placements_count - 1)


def estimate_optimal_cluster(pipeline_cpu: int, pipeline_memory: int, data_location: str,
                             clusters_state: Dict[str, SimulatedClusterState],
                             network_model: NetworkLatencyModel,
                             exec_simulator: ExecutionTimeSimulator,
                             data_size_bytes: int = 0) -> Tuple[str, float]:
    """Estimate optimal cluster for pipeline (ground truth for evaluation)."""
    best_cluster, best_time = None, float('inf')
    
    # Get source cluster type for edge-to-edge check
    src_type = "unknown"
    for name, state in clusters_state.items():
        if name == data_location:
            src_type = state.cluster_type
            break
    
    for name, state in clusters_state.items():
        if not state.can_fit(pipeline_cpu, pipeline_memory):
            continue
        
        is_local = (data_location == name or data_location == "none")
        latency = 0.0 if is_local else network_model.get_latency(data_location, name)
        is_edge_to_edge = (src_type == "edge" and state.cluster_type == "edge")
        
        exec_time = exec_simulator.simulate(
            pipeline_cpu, pipeline_memory,
            state.cpu_utilization, state.memory_utilization,
            latency, is_local,
            data_size_bytes=data_size_bytes if not is_local else 0,
            is_edge_to_edge=is_edge_to_edge
        )
        
        if exec_time < best_time:
            best_time = exec_time
            best_cluster = name
    
    return best_cluster, best_time