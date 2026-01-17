# internal/rl/simulator.py
"""
Simulatori per execution time e network latency
Modelli realistici basati su dati produzione CloudContinuum
"""

import numpy as np
from typing import Dict


class ExecutionTimeSimulator:
    """
    Simula execution time di pipeline basato su risorse e latenza.
    
    MODELLO:
    exec_time = base_time * (1 + cpu_factor + memory_factor + network_factor)
    
    - base_time: Tempo baseline per pipeline (~60s)
    - cpu_factor: Penalità se CPU scarso
    - memory_factor: Penalità se memoria scarsa
    - network_factor: Overhead da data transfer remoto
    """
    
    def __init__(self, base_time: float = 60.0):
        self.base_time = base_time
        
        # Pesi componenti (devono sommare a 1.0)
        self.cpu_weight = 0.5
        self.memory_weight = 0.3
        self.network_weight = 0.2
        
        # Soglie per penalità
        self.cpu_shortage_threshold = 0.7      # Se < 30% disponibile
        self.memory_shortage_threshold = 0.6   # Se < 40% disponibile
    
    def simulate(
        self,
        pipeline_cpu: int,                  # millicores richiesti
        pipeline_memory: int,               # bytes richiesti
        cluster_cpu_available: int,         # millicores disponibili
        cluster_memory_available: int,      # bytes disponibili
        network_latency: float = 0.0,       # ms
        is_data_local: bool = True
    ) -> float:
        """
        Simula execution time (in secondi).
        
        Returns:
            Execution time in secondi
        """
        # Base time
        exec_time = self.base_time
        
        # CPU factor: penalità se risorse scarse
        cpu_utilization = pipeline_cpu / max(cluster_cpu_available, 1)
        if cpu_utilization > self.cpu_shortage_threshold:
            # Overhead proporzionale alla scarsità
            cpu_penalty = (cpu_utilization - self.cpu_shortage_threshold) * 2.0
            exec_time += cpu_penalty * self.base_time * self.cpu_weight
        
        # Memory factor
        memory_utilization = pipeline_memory / max(cluster_memory_available, 1)
        if memory_utilization > self.memory_shortage_threshold:
            memory_penalty = (memory_utilization - self.memory_shortage_threshold) * 1.5
            exec_time += memory_penalty * self.base_time * self.memory_weight
        
        # Network factor: data transfer overhead
        if not is_data_local:
            # Convert network latency (ms) to seconds
            transfer_overhead = (network_latency / 1000.0) * 10  # Assume 10x latency
            exec_time += transfer_overhead * self.network_weight
        
        # Add random variance (±10%)
        noise = np.random.uniform(-0.1, 0.1) * exec_time
        exec_time += noise
        
        # Ensure non-negative
        exec_time = max(exec_time, 1.0)
        
        return exec_time


class NetworkLatencyModel:
    """
    Modello di latenza di rete tra cluster.
    
    BASATO SU MISURAZIONE REALE (dai log collector):
    - Cloud ↔ Edge: 30-60ms
    - Edge ↔ Edge: 10-20ms
    - Same cluster: 0ms
    """
    
    def __init__(self):
        # Latency matrix (ms) - simmetrica
        self.latency_matrix = {
            # Cloud cluster
            ("cloud_cluster", "edge_cluster_1"): self._random_latency(35, 55),
            ("cloud_cluster", "edge_cluster_2"): self._random_latency(30, 50),
            ("cloud_cluster", "edge_cluster_3"): self._random_latency(40, 60),
            
            # Edge cluster 1
            ("edge_cluster_1", "cloud_cluster"): self._random_latency(35, 55),
            ("edge_cluster_1", "edge_cluster_2"): self._random_latency(12, 18),
            ("edge_cluster_1", "edge_cluster_3"): self._random_latency(10, 16),
            
            # Edge cluster 2
            ("edge_cluster_2", "cloud_cluster"): self._random_latency(30, 50),
            ("edge_cluster_2", "edge_cluster_1"): self._random_latency(12, 18),
            ("edge_cluster_2", "edge_cluster_3"): self._random_latency(11, 17),
            
            # Edge cluster 3
            ("edge_cluster_3", "cloud_cluster"): self._random_latency(40, 60),
            ("edge_cluster_3", "edge_cluster_1"): self._random_latency(10, 16),
            ("edge_cluster_3", "edge_cluster_2"): self._random_latency(11, 17),
        }
    
    def _random_latency(self, min_ms: float, max_ms: float) -> float:
        """Genera latenza casuale nel range specificato"""
        return np.random.uniform(min_ms, max_ms)
    
    def get_latency(self, source_cluster: str, target_cluster: str) -> float:
        """
        Ritorna latenza di rete (ms) tra due cluster.
        
        Args:
            source_cluster: Nome cluster sorgente
            target_cluster: Nome cluster destinazione
        
        Returns:
            Latenza in millisecondi
        """
        # Same cluster = 0 latency
        if source_cluster == target_cluster:
            return 0.0
        
        # Lookup in matrix
        key = (source_cluster, target_cluster)
        if key in self.latency_matrix:
            # Add jitter (±5ms)
            base_latency = self.latency_matrix[key]
            jitter = np.random.uniform(-5, 5)
            return max(base_latency + jitter, 0.0)
        
        # Default: assume edge-to-edge latency
        return self._random_latency(10, 20)
    
    def get_bandwidth_estimate(
        self,
        source_cluster: str,
        target_cluster: str
    ) -> float:
        """
        Stima bandwidth (Mbps) tra due cluster.
        
        ASSUNZIONI REALISTICHE:
        - Cloud ↔ Edge: 100 Mbps (WAN)
        - Edge ↔ Edge: 1 Gbps (LAN)
        """
        if source_cluster == target_cluster:
            return float('inf')  # Infinito bandwidth (locale)
        
        # Cloud involved = WAN bandwidth
        if "cloud" in source_cluster or "cloud" in target_cluster:
            return 100.0  # Mbps
        
        # Edge-to-edge = LAN bandwidth
        return 1000.0  # Mbps (1 Gbps)


class ResourceUtilizationSimulator:
    """
    Simula dinamica di utilizzo risorse nel tempo.
    
    UTILE PER: Simulare variazioni realistiche di baseline utilization
    durante episodi lunghi.
    """
    
    def __init__(self, update_interval: int = 10):
        """
        Args:
            update_interval: Ogni quanti step aggiornare baseline
        """
        self.update_interval = update_interval
        self.step_counter = 0
    
    def update_cluster_baseline(self, cluster: Dict) -> Dict:
        """
        Aggiorna baseline utilization con variazione random.
        
        Simula workload esterno che entra/esce dal cluster.
        """
        self.step_counter += 1
        
        if self.step_counter % self.update_interval != 0:
            return cluster  # No update
        
        # Random variation: ±5% della capacità
        cpu_variation = int(np.random.uniform(-0.05, 0.05) * cluster['cpu_capacity'])
        mem_variation = int(np.random.uniform(-0.05, 0.05) * cluster['memory_capacity'])
        
        # Apply variation (con clipping)
        new_cpu_used = np.clip(
            cluster['cpu_used'] + cpu_variation,
            0,
            cluster['cpu_capacity']
        )
        new_mem_used = np.clip(
            cluster['memory_used'] + mem_variation,
            0,
            cluster['memory_capacity']
        )
        
        # Update available
        cluster['cpu_used'] = new_cpu_used
        cluster['memory_used'] = new_mem_used
        cluster['cpu_available'] = cluster['cpu_capacity'] - new_cpu_used
        cluster['memory_available'] = cluster['memory_capacity'] - new_mem_used
        
        return cluster


# ========== UTILITIES ==========

def format_bytes(bytes_val: int) -> str:
    """Format bytes in human-readable form"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PB"


def format_millicores(millicores: int) -> str:
    """Format millicores in cores"""
    cores = millicores / 1000.0
    return f"{cores:.2f} cores ({millicores}m)"


# ========== TESTING ==========
if __name__ == "__main__":
    print("=" * 60)
    print("SIMULATOR TESTING")
    print("=" * 60)
    
    # Test Execution Time Simulator
    print("\n1. Execution Time Simulator:")
    exec_sim = ExecutionTimeSimulator()
    
    scenarios = [
        {
            "name": "Light pipeline, ample resources, data local",
            "pipeline_cpu": 500,
            "pipeline_memory": 1 * 1024**3,
            "cluster_cpu_available": 5000,
            "cluster_memory_available": 10 * 1024**3,
            "network_latency": 0,
            "is_data_local": True
        },
        {
            "name": "Heavy pipeline, scarce CPU, data remote",
            "pipeline_cpu": 3000,
            "pipeline_memory": 6 * 1024**3,
            "cluster_cpu_available": 4000,
            "cluster_memory_available": 8 * 1024**3,
            "network_latency": 45,
            "is_data_local": False
        },
    ]
    
    for scenario in scenarios:
        exec_time = exec_sim.simulate(**{k: v for k, v in scenario.items() if k != 'name'})
        print(f"\n  {scenario['name']}:")
        print(f"    Execution time: {exec_time:.2f}s")
    
    # Test Network Latency Model
    print("\n2. Network Latency Model:")
    latency_model = NetworkLatencyModel()
    
    cluster_pairs = [
        ("cloud_cluster", "edge_cluster_1"),
        ("edge_cluster_1", "edge_cluster_2"),
        ("edge_cluster_2", "edge_cluster_3"),
    ]
    
    for source, target in cluster_pairs:
        latency = latency_model.get_latency(source, target)
        bandwidth = latency_model.get_bandwidth_estimate(source, target)
        print(f"\n  {source} → {target}:")
        print(f"    Latency: {latency:.2f}ms")
        print(f"    Bandwidth: {bandwidth:.0f} Mbps")
    
    print("\n" + "=" * 60)