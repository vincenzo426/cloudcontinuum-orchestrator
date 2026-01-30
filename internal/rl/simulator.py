# internal/rl/simulator.py
"""
CloudContinuum RL - Simulation Module
VERSIONE 4.0 - REALISTIC EXECUTION TIME MODEL

Modelli di simulazione per:
1. Tempo di esecuzione pipeline (funzione di CPU, locality, contesa)
2. Latenza di rete tra cluster
3. Costo trasferimento dati

Il tempo di esecuzione è l'OBIETTIVO PRIMARIO da minimizzare.
"""

import numpy as np
from typing import Dict, Optional, Tuple
from dataclasses import dataclass


# =============================================================================
# NETWORK LATENCY MODEL
# =============================================================================

class NetworkLatencyModel:
    """
    Modello di latenza di rete tra cluster.
    
    Latenza dipende da:
    - Distanza geografica (approssimata da latency_to_cloud)
    - Se source == destination → 0ms (locale)
    - Se uno dei due è cloud → latency_to_cloud del cluster edge
    - Se entrambi edge → somma delle latenze (passano per cloud)
    """
    
    def __init__(self, clusters_config: list = None):
        """
        Inizializza modello latenza.
        
        Args:
            clusters_config: Lista di ClusterConfig
        """
        self.latency_matrix: Dict[str, Dict[str, float]] = {}
        
        if clusters_config:
            self._build_latency_matrix(clusters_config)
    
    def _build_latency_matrix(self, clusters: list):
        """Costruisce matrice di latenza tra tutti i cluster."""
        for src in clusters:
            self.latency_matrix[src.name] = {}
            for dst in clusters:
                if src.name == dst.name:
                    # Stesso cluster = locale
                    self.latency_matrix[src.name][dst.name] = 0.0
                elif src.cluster_type == "cloud":
                    # Cloud → Edge = latenza dell'edge
                    self.latency_matrix[src.name][dst.name] = dst.latency_to_cloud
                elif dst.cluster_type == "cloud":
                    # Edge → Cloud = latenza dell'edge sorgente
                    self.latency_matrix[src.name][dst.name] = src.latency_to_cloud
                else:
                    # Edge → Edge = passano per cloud (somma latenze)
                    self.latency_matrix[src.name][dst.name] = \
                        src.latency_to_cloud + dst.latency_to_cloud
    
    def get_latency(self, source: str, destination: str) -> float:
        """
        Ritorna latenza in millisecondi tra due cluster.
        
        Args:
            source: Nome cluster sorgente (dove sono i dati)
            destination: Nome cluster destinazione (dove esegue la pipeline)
        
        Returns:
            Latenza in ms
        """
        if source in self.latency_matrix and destination in self.latency_matrix[source]:
            return self.latency_matrix[source][destination]
        
        # Fallback: nessuna latenza se non configurato
        return 0.0
    
    def get_transfer_time(self, source: str, destination: str, 
                          data_size_bytes: int = 1024 * 1024 * 100) -> float:
        """
        Stima tempo di trasferimento dati in secondi.
        
        Modello semplificato:
        - Bandwidth stimata ~100 Mbps tra cluster
        - Latenza aggiunge overhead fisso
        
        Args:
            source: Cluster sorgente
            destination: Cluster destinazione
            data_size_bytes: Dimensione dati (default 100MB)
        
        Returns:
            Tempo trasferimento in secondi
        """
        if source == destination:
            return 0.0
        
        latency_ms = self.get_latency(source, destination)
        latency_sec = latency_ms / 1000.0
        
        # Bandwidth stimata: 100 Mbps = 12.5 MB/s
        bandwidth_bytes_per_sec = 12.5 * 1024 * 1024
        transfer_time = data_size_bytes / bandwidth_bytes_per_sec
        
        # Tempo totale = latenza + trasferimento
        return latency_sec + transfer_time


# =============================================================================
# EXECUTION TIME SIMULATOR
# =============================================================================

class ExecutionTimeSimulator:
    """
    Simula il tempo di esecuzione di una pipeline.
    
    Il tempo dipende da:
    1. DIMENSIONE PIPELINE: più CPU richiesta = più tempo
    2. DATA LOCALITY: se dati non locali → overhead trasferimento
    3. CONTESA RISORSE: cluster più carico = esecuzione più lenta
    
    Formula:
        exec_time = base_time * contention_factor + transfer_overhead
    
    Dove:
        base_time = cpu_required / 1000 * time_per_core
        contention_factor = 1.0 + (cluster_utilization * contention_impact)
        transfer_overhead = latency * latency_impact (se non locale)
    """
    
    def __init__(
        self,
        time_per_cpu_core: float = 30.0,
        latency_impact_factor: float = 0.5,
        contention_impact_factor: float = 0.5,
        baseline_time: float = 60.0
    ):
        """
        Inizializza simulatore.
        
        Args:
            time_per_cpu_core: Secondi per CPU core richiesto (default 30s)
            latency_impact_factor: Quanto la latenza impatta il tempo (0-1)
            contention_impact_factor: Quanto la contesa impatta il tempo (0-1)
            baseline_time: Tempo baseline per normalizzazione
        """
        self.time_per_cpu_core = time_per_cpu_core
        self.latency_impact_factor = latency_impact_factor
        self.contention_impact_factor = contention_impact_factor
        self.baseline_time = baseline_time
    
    def simulate(
        self,
        pipeline_cpu: int,
        pipeline_memory: int,
        cluster_cpu_utilization: float,
        cluster_memory_utilization: float,
        network_latency_ms: float,
        is_data_local: bool
    ) -> float:
        """
        Simula tempo di esecuzione della pipeline.
        
        Args:
            pipeline_cpu: CPU richiesta in millicores
            pipeline_memory: Memoria richiesta in bytes
            cluster_cpu_utilization: Utilizzo CPU del cluster (0-1)
            cluster_memory_utilization: Utilizzo memoria del cluster (0-1)
            network_latency_ms: Latenza di rete in ms (se dati remoti)
            is_data_local: True se i dati sono sul cluster di esecuzione
        
        Returns:
            Tempo di esecuzione stimato in secondi
        """
        # 1. TEMPO BASE (proporzionale a CPU richiesta)
        # 1000m = 1 core → 30 secondi base
        cpu_cores = pipeline_cpu / 1000.0
        base_time = cpu_cores * self.time_per_cpu_core
        
        # Minimo 10 secondi per qualsiasi pipeline
        base_time = max(10.0, base_time)
        
        # 2. FATTORE CONTESA (cluster carico = più lento)
        # Media tra utilizzo CPU e memoria
        avg_utilization = (cluster_cpu_utilization + cluster_memory_utilization) / 2.0
        
        # Contesa: da 1.0 (cluster vuoto) a 1.5 (cluster saturo)
        contention_factor = 1.0 + (avg_utilization * self.contention_impact_factor)
        
        # 3. OVERHEAD TRASFERIMENTO DATI
        transfer_overhead = 0.0
        if not is_data_local:
            # Latenza aggiunge overhead (convertita in secondi * fattore impatto)
            transfer_overhead = (network_latency_ms / 1000.0) * self.latency_impact_factor * 10.0
            
            # Overhead minimo per trasferimento: 2 secondi
            transfer_overhead = max(2.0, transfer_overhead)
        
        # 4. TEMPO FINALE
        exec_time = (base_time * contention_factor) + transfer_overhead
        
        # Aggiungi variabilità realistica (±10%)
        noise = np.random.uniform(-0.1, 0.1)
        exec_time *= (1.0 + noise)
        
        return round(exec_time, 2)
    
    def normalize_time(self, exec_time: float) -> float:
        """
        Normalizza tempo rispetto al baseline.
        
        Returns:
            Tempo normalizzato (1.0 = baseline, <1.0 = veloce, >1.0 = lento)
        """
        return exec_time / self.baseline_time


# =============================================================================
# RESOURCE CONTENTION MODEL
# =============================================================================

class ResourceContentionModel:
    """
    Modella la contesa delle risorse su un cluster.
    
    Quando un cluster è carico:
    - Le pipeline esistenti competono per CPU/memoria
    - Le nuove pipeline subiscono rallentamenti
    - Il rischio di OOM/throttling aumenta
    """
    
    @staticmethod
    def get_contention_score(cpu_utilization: float, 
                             memory_utilization: float) -> float:
        """
        Calcola score di contesa (0 = nessuna, 1 = massima).
        
        Args:
            cpu_utilization: Utilizzo CPU (0-1)
            memory_utilization: Utilizzo memoria (0-1)
        
        Returns:
            Score di contesa (0-1)
        """
        # Peso maggiore a CPU (più critica per performance)
        weighted_util = (cpu_utilization * 0.6) + (memory_utilization * 0.4)
        
        # Contesa cresce esponenzialmente dopo 70% utilizzo
        if weighted_util < 0.7:
            return weighted_util * 0.5  # Lineare fino a 70%
        else:
            # Esponenziale dopo 70%
            excess = weighted_util - 0.7
            return 0.35 + (excess * 2.0) ** 1.5
    
    @staticmethod
    def get_failure_probability(cpu_utilization: float,
                                memory_utilization: float,
                                pipeline_cpu_ratio: float,
                                pipeline_memory_ratio: float) -> float:
        """
        Stima probabilità di fallimento del placement.
        Soglie basate su utilizzo CPU e memoria.
        MODIFICATO PER LATENZA METRICHE (20s):
        Le soglie sono state abbassate per creare un 'safety buffer'.
        L'agente deve percepire il rischio già al 75% perché i dati reali
        potrebbero essere già all'85-90%.
        
        Returns:
            Probabilità di fallimento (0-1)
        """
        # SOGLIA DI SICUREZZA: 75% (allineata con config.contention_activation_threshold)
        # Se siamo sotto il 75%, siamo ragionevolmente sicuri anche con metriche vecchie.
        if cpu_utilization < 0.75 and memory_utilization < 0.75:
            return 0.0
        
        max_util = max(cpu_utilization, memory_utilization)
        
        # GRADIENTE DI RISCHIO ANTICIPATO
        if max_util < 0.85:
            # Fascia 75% - 85%: Zona "Gialla"
            # Qui il cluster sembra ok, ma a causa del lag potrebbe essere pieno.
            # Introduciamo un rischio basso (5%) per scoraggiare l'uso massiccio.
            return 0.05
            
        elif max_util < 0.90:
            # Fascia 85% - 90%: Zona "Arancione"
            # Rischio significativo. Con 20s di lag, qui sei probabilmente già in crash.
            return 0.15
            
        else:
            # Fascia > 90%: Zona "Rossa"
            # Fallimento molto probabile.
            return 0.30


# =============================================================================
# CLUSTER STATE SIMULATOR
# =============================================================================

@dataclass
class SimulatedClusterState:
    """Stato simulato di un cluster durante un episodio."""
    
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
        """Verifica se la pipeline può essere ospitata."""
        return (self.cpu_available >= cpu_required and 
                self.memory_available >= memory_required)
    
    def allocate(self, cpu: int, memory: int) -> bool:
        """
        Alloca risorse per una pipeline.
        
        Returns:
            True se allocazione riuscita, False altrimenti
        """
        if not self.can_fit(cpu, memory):
            return False
        
        self.cpu_used += cpu
        self.memory_used += memory
        self.placements_count += 1
        return True
    
    def release(self, cpu: int, memory: int):
        """Rilascia risorse (quando pipeline termina)."""
        self.cpu_used = max(0, self.cpu_used - cpu)
        self.memory_used = max(0, self.memory_used - memory)
        self.placements_count = max(0, self.placements_count - 1)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def estimate_optimal_cluster(
    pipeline_cpu: int,
    pipeline_memory: int,
    data_location: str,
    clusters_state: Dict[str, SimulatedClusterState],
    network_model: NetworkLatencyModel,
    exec_simulator: ExecutionTimeSimulator
) -> Tuple[str, float]:
    """
    Stima il cluster ottimale per una pipeline (ground truth per evaluation).
    
    Considera:
    - Tempo di esecuzione stimato
    - Data locality
    - Contesa risorse
    
    Returns:
        (cluster_name, estimated_exec_time)
    """
    best_cluster = None
    best_time = float('inf')
    
    for name, state in clusters_state.items():
        # Verifica se la pipeline ci entra
        if not state.can_fit(pipeline_cpu, pipeline_memory):
            continue
        
        # Calcola tempo stimato
        is_local = (data_location == name or data_location == "none")
        latency = network_model.get_latency(data_location, name) if not is_local else 0.0
        
        exec_time = exec_simulator.simulate(
            pipeline_cpu=pipeline_cpu,
            pipeline_memory=pipeline_memory,
            cluster_cpu_utilization=state.cpu_utilization,
            cluster_memory_utilization=state.memory_utilization,
            network_latency_ms=latency,
            is_data_local=is_local
        )
        
        if exec_time < best_time:
            best_time = exec_time
            best_cluster = name
    
    return best_cluster, best_time


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("CloudContinuum RL - Simulator Test")
    print("=" * 70)
    
    # Test Network Latency Model
    print("\n📡 Network Latency Model:")
    print("-" * 70)
    
    from config import DEFAULT_CLUSTERS
    
    network = NetworkLatencyModel(DEFAULT_CLUSTERS)
    
    pairs = [
        ("cloud_cluster", "cloud_cluster"),
        ("cloud_cluster", "edge_cluster_1"),
        ("edge_cluster_1", "cloud_cluster"),
        ("edge_cluster_1", "edge_cluster_2"),
    ]
    
    for src, dst in pairs:
        latency = network.get_latency(src, dst)
        print(f"  {src} → {dst}: {latency}ms")
    
    # Test Execution Time Simulator
    print("\n⏱️  Execution Time Simulator:")
    print("-" * 70)
    
    simulator = ExecutionTimeSimulator()
    
    test_cases = [
        {"cpu": 500, "util": 0.3, "local": True, "desc": "Small pipeline, light cluster, local"},
        {"cpu": 500, "util": 0.3, "local": False, "desc": "Small pipeline, light cluster, remote"},
        {"cpu": 2000, "util": 0.3, "local": True, "desc": "Large pipeline, light cluster, local"},
        {"cpu": 500, "util": 0.8, "local": True, "desc": "Small pipeline, busy cluster, local"},
        {"cpu": 2000, "util": 0.8, "local": False, "desc": "Large pipeline, busy cluster, remote"},
    ]
    
    for tc in test_cases:
        time = simulator.simulate(
            pipeline_cpu=tc["cpu"],
            pipeline_memory=1024*1024*1024,  # 1GB
            cluster_cpu_utilization=tc["util"],
            cluster_memory_utilization=tc["util"],
            network_latency_ms=0 if tc["local"] else 20,
            is_data_local=tc["local"]
        )
        normalized = simulator.normalize_time(time)
        print(f"  {tc['desc']}")
        print(f"    → {time:.1f}s (normalized: {normalized:.2f})")
    
    # Test Contention Model
    print("\n🔥 Resource Contention Model:")
    print("-" * 70)
    
    for util in [0.3, 0.5, 0.7, 0.85, 0.95]:
        score = ResourceContentionModel.get_contention_score(util, util)
        fail_prob = ResourceContentionModel.get_failure_probability(util, util, 0.2, 0.2)
        print(f"  Utilization {util*100:.0f}%: contention={score:.2f}, fail_prob={fail_prob*100:.0f}%")
    
    print("\n" + "=" * 70)
    print("✅ Simulator OK!")
    print("=" * 70)