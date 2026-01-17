# internal/rl/config.py
"""
Configurazione centralizzata per RL-based pipeline placement
VERSIONE 2.0 - OTTIMIZZATA PER SUCCESS RATE ≥95%

CHANGELOG v2.0:
- Reward shaping drasticamente migliorato
- Baseline di utilizzo realistico dai log produzione
- Parametri per action masking
- Target utilization ranges
"""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple

@dataclass
class ClusterConfig:
    """Configurazione capacità cluster (da ambiente reale)"""
    name: str
    cpu_capacity: int      # millicores
    memory_capacity: int   # bytes
    cluster_type: str      # "cloud" o "edge"
    
    # Baseline di utilizzo realistico (dai log produzione 2026-01-16)
    baseline_cpu_used: int = 0
    baseline_memory_used: int = 0

@dataclass
class EnvironmentConfig:
    """Configurazione Gymnasium Environment - OTTIMIZZATA v2.0"""
    
    # ========== CLUSTERS CONFIGURATION ==========
    clusters: List[ClusterConfig] = None
    
    # ========== EPISODE PARAMETERS ==========
    pipelines_per_episode: int = 8  # Aumentato da 4 per più learning
    max_episode_steps: int = 100     # Aumentato da 50
    
    # ========== STATE SPACE DIMENSIONS ==========
    # AGGIORNATO: più feature per cluster
    state_features_per_cluster: int = 9  # Era 7, ora include safety_margin, stress_level, balance_score
    state_features_global: int = 6
    state_features_temporal: int = 4
    
    # ========== REWARD SHAPING OTTIMIZZATO V2.0 ==========
    # CRITICO: Questi valori sono calibrati per success rate ≥95%
    
    reward_scale: float = 1.0
    
    # Penalità DRAMMATICAMENTE aumentate per errori
    penalty_failed_placement: float = -500.0      # Era -100 → x5
    penalty_invalid_action: float = -300.0        # NUOVO: azione su cluster senza risorse
    penalty_overload: float = -200.0              # NUOVO: cluster oltre 90% utilizzo
    penalty_underutilization: float = -50.0       # NUOVO: cluster sotto 20% utilizzo
    penalty_cluster_monopoly: float = -100.0      # NUOVO: troppi task su singolo cluster
    
    # Reward positivi graduali per incentivare comportamenti corretti
    bonus_successful_placement: float = 100.0     # Era 10 → x10
    bonus_data_locality: float = 150.0            # Era 50 → x3
    bonus_balanced_utilization: float = 200.0     # Era 80 → x2.5
    bonus_new_cluster: float = 30.0               # NUOVO: incentiva distribuzione
    
    # NUOVO: Target utilization range (sweet spot)
    target_utilization_min: float = 0.40  # 40%
    target_utilization_max: float = 0.75  # 75%
    
    # Penalità per data transfer remoto
    penalty_remote_placement: float = -20.0
    
    # ========== SIMULATION PARAMETERS ==========
    simulation_speedup: float = 1000.0
    baseline_execution_time: float = 60.0
    
    # Network latency weights (per simulatore)
    cpu_weight: float = 0.7
    memory_weight: float = 0.2
    latency_weight: float = 0.1
    
    # ========== DIFFICULTY CURRICULUM ==========
    difficulty: str = "medium"  # easy/medium/hard
    
    # Parametri variabili per difficulty
    difficulty_params: Dict[str, Dict] = field(default_factory=lambda: {
        "easy": {
            "pipelines_per_episode": 4,
            "resource_variance": 0.1,    # Bassa variabilità
            "failure_tolerance": 0.3,    # Permette 30% fallimenti
        },
        "medium": {
            "pipelines_per_episode": 8,
            "resource_variance": 0.25,   # Media variabilità
            "failure_tolerance": 0.15,   # Permette 15% fallimenti
        },
        "hard": {
            "pipelines_per_episode": 12,
            "resource_variance": 0.4,    # Alta variabilità
            "failure_tolerance": 0.05,   # Permette solo 5% fallimenti
        }
    })
    
    # ========== SEED MANAGEMENT ==========
    master_seed: int = 42
    
    # ========== ACTION MASKING ==========
    enable_action_masking: bool = True  # CRITICO per success rate
    
    # ========== LOGGING & MONITORING ==========
    verbose: bool = True
    log_episode_stats: bool = True
    
    def __post_init__(self):
        if self.clusters is None:
            # ===== METRICHE REALI DAL LOG PRODUZIONE (2026-01-17 15:32:52) =====
            # Tutti i cluster hanno STESSA capacità: 6000m CPU, 16GB RAM
            # Baseline usage: ~60% CPU (3600-3700m), ~55% RAM (8-9GB)
            self.clusters = [
                ClusterConfig(
                    name="cloud_cluster",
                    cpu_capacity=6000,            # 6 cores
                    memory_capacity=16739684352,  # ~16GB (dal log)
                    cluster_type="cloud",
                    baseline_cpu_used=3715,       # 62% utilizzato (dal log)
                    baseline_memory_used=9189720064  # ~8.6GB (dal log)
                ),
                ClusterConfig(
                    name="edge_cluster_1",
                    cpu_capacity=6000,            # 6 cores
                    memory_capacity=16739696640,  # ~16GB
                    cluster_type="edge",
                    baseline_cpu_used=3605,       # 60% utilizzato
                    baseline_memory_used=8912896000  # ~8.3GB
                ),
                ClusterConfig(
                    name="edge_cluster_2",
                    cpu_capacity=6000,            # 6 cores
                    memory_capacity=16739692544,  # ~16GB
                    cluster_type="edge",
                    baseline_cpu_used=3605,       # 60% utilizzato
                    baseline_memory_used=8912896000  # ~8.3GB
                ),
                ClusterConfig(
                    name="edge_cluster_3",
                    cpu_capacity=6000,            # 6 cores
                    memory_capacity=16739700736,  # ~16GB
                    cluster_type="edge",
                    baseline_cpu_used=3605,       # 60% utilizzato
                    baseline_memory_used=8912896000  # ~8.3GB
                ),
            ]
        
        # Applica parametri specifici per difficulty
        if self.difficulty in self.difficulty_params:
            params = self.difficulty_params[self.difficulty]
            self.pipelines_per_episode = params["pipelines_per_episode"]
    
    @property
    def num_clusters(self) -> int:
        return len(self.clusters)
    
    @property
    def total_state_size(self) -> int:
        """Dimensione totale del vettore stato"""
        return (
            self.num_clusters * self.state_features_per_cluster +
            self.state_features_global +
            self.state_features_temporal
        )
    
    def get_reward_config(self) -> Dict[str, float]:
        """Ritorna dizionario con tutti i parametri reward (per logging)"""
        return {
            "penalty_failed_placement": self.penalty_failed_placement,
            "penalty_invalid_action": self.penalty_invalid_action,
            "penalty_overload": self.penalty_overload,
            "penalty_underutilization": self.penalty_underutilization,
            "penalty_cluster_monopoly": self.penalty_cluster_monopoly,
            "bonus_successful_placement": self.bonus_successful_placement,
            "bonus_data_locality": self.bonus_data_locality,
            "bonus_balanced_utilization": self.bonus_balanced_utilization,
            "bonus_new_cluster": self.bonus_new_cluster,
            "penalty_remote_placement": self.penalty_remote_placement,
        }


# ========== PIPELINE WORKLOAD TEMPLATES ==========
# AGGIORNATO: Basati su DATI REALI dal progetto CloudContinuum
# Fonte: examples/pipeline.yaml e log produzione
# Executor tipico: 100m CPU + 512MB-4GB RAM
# Pipeline tipiche: 2-10 executor = 200m-1000m CPU totali, 1-8GB RAM totali

PIPELINE_TEMPLATES = {
    "light": {
        "cpu_range": (200, 600),       # millicores (2-6 executor @ 100m)
        "memory_range": (0.5, 2),      # GB (512MB-2GB totali)
        "probability": 0.5,            # 50% dei workload (più comune)
        "description": "2-6 executors, preprocessing/inference leggero"
    },
    "medium": {
        "cpu_range": (600, 1200),      # millicores (6-12 executor @ 100m)
        "memory_range": (2, 5),        # GB (2-5GB totali)
        "probability": 0.35,           # 35% dei workload
        "description": "6-12 executors, training moderato"
    },
    "heavy": {
        "cpu_range": (1200, 2000),     # millicores (12-20 executor @ 100m)
        "memory_range": (5, 8),        # GB (5-8GB totali)
        "probability": 0.15,           # 15% dei workload (rari)
        "description": "12-20 executors, training intensivo"
    },
}


# ========== INSTANCE GLOBALE ==========
DEFAULT_CONFIG = EnvironmentConfig()


# ========== UTILITIES ==========
def get_config_for_difficulty(difficulty: str) -> EnvironmentConfig:
    """Factory per creare config basata su difficulty"""
    config = EnvironmentConfig()
    config.difficulty = difficulty
    return config


def print_config_summary(config: EnvironmentConfig):
    """Stampa riepilogo configurazione (utile per debugging)"""
    print("=" * 60)
    print("ENVIRONMENT CONFIGURATION SUMMARY")
    print("=" * 60)
    print(f"Difficulty: {config.difficulty}")
    print(f"Clusters: {config.num_clusters}")
    print(f"Pipelines per episode: {config.pipelines_per_episode}")
    print(f"State size: {config.total_state_size}")
    print(f"Action masking: {config.enable_action_masking}")
    print("\nReward Configuration:")
    for key, value in config.get_reward_config().items():
        print(f"  {key:30s}: {value:8.1f}")
    print("=" * 60)


if __name__ == "__main__":
    # Test configuration
    config = DEFAULT_CONFIG
    print_config_summary(config)
    
    # Test difficulty variations
    for difficulty in ["easy", "medium", "hard"]:
        print(f"\n{difficulty.upper()} difficulty:")
        cfg = get_config_for_difficulty(difficulty)
        print(f"  Pipelines per episode: {cfg.pipelines_per_episode}")
        print(f"  Resource variance: {cfg.difficulty_params[difficulty]['resource_variance']}")