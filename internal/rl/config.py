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

# =============================================================================
# CORREZIONE: Definisci DEFAULT_CLUSTERS a livello globale per l'export
# =============================================================================
DEFAULT_CLUSTERS = [
    ClusterConfig(
        name="cloud_cluster",
        cpu_capacity=6000,            # 6 cores
        memory_capacity=16739684352,  # ~16GB (dal log)
        cluster_type="cloud",
        baseline_cpu_used=500,       # 62% utilizzato (dal log)
        baseline_memory_used=1073741824  # ~8.6GB (dal log)
    ),
    ClusterConfig(
        name="edge_cluster_1",
        cpu_capacity=6000,            # 6 cores
        memory_capacity=16739696640,  # ~16GB
        cluster_type="edge",
        baseline_cpu_used=500,       # 60% utilizzato
        baseline_memory_used=1073741824  # ~8.3GB
    ),
    ClusterConfig(
        name="edge_cluster_2",
        cpu_capacity=6000,            # 6 cores
        memory_capacity=16739692544,  # ~16GB
        cluster_type="edge",
        baseline_cpu_used=500,       # 60% utilizzato
        baseline_memory_used=1073741824  # ~8.3GB
    ),
    ClusterConfig(
        name="edge_cluster_3",
        cpu_capacity=6000,            # 6 cores
        memory_capacity=16739700736,  # ~16GB
        cluster_type="edge",
        baseline_cpu_used=500,       # 60% utilizzato
        baseline_memory_used=1073741824  # ~8.3GB
    ),
]

@dataclass
class EnvironmentConfig:
    """Configurazione Gymnasium Environment - OTTIMIZZATA v2.0"""
    
    # ========== CLUSTERS CONFIGURATION ==========
    clusters: List[ClusterConfig] = None
    
    # ========== EPISODE PARAMETERS ==========
    pipelines_per_episode: int = 8 
    max_episode_steps: int = 100
    
    # ========== STATE SPACE DIMENSIONS ==========
    state_features_per_cluster: int = 9
    state_features_global: int = 6
    state_features_temporal: int = 5
    
    # ========== REWARD SHAPING OTTIMIZZATO V3.0 (Tetris Mode) ==========
    reward_scale: float = 1.0
    
    # Penalità
    penalty_failed_placement: float = -500.0
    penalty_invalid_action: float = -300.0
    
    # ### FIX: Ridotta penalità overload perché in 'Hard' vogliamo riempire i cluster
    penalty_overload: float = -50.0       # Era -200. Puniamo solo se stiamo scoppiando (>95%)
    penalty_underutilization: float = -20.0 # Era -50. Meno grave se siamo all'inizio
    penalty_cluster_monopoly: float = -100.0
    penalty_remote_placement: float = -20.0
    
    # Reward e Bonus
    bonus_successful_placement: float = 100.0
    
    # ### FIX: Aumentata Data Locality per renderla prioritaria
    bonus_data_locality: float = 200.0            # Era 150. Locality è cruciale.
    bonus_balanced_utilization: float = 150.0     # Era 200. Bilanciamento secondario alla locality.
    
    bonus_new_cluster: float = 30.0
    bonus_new_cluster_usage: float = 50.0
    bonus_perfect_episode: float = 500.0
    
    # ### FIX: Allargato il range ideale per supportare carico elevato (Hard scenario)

    target_utilization_min: float = 0.30  # 30% - Accetta carico iniziale basso
    target_utilization_max: float = 0.95  # 95% - Accetta cluster quasi pieni (Tetris!)
    target_utilization_ideal: float = 0.70 # Target spostato leggermente in alto
    
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
            "pipelines_per_episode": 6,   # Aumentato da 4
            "resource_variance": 0.1,
            "failure_tolerance": 0.3,
        },
        "medium": {
            "pipelines_per_episode": 10,  # Aumentato da 7
            "resource_variance": 0.25,
            "failure_tolerance": 0.15,
        },
        "hard": {
            "pipelines_per_episode": 16,  
            "resource_variance": 0.45,    # Aumentata varianza
            "failure_tolerance": 0.05,
            "data_locality_probability": 0.2
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
            self.clusters = DEFAULT_CLUSTERS
        
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

    def get_difficulty_params(self) -> Dict:
        """Helper per recuperare i parametri della difficulty corrente"""
        return self.difficulty_params.get(self.difficulty, self.difficulty_params["medium"])

# ========== PIPELINE WORKLOAD TEMPLATES ==========
# AGGIORNATO: Basati su DATI REALI dal progetto CloudContinuum
# Fonte: examples/pipeline.yaml e log produzione
# Executor tipico: 100m CPU + 512MB-4GB RAM
# Pipeline tipiche: 2-10 executor = 200m-1000m CPU totali, 1-8GB RAM totali

PIPELINE_TEMPLATES = {
    "light": {
        "cpu_range": (200, 600),
        "memory_range": (0.5, 2),
        "probability": 0.4,            # Ridotto leggermente (era 0.5)
        "description": "2-6 executors, preprocessing/inference leggero"
    },
    "medium": {
        "cpu_range": (600, 1200),
        "memory_range": (2, 5),
        "probability": 0.4,            # Aumentato (era 0.35) - Più carico medio
        "description": "6-12 executors, training moderato"
    },
    "heavy": {
        "cpu_range": (1500, 2500),     # AUMENTATO MAX: Da 2000 a 2500
        "memory_range": (5, 8),        # Aumentato leggermente RAM
        "probability": 0.2,            # Aumentato (era 0.15) - Più heavy
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