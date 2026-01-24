# internal/rl/config.py
"""
Configurazione centralizzata per RL-based pipeline placement
VERSIONE 2.2 - FIX CLOUD BIAS & BALANCED REWARDS

CHANGELOG v2.2:
- Fix reward imbalance: penalty_remote_placement aumentato -20 → -180
- Aggiunti penalty_edge_imbalance e bonus_cloud_usage
- Target utilization range ristretto per forzare bilanciamento
- Baseline cluster bilanciato (cloud meno carico iniziale)
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
    
    # Utilizzo corrente (ex baseline)
    cpu_used: int = 0
    memory_used: int = 0

# =============================================================================
# DEFAULT_CLUSTERS - BILANCIATO v2.2
# =============================================================================
DEFAULT_CLUSTERS = [
    ClusterConfig(
        name="cloud_cluster",
        cpu_capacity=8000,            # 8 cores
        memory_capacity=16739684352,  # ~16GB
        cluster_type="cloud",
        cpu_used=400,                 # ⚠️ RIDOTTO da 700 → 5% utilizzato (più attraente)
        memory_used=805306368         # ~750MB
    ),
    ClusterConfig(
        name="edge_cluster_1",
        cpu_capacity=6000,            # 6 cores
        memory_capacity=16739696640,  # ~16GB
        cluster_type="edge",
        cpu_used=450,                 # 7.5% utilizzato
        memory_used=1073741824        # ~1GB
    ),
    ClusterConfig(
        name="edge_cluster_2",
        cpu_capacity=6000,            # 6 cores
        memory_capacity=16739692544,  # ~16GB
        cluster_type="edge",
        cpu_used=450,                 # 7.5% utilizzato
        memory_used=1073741824        # ~1GB
    ),
    ClusterConfig(
        name="edge_cluster_3",
        cpu_capacity=6000,            # 6 cores
        memory_capacity=16739700736,  # ~16GB
        cluster_type="edge",
        cpu_used=450,                 # 7.5% utilizzato
        memory_used=1073741824        # ~1GB
    ),
]

@dataclass
class EnvironmentConfig:
    """Configurazione Gymnasium Environment - BALANCED v2.2"""
    
    # ========== CLUSTERS CONFIGURATION ==========
    clusters: List[ClusterConfig] = None
    
    # ========== EPISODE PARAMETERS ==========
    pipelines_per_episode: int = 8 
    max_episode_steps: int = 100
    
    # ========== STATE SPACE DIMENSIONS ==========
    state_features_per_cluster: int = 9
    state_features_global: int = 9  # ⚠️ AUMENTATO da 6 → 9 (+3 features balance)
    state_features_temporal: int = 5
    
    # ========== REWARD SHAPING V4.0 - BALANCED & CLOUD-FRIENDLY ==========
    reward_scale: float = 1.0
    
    # Penalità
    penalty_failed_placement: float = -500.0
    penalty_invalid_action: float = -300.0
    penalty_overload: float = -50.0
    penalty_underutilization: float = -20.0
    
    # ⚠️ FIX CRITICO #1: Monopoly più severa
    penalty_cluster_monopoly: float = -200.0  # Era -100
    
    # ⚠️ FIX CRITICO #2: Remote placement MOLTO più costoso
    penalty_remote_placement: float = -180.0  # Era -20 → Ora -180
    
    # ⚠️ FIX CRITICO #3: Penalità per squilibrio edge clusters
    penalty_edge_imbalance: float = -120.0  # NUOVO
    
    # Reward e Bonus
    bonus_successful_placement: float = 100.0
    bonus_data_locality: float = 200.0
    bonus_balanced_utilization: float = 150.0
    bonus_new_cluster: float = 30.0
    bonus_new_cluster_usage: float = 50.0
    
    # ⚠️ FIX CRITICO #4: Bonus per uso cloud (compensa costi percepiti)
    bonus_cloud_usage: float = 80.0  # NUOVO
    
    bonus_perfect_episode: float = 500.0
    
    # ⚠️ FIX CRITICO #5: Range utilization ristretto per forzare bilanciamento
    target_utilization_min: float = 0.45  # Era 0.30 → Più stretto
    target_utilization_max: float = 0.85  # Era 0.95 → Più stretto
    target_utilization_ideal: float = 0.70
    
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
            "pipelines_per_episode": 6,
            "resource_variance": 0.1,
            "failure_tolerance": 0.3,
        },
        "medium": {
            "pipelines_per_episode": 10,
            "resource_variance": 0.25,
            "failure_tolerance": 0.15,
        },
        "hard": {
            "pipelines_per_episode": 16,  
            "resource_variance": 0.45,
            "failure_tolerance": 0.05,
            "data_locality_probability": 0.2
        }
    })
    
    # ========== SEED MANAGEMENT ==========
    master_seed: int = 42
    
    # ========== ACTION MASKING ==========
    enable_action_masking: bool = True
    
    # ========== LOGGING & MONITORING ==========
    verbose: bool = True
    log_episode_stats: bool = True
    
    def __post_init__(self):
        if self.clusters is None:
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
            "penalty_remote_placement": self.penalty_remote_placement,
            "penalty_edge_imbalance": self.penalty_edge_imbalance,
            "bonus_successful_placement": self.bonus_successful_placement,
            "bonus_data_locality": self.bonus_data_locality,
            "bonus_balanced_utilization": self.bonus_balanced_utilization,
            "bonus_new_cluster": self.bonus_new_cluster,
            "bonus_cloud_usage": self.bonus_cloud_usage,
        }

    def get_difficulty_params(self) -> Dict:
        """Helper per recuperare i parametri della difficulty corrente"""
        return self.difficulty_params.get(self.difficulty, self.difficulty_params["medium"])

# ========== PIPELINE WORKLOAD TEMPLATES ==========
PIPELINE_TEMPLATES = {
    "light": {
        "cpu_range": (200, 600),
        "memory_range": (0.5, 2),
        "probability": 0.4,
        "description": "2-6 executors, preprocessing/inference leggero"
    },
    "medium": {
        "cpu_range": (600, 1200),
        "memory_range": (2, 5),
        "probability": 0.4,
        "description": "6-12 executors, training moderato"
    },
    "heavy": {
        "cpu_range": (1500, 2500),
        "memory_range": (5, 8),
        "probability": 0.2,
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