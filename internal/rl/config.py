# internal/rl/config.py
"""
CloudContinuum RL - Configuration Module
VERSIONE 4.0 - ANTI-MYOPIC ARCHITECTURE

Architettura ottimizzata per:
1. Minimizzare tempo di esecuzione
2. Evitare scelte miopi (non sprecare cloud con pipeline piccole)
3. Bilanciamento carico tra cluster
4. Data locality come fattore secondario (impatta il tempo)

State Space: 28 features
Reward: Orientato al tempo + penalità strategiche
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple


# =============================================================================
# CLUSTER CONFIGURATION
# =============================================================================

@dataclass
class ClusterConfig:
    """Configurazione singolo cluster Kubernetes."""
    
    name: str
    cpu_capacity: int          # millicores
    memory_capacity: int       # bytes
    cluster_type: str          # "cloud" o "edge"
    
    # Baseline: risorse GIÀ RICHIESTE da workload esistenti
    baseline_cpu_requested: int = 0
    baseline_memory_requested: int = 0
    
    # Network latency verso cloud (ms)
    latency_to_cloud: float = 0.0
    
    def __post_init__(self):
        if self.cluster_type not in ("cloud", "edge"):
            raise ValueError(f"cluster_type must be 'cloud' or 'edge', got {self.cluster_type}")
    
    @property
    def cpu_available(self) -> int:
        return max(0, self.cpu_capacity - self.baseline_cpu_requested)
    
    @property
    def memory_available(self) -> int:
        return max(0, self.memory_capacity - self.baseline_memory_requested)
    
    @property
    def cpu_utilization(self) -> float:
        return self.baseline_cpu_requested / self.cpu_capacity if self.cpu_capacity > 0 else 0.0
    
    @property
    def memory_utilization(self) -> float:
        return self.baseline_memory_requested / self.memory_capacity if self.memory_capacity > 0 else 0.0


# =============================================================================
# DEFAULT CLUSTERS - Valori REALI dal tuo ambiente
# =============================================================================

DEFAULT_CLUSTERS: List[ClusterConfig] = [
    ClusterConfig(
        name="cloud_cluster",
        cpu_capacity=8000,
        memory_capacity=16739680256,          # ~15.59 GB
        cluster_type="cloud",
        baseline_cpu_requested=4905,          # ~61%
        baseline_memory_requested=11001659392, # ~65%
        latency_to_cloud=0.0,
    ),
    ClusterConfig(
        name="edge_cluster_1",
        cpu_capacity=6000,
        memory_capacity=16739696640,
        cluster_type="edge",
        baseline_cpu_requested=3605,          # ~60%
        baseline_memory_requested=8912896000, # ~53%
        latency_to_cloud=15.0,
    ),
    ClusterConfig(
        name="edge_cluster_2",
        cpu_capacity=6000,
        memory_capacity=16739692544,
        cluster_type="edge",
        baseline_cpu_requested=3605,
        baseline_memory_requested=8912896000,
        latency_to_cloud=20.0,
    ),
    ClusterConfig(
        name="edge_cluster_3",
        cpu_capacity=6000,
        memory_capacity=16739700736,
        cluster_type="edge",
        baseline_cpu_requested=3605,
        baseline_memory_requested=8912896000,
        latency_to_cloud=25.0,
    ),
]


# =============================================================================
# PIPELINE TEMPLATES - Calibrate per spazio disponibile reale
# =============================================================================

# Spazio disponibile:
# - Cloud: ~3095m CPU, ~5.3GB RAM
# - Edge:  ~2395m CPU, ~7.3GB RAM

PIPELINE_TEMPLATES: List[Dict] = [
    # SMALL - Entrano su tutti gli edge
    {
        "name": "data_preprocessing",
        "cpu_range": (200, 600),
        "memory_range": (256 * 1024 * 1024, 768 * 1024 * 1024),
        "weight": 0.30,
        "expected_size": "small",
    },
    {
        "name": "feature_extraction",
        "cpu_range": (400, 900),
        "memory_range": (512 * 1024 * 1024, 1024 * 1024 * 1024),
        "weight": 0.25,
        "expected_size": "small",
    },
    
    # MEDIUM - Entrano su alcuni edge
    {
        "name": "model_inference",
        "cpu_range": (800, 1500),
        "memory_range": (1 * 1024 * 1024 * 1024, 2 * 1024 * 1024 * 1024),
        "weight": 0.25,
        "expected_size": "medium",
    },
    
    # LARGE - Potrebbero entrare SOLO su cloud
    {
        "name": "model_training",
        "cpu_range": (1500, 2500),
        "memory_range": (2 * 1024 * 1024 * 1024, 4 * 1024 * 1024 * 1024),
        "weight": 0.15,
        "expected_size": "large",
    },
    {
        "name": "batch_processing",
        "cpu_range": (2000, 3000),
        "memory_range": (3 * 1024 * 1024 * 1024, 5 * 1024 * 1024 * 1024),
        "weight": 0.05,
        "expected_size": "large",
    },
]


# =============================================================================
# PIPELINE SIZE CLASSIFICATION
# =============================================================================

class PipelineSizeCategory:
    """
    Classificazione dimensione pipeline per decisioni strategiche.
    
    IMPORTANTE: Usa soglie ASSOLUTE basate sulla dimensione della pipeline,
    non su dove può entrare. Questo garantisce comportamento consistente
    indipendentemente dallo stato dei cluster.
    
    Soglie basate sui PIPELINE_TEMPLATES:
    - Small: < 1000m CPU (data_preprocessing, feature_extraction)
    - Medium: 1000-2000m CPU (model_inference)
    - Large: >= 2000m CPU (model_training, batch_processing)
    """
    SMALL = "small"    # Pipeline leggere, dovrebbero andare su edge
    MEDIUM = "medium"  # Pipeline medie, preferibilmente edge se c'è spazio
    LARGE = "large"    # Pipeline pesanti, richiedono cloud
    
    # Soglie assolute in millicores
    THRESHOLD_SMALL = 1000   # < 1000m = small
    THRESHOLD_LARGE = 2000   # >= 2000m = large, altrimenti medium
    
    @staticmethod
    def classify(cpu_required: int, memory_required: int, 
                 clusters: List[ClusterConfig] = None) -> Tuple[str, float]:
        """
        Classifica pipeline basandosi su SOGLIE ASSOLUTE di CPU.
        
        Args:
            cpu_required: CPU richiesta in millicores
            memory_required: Memoria richiesta (non usata per classificazione)
            clusters: Lista cluster (non usata, mantenuta per compatibilità)
        
        Returns:
            (category, numeric_value) dove numeric_value è 0.0/0.5/1.0
        """
        if cpu_required < PipelineSizeCategory.THRESHOLD_SMALL:
            return PipelineSizeCategory.SMALL, 0.0
        elif cpu_required >= PipelineSizeCategory.THRESHOLD_LARGE:
            return PipelineSizeCategory.LARGE, 1.0
        else:
            return PipelineSizeCategory.MEDIUM, 0.5


# =============================================================================
# ENVIRONMENT CONFIGURATION
# =============================================================================

@dataclass
class EnvironmentConfig:
    """
    Configurazione Gymnasium Environment.
    
    VERSIONE 4.0 - Anti-Myopic Architecture
    
    STATE SPACE (28 features):
    ─────────────────────────────────────────────────────────────────
    Per cluster (5 features × 4 cluster = 20):
        [0] cpu_available_ratio      - CPU libera (0-1)
        [1] memory_available_ratio   - RAM libera (0-1)
        [2] is_data_local           - Dati qui? (0/1)
        [3] can_fit_pipeline        - Pipeline ci entra? (0/1)
        [4] is_cloud                - È il cloud? (0/1)
    
    Pipeline features (4):
        [20] pipeline_cpu_ratio      - Dimensione CPU normalizzata
        [21] pipeline_memory_ratio   - Dimensione RAM normalizzata
        [22] pipeline_size_category  - small=0, medium=0.5, large=1
        [23] fits_on_any_edge       - Può stare su almeno 1 edge? (0/1)
    
    Global features (4):
        [24] cloud_cpu_headroom     - Spazio strategico cloud (0-1)
        [25] avg_edge_utilization   - Media utilizzo edge (0-1)
        [26] utilization_variance   - Sbilanciamento cluster (0-1)
        [27] pipelines_remaining    - Pipeline rimaste (0-1)
    ─────────────────────────────────────────────────────────────────
    """
    
    # ==================== CLUSTERS ====================
    clusters: List[ClusterConfig] = field(default_factory=lambda: DEFAULT_CLUSTERS.copy())
    
    # ==================== EPISODE PARAMETERS ====================
    pipelines_per_episode: int = 8
    max_episode_steps: int = 50
    
    # ==================== CURRICULUM PARAMETERS ====================
    baseline_load_variation: float = 0.0
    data_locality_probability: float = 0.5
    pipeline_size_multiplier: float = 1.0
    
    # ==================== STATE SPACE ====================
    features_per_cluster: int = 5
    features_pipeline: int = 4
    features_global: int = 4
    
    # ==================== REWARD SHAPING (Anti-Myopic) ====================
    
    # --- Fallimenti ---
    reward_invalid_action: float = -1.0
    reward_placement_failed: float = -0.8
    
    # --- Tempo di Esecuzione (Obiettivo Primario) ---
    # reward = reward_time_weight * (2.0 - normalized_exec_time)
    # Se exec_time = baseline → reward = 0.3 * (2.0 - 1.0) = 0.3
    # Se exec_time = 0.5*baseline → reward = 0.3 * (2.0 - 0.5) = 0.45
    # Se exec_time = 1.5*baseline → reward = 0.3 * (2.0 - 1.5) = 0.15
    reward_time_weight: float = 0.3
    
    # --- Data Locality (Impatta tempo, reward secondario) ---
    reward_data_locality: float = 0.2
    penalty_remote_placement: float = -0.1
    
    # --- Strategic Placement (Anti-Miopatia) ---
    # PESI MOLTO FORTI: devono DOMINARE tutti gli altri reward!
    # Penalità per pipeline PICCOLE su CLOUD (spreco risorse strategiche)
    penalty_small_on_cloud: float = -0.8     # Era -0.5, ancora più forte!
    # Bonus per pipeline GRANDI su CLOUD (uso appropriato)
    bonus_large_on_cloud: float = 0.5        # Era 0.35, aumentato!
    # Bonus per pipeline PICCOLE su EDGE (scelta corretta)
    bonus_small_on_edge: float = 0.3         # Era 0.2, aumentato
    # Bonus per pipeline MEDIE su EDGE (anche loro dovrebbero andare su edge se possibile)
    bonus_medium_on_edge: float = 0.15       # NUOVO!
    
    # --- Bilanciamento ---
    reward_balanced_utilization: float = 0.1
    penalty_near_saturation: float = -0.15
    
    # --- Fine Episodio ---
    reward_perfect_episode: float = 0.3
    penalty_per_failure: float = -0.1
    
    # ==================== UTILIZATION THRESHOLDS ====================
    utilization_optimal_min: float = 0.5
    utilization_optimal_max: float = 0.8
    utilization_danger: float = 0.9
    
    # ==================== EXECUTION TIME SIMULATION ====================
    # Tempo base: ~30 secondi per CPU core richiesto
    exec_time_per_cpu_core: float = 30.0
    # Fattore latenza: quanto la latency di rete impatta il tempo
    latency_impact_factor: float = 0.5
    # Fattore contesa: quanto un cluster carico rallenta l'esecuzione
    contention_impact_factor: float = 0.5
    
    # ==================== POISSON PROCESS (Arrivo Pipeline) ====================
    # Tempo medio tra arrivi di pipeline (secondi) - distribuzione esponenziale
    # Valore basso = burst frequenti, valore alto = arrivi diluiti
    avg_inter_arrival_time: float = 45.0
    
    # ==================== RESOURCE RELEASE (Rilascio Risorse) ====================
    # Le pipeline terminano e rilasciano risorse dopo exec_time
    enable_resource_release: bool = True
    
    # ==================== CONTENTION MODEL (Probabilità Fallimento) ====================
    # Soglia di utilizzo oltre la quale il modello di contesa viene attivato
    contention_activation_threshold: float = 0.85
    # Se True, un cluster molto carico può far fallire il placement stocasticamente
    enable_stochastic_failure: bool = True
    
    # ==================== BASELINE (Reference) ====================
    # Tempo di esecuzione baseline per reference (non usato nel reward, usa ideal_time)
    baseline_execution_time: float = 60.0
    
    # ==================== RANDOM SEED ====================
    master_seed: int = 42
    
    # ==================== COMPUTED PROPERTIES ====================
    @property
    def num_clusters(self) -> int:
        return len(self.clusters)
    
    @property
    def total_state_size(self) -> int:
        return (self.features_per_cluster * self.num_clusters) + \
               self.features_pipeline + self.features_global
    
    @property
    def cluster_names(self) -> List[str]:
        return [c.name for c in self.clusters]
    
    @property
    def cloud_cluster(self) -> Optional[ClusterConfig]:
        for c in self.clusters:
            if c.cluster_type == "cloud":
                return c
        return None
    
    @property
    def edge_clusters(self) -> List[ClusterConfig]:
        return [c for c in self.clusters if c.cluster_type == "edge"]
    
    @property
    def max_cpu_available(self) -> int:
        return max(c.cpu_available for c in self.clusters)
    
    @property
    def max_memory_available(self) -> int:
        return max(c.memory_available for c in self.clusters)


# =============================================================================
# CURRICULUM LEARNING
# =============================================================================

def get_config_for_difficulty(difficulty: str) -> EnvironmentConfig:
    """
    Ritorna configurazione per livello di difficoltà.
    
    CURRICULUM REALISTICO - Ogni stage cambia l'ambiente in modo significativo:
    
    EASY:
        - 4 pipeline per episodio
        - Cluster con 30% meno carico (più spazio)
        - Data locality sempre chiara (100%)
        - Pipeline piccole (×0.7)
        - Arrivi LENTI (60s) → più tempo per liberare risorse
        → L'agente impara le basi: action masking, data locality
    
    MEDIUM:
        - 8 pipeline per episodio
        - Carico reale dei cluster
        - Data locality 50%
        - Pipeline normali
        - Arrivi MEDI (45s) → equilibrio realistico
        → L'agente impara: bilanciamento, quando usare cloud vs edge
    
    HARD:
        - 12 pipeline per episodio
        - Cluster con 15% più carico (meno spazio)
        - Data locality rara (20%)
        - Pipeline grandi (×1.2)
        - Arrivi VELOCI (30s) → burst frequenti, stress test
        → L'agente impara: decisioni strategiche anti-miopatia
    """
    
    if difficulty == "easy":
        return EnvironmentConfig(
            pipelines_per_episode=4,
            max_episode_steps=25,
            baseline_load_variation=-0.3,
            data_locality_probability=1.0,
            pipeline_size_multiplier=0.7,
            avg_inter_arrival_time=60.0,  # Arrivi lenti → ambiente rilassato
        )
    
    elif difficulty == "medium":
        return EnvironmentConfig(
            pipelines_per_episode=8,
            max_episode_steps=50,
            baseline_load_variation=0.0,
            data_locality_probability=0.5,
            pipeline_size_multiplier=1.0,
            avg_inter_arrival_time=45.0,  # Equilibrio realistico
        )
    
    elif difficulty == "hard":
        # BILANCIATO: difficile ma non impossibile
        # Con questi valori:
        # - Cloud: 4905 + (4905 × 0.05) = 5150m usati → 2850m disponibili
        # - Edge: 3605 + (3605 × 0.05) = 3785m usati → 2215m disponibili
        # - Pipeline large max: 2500 × 1.0 = 2500m → ENTRA su cloud!
        return EnvironmentConfig(
            pipelines_per_episode=12,
            max_episode_steps=75,
            baseline_load_variation=0.05,    # Era 0.15, ridotto! Cluster con solo 5% più carico
            data_locality_probability=0.2,
            pipeline_size_multiplier=1.0,    # Era 1.1, ora normale (no scaling)
            avg_inter_arrival_time=30.0,     # Burst frequenti → stress test
        )
    
    else:
        raise ValueError(f"Unknown difficulty: {difficulty}")


# =============================================================================
# DEFAULT CONFIG
# =============================================================================

DEFAULT_CONFIG = EnvironmentConfig()


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("CloudContinuum RL - Config v4.0 (Anti-Myopic Architecture)")
    print("=" * 70)
    
    config = DEFAULT_CONFIG
    
    print(f"\n📊 Clusters ({config.num_clusters}):")
    print("-" * 70)
    for c in config.clusters:
        print(f"  {c.name} [{c.cluster_type}]:")
        print(f"    CPU:  {c.cpu_available}m / {c.cpu_capacity}m available "
              f"({c.cpu_utilization*100:.1f}% used)")
        print(f"    MEM:  {c.memory_available/(1024**3):.2f}GB / "
              f"{c.memory_capacity/(1024**3):.2f}GB available")
        print(f"    Latency to cloud: {c.latency_to_cloud}ms")
    
    print(f"\n📐 State Space: {config.total_state_size} features")
    print(f"    Per cluster: {config.features_per_cluster} × {config.num_clusters} = "
          f"{config.features_per_cluster * config.num_clusters}")
    print(f"    Pipeline: {config.features_pipeline}")
    print(f"    Global: {config.features_global}")
    
    print(f"\n🎯 Reward Structure (Anti-Myopic):")
    print(f"    Time weight:        {config.reward_time_weight}")
    print(f"    Data locality:     +{config.reward_data_locality}")
    print(f"    Small on cloud:     {config.penalty_small_on_cloud} (PENALTY)")
    print(f"    Large on cloud:    +{config.bonus_large_on_cloud} (BONUS)")
    print(f"    Small on edge:     +{config.bonus_small_on_edge} (BONUS)")
    
    print(f"\n📦 Pipeline Size Classification:")
    for template in PIPELINE_TEMPLATES:
        cpu_avg = sum(template["cpu_range"]) / 2
        print(f"    {template['name']}: ~{cpu_avg:.0f}m CPU → {template['expected_size']}")
    
    print(f"\n📚 Curriculum Learning:")
    for diff in ["easy", "medium", "hard"]:
        cfg = get_config_for_difficulty(diff)
        print(f"  {diff.upper():6s}: {cfg.pipelines_per_episode} pipelines, "
              f"load {cfg.baseline_load_variation:+.0%}, "
              f"locality {cfg.data_locality_probability:.0%}, "
              f"arrivals ~{cfg.avg_inter_arrival_time:.0f}s")
    
    print("\n" + "=" * 70)
    print("✅ Configuration OK!")
    print("=" * 70)