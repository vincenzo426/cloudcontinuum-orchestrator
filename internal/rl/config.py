# internal/rl/config.py
"""CloudContinuum RL - Configuration Module."""

from dataclasses import dataclass, field
from typing import List, Optional, Tuple


# === CLUSTER CONFIGURATION ===

@dataclass
class ClusterConfig:
    """Single Kubernetes cluster configuration."""
    
    name: str
    cpu_capacity: int           # millicores
    memory_capacity: int        # bytes
    cluster_type: str           # "cloud" or "edge"
    baseline_cpu_requested: int = 0
    baseline_memory_requested: int = 0
    latency_to_cloud: float = 0.0  # ms
    
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


# === DEFAULT CLUSTERS ===

DEFAULT_CLUSTERS: List[ClusterConfig] = [
    ClusterConfig(
        name="cloud_cluster",
        cpu_capacity=8000,
        memory_capacity=16739680256,
        cluster_type="cloud",
        baseline_cpu_requested=4905,
        baseline_memory_requested=11001659392,
        latency_to_cloud=0.0,
    ),
    ClusterConfig(
        name="edge_cluster_1",
        cpu_capacity=6000,
        memory_capacity=16739696640,
        cluster_type="edge",
        baseline_cpu_requested=3605,
        baseline_memory_requested=8912896000,
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


# === PIPELINE TEMPLATES ===

# Data size ranges in bytes (affects transfer time when data is not local)
MB = 1024 * 1024
GB = 1024 * MB

PIPELINE_TEMPLATES: List[dict] = [
    # Small - fits on all edges, small data
    {"name": "data_preprocessing", "cpu_range": (200, 600),
     "memory_range": (256*MB, 768*MB),
     "data_size_range": (10*MB, 100*MB),
     "weight": 0.30, "expected_size": "small"},
    {"name": "feature_extraction", "cpu_range": (400, 900),
     "memory_range": (512*MB, 1*GB),
     "data_size_range": (50*MB, 200*MB),
     "weight": 0.25, "expected_size": "small"},
    # Medium - fits on some edges, medium data
    {"name": "model_inference", "cpu_range": (800, 1500),
     "memory_range": (1*GB, 2*GB),
     "data_size_range": (100*MB, 500*MB),
     "weight": 0.25, "expected_size": "medium"},
    # Large - cloud only, large data
    {"name": "model_training", "cpu_range": (1500, 2500),
     "memory_range": (2*GB, 4*GB),
     "data_size_range": (500*MB, 2*GB),
     "weight": 0.15, "expected_size": "large"},
    {"name": "batch_processing", "cpu_range": (2000, 3000),
     "memory_range": (3*GB, 5*GB),
     "data_size_range": (1*GB, 5*GB),
     "weight": 0.05, "expected_size": "large"},
]


# === PIPELINE SIZE CLASSIFICATION ===

class PipelineSizeCategory:
    """Pipeline size classification based on CPU thresholds."""
    
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"
    
    THRESHOLD_SMALL = 1000   # < 1000m = small
    THRESHOLD_LARGE = 2000   # >= 2000m = large
    
    @staticmethod
    def classify(cpu_required: int, memory_required: int,
                 clusters: List[ClusterConfig] = None) -> Tuple[str, float]:
        """Classify pipeline by CPU thresholds. Returns (category, numeric_value)."""
        if cpu_required < PipelineSizeCategory.THRESHOLD_SMALL:
            return PipelineSizeCategory.SMALL, 0.0
        elif cpu_required >= PipelineSizeCategory.THRESHOLD_LARGE:
            return PipelineSizeCategory.LARGE, 1.0
        return PipelineSizeCategory.MEDIUM, 0.5


# === ENVIRONMENT CONFIGURATION ===

@dataclass
class EnvironmentConfig:
    """
    Gymnasium environment configuration.
    
    State space: 28 features (5 per cluster × 4 + 4 pipeline + 4 global)
    Action space: Discrete(4) - cluster selection
    """
    
    # Clusters
    clusters: List[ClusterConfig] = field(default_factory=lambda: DEFAULT_CLUSTERS.copy())
    
    # Episode parameters
    pipelines_per_episode: int = 8
    max_episode_steps: int = 50
    
    # Curriculum parameters
    baseline_load_variation: float = 0.0
    data_locality_probability: float = 0.5
    pipeline_size_multiplier: float = 1.0
    
    # State space (now 29 features: added data_size_ratio)
    features_per_cluster: int = 5
    features_pipeline: int = 5  # +1 for data_size_ratio
    features_global: int = 4
    
    # Max data size for normalization (5 GB)
    max_data_size: int = 5 * 1024 * 1024 * 1024
    
    # Reward - failures
    reward_invalid_action: float = -1.0
    reward_placement_failed: float = -0.8
    
    # Reward - execution time (primary objective)
    reward_time_weight: float = 0.3
    
    # Reward - data locality
    reward_data_locality: float = 0.2
    penalty_remote_placement: float = -0.1
    
    # Reward - strategic placement (anti-myopic)
    penalty_small_on_cloud: float = -0.8
    bonus_large_on_cloud: float = 0.5
    bonus_small_on_edge: float = 0.3
    bonus_medium_on_edge: float = 0.15
    
    # Reward - balancing
    reward_balanced_utilization: float = 0.1
    penalty_near_saturation: float = -0.15
    
    # Reward - episode end
    reward_perfect_episode: float = 0.3
    penalty_per_failure: float = -0.1
    
    # Utilization thresholds
    utilization_optimal_min: float = 0.5
    utilization_optimal_max: float = 0.8
    utilization_danger: float = 0.9
    
    # Execution time simulation
    exec_time_per_cpu_core: float = 30.0
    latency_impact_factor: float = 0.5
    contention_impact_factor: float = 0.5
    baseline_execution_time: float = 60.0
    
    # Poisson process (pipeline arrivals)
    avg_inter_arrival_time: float = 45.0
    
    # Resource release
    enable_resource_release: bool = True
    
    # Contention model
    contention_activation_threshold: float = 0.75
    enable_stochastic_failure: bool = True
    
    # Random seed
    master_seed: int = 42
    
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
        return next((c for c in self.clusters if c.cluster_type == "cloud"), None)
    
    @property
    def edge_clusters(self) -> List[ClusterConfig]:
        return [c for c in self.clusters if c.cluster_type == "edge"]
    
    @property
    def max_cpu_available(self) -> int:
        return max(c.cpu_available for c in self.clusters)
    
    @property
    def max_memory_available(self) -> int:
        return max(c.memory_available for c in self.clusters)


# === CURRICULUM LEARNING ===

def get_config_for_difficulty(difficulty: str) -> EnvironmentConfig:
    """Get configuration for curriculum difficulty level."""
    configs = {
        "easy": EnvironmentConfig(
            pipelines_per_episode=4,
            max_episode_steps=25,
            baseline_load_variation=-0.3,
            data_locality_probability=1.0,
            pipeline_size_multiplier=0.7,
            avg_inter_arrival_time=60.0,
        ),
        "medium": EnvironmentConfig(
            pipelines_per_episode=8,
            max_episode_steps=50,
            baseline_load_variation=0.0,
            data_locality_probability=0.5,
            pipeline_size_multiplier=1.0,
            avg_inter_arrival_time=45.0,
        ),
        "hard": EnvironmentConfig(
            pipelines_per_episode=12,
            max_episode_steps=75,
            baseline_load_variation=0.05,
            data_locality_probability=0.2,
            pipeline_size_multiplier=1.0,
            avg_inter_arrival_time=30.0,
        ),
    }
    if difficulty not in configs:
        raise ValueError(f"Unknown difficulty: {difficulty}")
    return configs[difficulty]


DEFAULT_CONFIG = EnvironmentConfig()