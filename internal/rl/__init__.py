# internal/rl/__init__.py
"""
CloudContinuum RL Package
VERSIONE 4.0 - ANTI-MYOPIC ARCHITECTURE

Package per training e inference di agente RL per placement intelligente
di pipeline ML su cluster Kubernetes distribuiti.

MODULI:
    config      - Configurazione cluster, environment, reward
    simulator   - Simulazione tempo esecuzione e latenza rete
    environment - Gymnasium environment per training
    train       - Training script con curriculum learning
    inference   - API Flask per produzione

OBIETTIVI DELL'AGENTE:
    1. Minimizzare tempo di esecuzione
    2. Evitare scelte miopi (non sprecare cloud con pipeline piccole)
    3. Bilanciare carico tra cluster
    4. Rispettare data locality

QUICK START:
    # Training
    python -m internal.rl.train --save-dir ./models
    
    # Evaluation
    python -m internal.rl.train --eval-only --model-path ./models/stage3_hard/final_model.zip
    
    # Inference server
    python -m internal.rl.inference --model-path ./models/stage3_hard/final_model.zip

USAGE IN CODE:
    from internal.rl import (
        CloudContinuumEnv,
        RLPlacementAgent,
        get_config_for_difficulty
    )
    
    # Training
    env = CloudContinuumEnv(config=get_config_for_difficulty("medium"))
    
    # Inference
    agent = RLPlacementAgent(model_path="./models/stage3_hard/final_model.zip")
    result = agent.predict(pipeline, clusters_state)
"""

__version__ = "4.0.0"
__author__ = "CloudContinuum Team"

# internal/rl/__init__.py
"""CloudContinuum RL - Reinforcement Learning for adaptive pipeline placement."""

from .config import (
    ClusterConfig,
    EnvironmentConfig,
    PipelineSizeCategory,
    DEFAULT_CLUSTERS,
    DEFAULT_CONFIG,
    PIPELINE_TEMPLATES,
    get_config_for_difficulty,
    MB, GB,  # Data size constants
)
from .simulator import (
    NetworkLatencyModel,
    ExecutionTimeSimulator,
    ResourceContentionModel,
    SimulatedClusterState,
    estimate_optimal_cluster,
)
from .environment import CloudContinuumEnv
from .inference import RLPlacementAgent, run_server

__all__ = [
    # Config
    "ClusterConfig",
    "EnvironmentConfig", 
    "PipelineSizeCategory",
    "DEFAULT_CLUSTERS",
    "DEFAULT_CONFIG",
    "PIPELINE_TEMPLATES",
    "get_config_for_difficulty",
    "MB", "GB",
    # Simulator
    "NetworkLatencyModel",
    "ExecutionTimeSimulator",
    "ResourceContentionModel",
    "SimulatedClusterState",
    "estimate_optimal_cluster",
    # Environment
    "CloudContinuumEnv",
    # Inference
    "RLPlacementAgent",
    "run_server",
]