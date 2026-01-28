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

# =============================================================================
# CONFIG EXPORTS
# =============================================================================

from .config import (
    # Dataclasses
    ClusterConfig,
    EnvironmentConfig,
    
    # Constants
    DEFAULT_CLUSTERS,
    DEFAULT_CONFIG,
    PIPELINE_TEMPLATES,
    
    # Utilities
    PipelineSizeCategory,
    get_config_for_difficulty,
)

# =============================================================================
# SIMULATOR EXPORTS
# =============================================================================

from .simulator import (
    NetworkLatencyModel,
    ExecutionTimeSimulator,
    ResourceContentionModel,
    SimulatedClusterState,
)

# =============================================================================
# ENVIRONMENT EXPORTS
# =============================================================================

from .environment import CloudContinuumEnv

# =============================================================================
# INFERENCE EXPORTS
# =============================================================================

from .inference import RLPlacementAgent

# =============================================================================
# ALL EXPORTS
# =============================================================================

__all__ = [
    # Version
    "__version__",
    
    # Config
    "ClusterConfig",
    "EnvironmentConfig",
    "DEFAULT_CLUSTERS",
    "DEFAULT_CONFIG",
    "PIPELINE_TEMPLATES",
    "PipelineSizeCategory",
    "get_config_for_difficulty",
    
    # Simulator
    "NetworkLatencyModel",
    "ExecutionTimeSimulator",
    "ResourceContentionModel",
    "SimulatedClusterState",
    
    # Environment
    "CloudContinuumEnv",
    
    # Inference
    "RLPlacementAgent",
]