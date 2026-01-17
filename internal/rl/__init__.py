"""
CloudContinuum RL Package

Moduli:
- config: Configurazione ambiente e parametri
- environment: Gymnasium environment per training
- simulator: Modelli di simulazione (execution time, network, resources)
- pretrain: Imitation learning da expert policy
- train: Training loop con curriculum learning
- inference: API Flask per integrazione produzione
"""

__version__ = "1.0.0"

from .config import (
    EnvironmentConfig,
    ClusterConfig,
    DEFAULT_CONFIG,
    PIPELINE_TEMPLATES,
    get_config_for_difficulty
)

from .environment import CloudContinuumEnv

from .inference import RLPlacementAgent

__all__ = [
    'EnvironmentConfig',
    'ClusterConfig',
    'DEFAULT_CONFIG',
    'PIPELINE_TEMPLATES',
    'get_config_for_difficulty',
    'CloudContinuumEnv',
    'RLPlacementAgent'
]