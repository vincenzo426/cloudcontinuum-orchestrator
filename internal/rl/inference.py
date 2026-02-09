# internal/rl/inference.py - Production Version (Standalone)
"""API Flask per inference RL - Integrazione con controller Go."""

import os
import logging
import numpy as np
from typing import Dict, Any
from flask import Flask, request, jsonify

import gymnasium as gym
from gymnasium import spaces
from sb3_contrib import MaskablePPO
from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize

from .config import ClusterConfig, DEFAULT_CLUSTERS, PipelineSizeCategory

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DummyEnv(gym.Env):
    """Environment minimale per caricare il modello."""
    def __init__(self):
        super().__init__()
        self.observation_space = spaces.Box(low=-1.0, high=2.0, shape=(28,), dtype=np.float32)
        self.action_space = spaces.Discrete(4)
    
    def reset(self, seed=None, options=None):
        return np.zeros(28, dtype=np.float32), {}
    
    def step(self, action):
        return np.zeros(28, dtype=np.float32), 0.0, True, False, {}
    
    def action_masks(self):
        return np.ones(4, dtype=bool)


class RLPlacementAgent:
    """Agente RL per placement pipeline in produzione."""
    
    def __init__(self, model_path: str, vec_normalize_path: str = None):
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model not found: {model_path}")
        
        self.clusters = DEFAULT_CLUSTERS
        self.cluster_names = [c.name for c in self.clusters]
        
        # Dummy env per caricare modello
        self.env = DummyVecEnv([lambda: DummyEnv()])
        
        if vec_normalize_path and os.path.exists(vec_normalize_path):
            self.env = VecNormalize.load(vec_normalize_path, self.env)
            self.env.training = False
            self.env.norm_reward = False
            self._has_normalize = True
        else:
            self._has_normalize = False
        
        self.model = MaskablePPO.load(model_path, env=self.env)
        logger.info(f"✅ RLPlacementAgent loaded: {model_path}")
    
    def predict(self, pipeline: Dict, clusters_state: Dict) -> Dict[str, Any]:
        """Predice cluster target per una pipeline."""
        try:
            obs = self._build_observation(pipeline, clusters_state)
            mask = self._build_action_mask(pipeline, clusters_state)
            
            if not mask.any():
                return {'target_cluster': None, 'confidence': 0.0,
                        'action_probabilities': {},
                        'reason': 'No cluster has sufficient resources', 'is_valid': False}
            
            if self._has_normalize:
                obs = self.env.normalize_obs(obs)
            
            action, _ = self.model.predict(obs, deterministic=True, action_masks=mask)
            action = int(action[0]) if hasattr(action, '__len__') else int(action)
            
            target = self.cluster_names[action]
            probs = self._get_probs(obs, mask)
            confidence = float(probs[action])
            
            size_cat, _ = PipelineSizeCategory.classify(
                pipeline.get('cpu_required', 0),
                pipeline.get('memory_required', 0)
            )
            reason = f"RL: {size_cat} pipeline → {target}, conf: {confidence:.0%}"
            
            # Action probabilities per ogni cluster
            action_probabilities = {
                name: float(probs[i]) for i, name in enumerate(self.cluster_names)
            }
            
            return {'target_cluster': target, 'confidence': confidence,
                    'action_probabilities': action_probabilities,
                    'reason': reason, 'is_valid': True}
        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            return {'target_cluster': None, 'confidence': 0.0,
                    'action_probabilities': {},
                    'reason': f'Error: {str(e)}', 'is_valid': False}
    
    def _build_observation(self, pipeline: Dict, clusters_state: Dict) -> np.ndarray:
        """Costruisce observation vector (28 features)."""
        obs = []
        cpu_req = pipeline.get('cpu_required', 0)
        mem_req = pipeline.get('memory_required', 0)
        data_loc = pipeline.get('data_location', 'distributed')
        
        # Per-cluster features (5 × 4 = 20)
        for c in self.clusters:
            state = clusters_state.get(c.name, {})
            cpu_cap = state.get('cpu_capacity', c.cpu_capacity)
            cpu_avail = state.get('cpu_available', c.cpu_available)
            mem_cap = state.get('memory_capacity', c.memory_capacity)
            mem_avail = state.get('memory_available', c.memory_available)
            
            obs.append(cpu_avail / cpu_cap if cpu_cap > 0 else 0.0)
            obs.append(mem_avail / mem_cap if mem_cap > 0 else 0.0)
            obs.append(1.0 if data_loc == c.name else 0.0)
            obs.append(1.0 if cpu_avail >= cpu_req and mem_avail >= mem_req else 0.0)
            obs.append(1.0 if c.cluster_type == "cloud" else 0.0)
        
        # Pipeline features (4)
        max_cpu = max(clusters_state.get(c.name, {}).get('cpu_available', c.cpu_available) for c in self.clusters)
        max_mem = max(clusters_state.get(c.name, {}).get('memory_available', c.memory_available) for c in self.clusters)
        _, size_val = PipelineSizeCategory.classify(cpu_req, mem_req)
        fits_edge = any(clusters_state.get(c.name, {}).get('cpu_available', 0) >= cpu_req and
                        clusters_state.get(c.name, {}).get('memory_available', 0) >= mem_req
                        for c in self.clusters if c.cluster_type == "edge")
        
        obs.append(min(cpu_req / max_cpu, 2.0) if max_cpu > 0 else 0.0)
        obs.append(min(mem_req / max_mem, 2.0) if max_mem > 0 else 0.0)
        obs.append(size_val)
        obs.append(1.0 if fits_edge else 0.0)
        
        # Global features (4)
        cloud = clusters_state.get('cloud_cluster', {})
        cloud_headroom = cloud.get('cpu_available', 0) / cloud.get('cpu_capacity', 8000)
        
        edge_utils = []
        for c in self.clusters:
            if c.cluster_type == "edge" and c.name in clusters_state:
                s = clusters_state[c.name]
                edge_utils.append(1.0 - s.get('cpu_available', 0) / s.get('cpu_capacity', 1))
        
        obs.append(cloud_headroom)
        obs.append(np.mean(edge_utils) if edge_utils else 0.0)
        # Calcola l'utilizzo di ogni cluster
        all_utils = []
        edge_utils = []
        for c in self.clusters:
            if c.name in clusters_state:
                s = clusters_state[c.name]
                cpu_util = 1.0 - (s.get('cpu_available', 0) / max(s.get('cpu_capacity', 1), 1))
                mem_util = 1.0 - (s.get('memory_available', 0) / max(s.get('memory_capacity', 1), 1))
                avg_util = (cpu_util + mem_util) / 2.0
                all_utils.append(avg_util)
                if c.cluster_type == "edge":
                    edge_utils.append(avg_util)

        # [27] Varianza normalizzata
        util_variance = np.var(all_utils) if all_utils else 0.0
        obs.append(min(util_variance * 10, 1.0))
        obs.append(1.0)  # single pipeline
        
        return np.array(obs, dtype=np.float32).reshape(1, -1)
    
    def _build_action_mask(self, pipeline: Dict, clusters_state: Dict) -> np.ndarray:
        """Action mask: True = cluster può ospitare pipeline."""
        cpu_req = pipeline.get('cpu_required', 0)
        mem_req = pipeline.get('memory_required', 0)
        mask = np.zeros(len(self.clusters), dtype=bool)
        
        for i, c in enumerate(self.clusters):
            state = clusters_state.get(c.name, {})
            if state.get('cpu_available', c.cpu_available) >= cpu_req and \
               state.get('memory_available', c.memory_available) >= mem_req:
                mask[i] = True
        return mask
    
    def _get_probs(self, obs: np.ndarray, mask: np.ndarray) -> np.ndarray:
        """Ottiene probabilità per ogni azione."""
        try:
            obs_tensor = self.model.policy.obs_to_tensor(obs)[0]
            dist = self.model.policy.get_distribution(obs_tensor)
            probs = dist.distribution.probs.detach().cpu().numpy()[0]
            probs = probs * mask
            return probs / probs.sum() if probs.sum() > 0 else probs
        except:
            probs = mask.astype(float)
            return probs / probs.sum() if probs.sum() > 0 else probs


# === FLASK API ===
app = Flask(__name__)
agent: RLPlacementAgent = None

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'healthy', 'model_loaded': agent is not None})

@app.route('/predict', methods=['POST'])
def predict():
    if agent is None:
        return jsonify({'error': 'Model not loaded'}), 503
    
    data = request.get_json()
    if not data or 'pipeline' not in data or 'clusters' not in data:
        return jsonify({'error': 'Missing pipeline or clusters'}), 400
    
    result = agent.predict(data['pipeline'], data['clusters'])
    return jsonify(result)


def run_server(model_path: str, vec_normalize_path: str = None, host: str = '0.0.0.0', port: int = 5000):
    """Avvia server Flask."""
    global agent
    agent = RLPlacementAgent(model_path, vec_normalize_path)
    logger.info(f"🚀 Starting server on {host}:{port}")
    app.run(host=host, port=port, threaded=True)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-path", required=True)
    parser.add_argument("--vec-normalize-path", default=None)
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=5000)
    args = parser.parse_args()
    
    vec_path = args.vec_normalize_path
    if not vec_path:
        vec_path = os.path.join(os.path.dirname(args.model_path), "vec_normalize.pkl")
    
    run_server(args.model_path, vec_path, args.host, args.port)