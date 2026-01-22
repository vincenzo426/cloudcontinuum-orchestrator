#!/usr/bin/env python3
# internal/rl/inference.py
"""
Production inference module per CloudContinuum RL Agent (Versione Corretta)

Correzioni effettuate:
1. Sostituito PPO con MaskablePPO (sb3_contrib) per compatibilità con il training.
2. Aggiunto supporto per action masking durante la predizione.
3. Ottimizzato il caricamento del modello.
"""

import os
import argparse
import numpy as np
from typing import Dict, List, Optional
from flask import Flask, request, jsonify
import logging

# IMPORTANTE: Usa MaskablePPO invece di PPO se hai usato action masking nel training
from sb3_contrib import MaskablePPO
from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize

from .environment import CloudContinuumEnv
from .config import EnvironmentConfig, DEFAULT_CLUSTERS


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RLPlacementAgent:
    """
    RL Agent per placement inference in produzione.
    Carica modello MaskablePPO e fornisce predizioni con supporto VecNormalize.
    """
    
    def __init__(
        self, 
        model_path: str, 
        vec_normalize_path: Optional[str] = None,
        config: Optional[EnvironmentConfig] = None
    ):
        logger.info(f"Loading RL model from: {model_path}")
        
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model not found: {model_path}")
        
        # Config
        if config is None:
            config = EnvironmentConfig(
                clusters=DEFAULT_CLUSTERS,
                pipelines_per_episode=1,
                max_episode_steps=1,
                master_seed=0
            )
        self.config = config
        
        # Create dummy environment
        def make_env():
            return CloudContinuumEnv(config=self.config, seed=0)
        
        self.env = DummyVecEnv([make_env])
        
        # Carica VecNormalize wrapper se presente
        if vec_normalize_path and os.path.exists(vec_normalize_path):
            logger.info(f"Loading VecNormalize from: {vec_normalize_path}")
            self.env = VecNormalize.load(vec_normalize_path, self.env)
            self.env.training = False
            self.env.norm_reward = False
            logger.info("✅ VecNormalize loaded successfully")
        else:
            logger.warning("⚠️ Running WITHOUT VecNormalize - results might be suboptimal!")
        
        # CARICAMENTO CORRETTO: Usa MaskablePPO
        self.model = MaskablePPO.load(model_path, env=self.env)
        logger.info("✅ MaskablePPO Model loaded successfully")
    
    def predict_placement(
        self,
        pipeline_request: Dict,
        clusters_state: Dict[str, Dict],
        deterministic: bool = True
    ) -> Dict:
        try:
            # 1. Costruisci observation (NON normalizzata)
            obs = self._build_observation(pipeline_request, clusters_state)
            
            # 2. Calcola Action Mask per l'inferenza
            # Questo assicura che il modello non scelga cluster che non soddisfano i requisiti
            action_masks = self._get_inference_action_mask(pipeline_request, clusters_state)
            
            # 3. Predict usando MaskablePPO con action_masks
            action, _states = self.model.predict(
                obs, 
                action_masks=action_masks, 
                deterministic=deterministic
            )
            action = int(action[0])
            
            # 4. Calcola probabilità e confidenza
            obs_tensor = self.model.policy.obs_to_tensor(obs)[0]
            with np.errstate(all='ignore'):
                # get_distribution richiede l'osservazione e opzionalmente la maschera
                distribution = self.model.policy.get_distribution(obs_tensor)
                # Applichiamo la maschera alle probabilità se necessario
                probs = distribution.distribution.probs.detach().cpu().numpy()[0]
            
            target_cluster = self.config.clusters[action].name
            confidence = float(probs[action])
            
            action_probs = {
                cluster.name: float(probs[idx])
                for idx, cluster in enumerate(self.config.clusters)
            }
            
            reason = self._generate_reason(
                pipeline_request, clusters_state, target_cluster, confidence
            )
            
            return {
                'target_cluster': target_cluster,
                'confidence': confidence,
                'reason': reason,
                'action_probabilities': action_probs
            }
        
        except Exception as e:
            logger.error(f"Prediction error: {e}", exc_info=True)
            raise

    def _get_inference_action_mask(self, pipeline_req: Dict, clusters_state: Dict) -> np.ndarray:
        """Calcola la maschera di validità dei cluster in tempo reale"""
        mask = np.ones(len(self.config.clusters), dtype=np.int8)
        cpu_req = pipeline_req.get('cpu_required', 0)
        mem_req = pipeline_req.get('memory_required', 0)

        for i, cluster_cfg in enumerate(self.config.clusters):
            state = clusters_state.get(cluster_cfg.name)
            if not state:
                mask[i] = 0
                continue
            
            # Se il cluster non ha risorse sufficienti, invalida l'azione
            if state['cpu_available'] < cpu_req or state['memory_available'] < mem_req:
                mask[i] = 0
        
        # Se tutti i cluster sono invalidi (caso estremo), permetti tutto per evitare crash
        if np.sum(mask) == 0:
            return np.ones(len(self.config.clusters), dtype=np.int8)
        return mask

    def _build_observation(self, pipeline_request: Dict, clusters_state: Dict) -> np.ndarray:
        """Costruisce il vettore di osservazione (deve matchare l'ambiente di training)"""
        obs_parts = []
        
        for cluster_cfg in self.config.clusters:
            cluster = clusters_state[cluster_cfg.name]
            
            cpu_util = 1.0 - (cluster['cpu_available'] / cluster['cpu_capacity'])
            mem_util = 1.0 - (cluster['memory_available'] / cluster['memory_capacity'])
            cpu_avail_norm = cluster['cpu_available'] / cluster['cpu_capacity']
            mem_avail_norm = cluster['memory_available'] / cluster['memory_capacity']
            
            cpu_above_usage = max(0, cluster['cpu_available'] - cluster.get('cpu_used', 0))
            mem_above_usage = max(0, cluster['memory_available'] - cluster.get('memory_used', 0))
            
            cpu_margin = cpu_above_usage / cluster['cpu_capacity']
            mem_margin = mem_above_usage / cluster['memory_capacity']
            safety_margin = (cpu_margin + mem_margin) / 2.0
            stress_level = max(cpu_util, mem_util)
            balance_score = 1.0 - abs(cpu_util - mem_util)
            headroom = min(cpu_avail_norm, mem_avail_norm)
            is_data_cluster = 1.0 if cluster_cfg.name == pipeline_request['data_location'] else 0.0
            
            obs_parts.extend([
                cpu_util, mem_util, cpu_avail_norm, mem_avail_norm,
                safety_margin, stress_level, balance_score, headroom, is_data_cluster
            ])
        
        max_cpu = max(c.cpu_capacity for c in self.config.clusters)
        max_mem = max(c.memory_capacity for c in self.config.clusters)
        
        pipeline_cpu_norm = pipeline_request['cpu_required'] / max_cpu
        pipeline_mem_norm = pipeline_request['memory_required'] / max_mem
        
        obs_parts.extend([
            pipeline_cpu_norm, pipeline_mem_norm, 
            (pipeline_cpu_norm + pipeline_mem_norm) / 2.0, 
            0.0, 1.0, 
            1.0 if pipeline_request['data_location'] != "none" else 0.0
        ])
        
        # Temporal features (5 dummy zeros come nel training)
        obs_parts.extend([0.0] * 5)
        
        return np.array(obs_parts, dtype=np.float32).reshape(1, -1)

    def _generate_reason(self, pipeline_request: Dict, clusters_state: Dict, target: str, conf: float) -> str:
        reasons = []
        if target == pipeline_request['data_location']: reasons.append("data locality")
        target_state = clusters_state[target]
        cpu_util = 1.0 - (target_state['cpu_available'] / target_state['cpu_capacity'])
        reasons.append("optimal load" if cpu_util < 0.7 else "high capacity")
        reasons.append(f"conf: {conf:.1%}")
        return f"RL: {', '.join(reasons)}"


# Flask API
app = Flask(__name__)
agent: Optional[RLPlacementAgent] = None

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'model_loaded': agent is not None,
        'model_type': 'MaskablePPO' if agent else None
    })

@app.route('/predict', methods=['POST'])
def predict():
    if agent is None:
        return jsonify({'error': 'Model not loaded'}), 500
    try:
        data = request.get_json()
        result = agent.predict_placement(data['pipeline'], data['clusters'])
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-path", type=str, required=True)
    parser.add_argument("--vec-normalize-path", type=str, default=None)
    parser.add_argument("--host", type=str, default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5000)
    args = parser.parse_args()
    
    global agent
    agent = RLPlacementAgent(args.model_path, args.vec_normalize_path)
    app.run(host=args.host, port=args.port, debug=False)

if __name__ == "__main__":
    main()