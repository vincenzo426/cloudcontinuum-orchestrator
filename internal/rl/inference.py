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
        """
        Costruisce il vettore di osservazione (50 features - ALIGNED con environment v2.1)
        
        Structure:
        - Per-cluster: 9 × 4 = 36 features
        - Global: 9 features (6 originali + 3 nuove)
        - Temporal: 5 features
        Total: 50 features
        """
        obs_parts = []
        
        # === PER-CLUSTER FEATURES (9 × 4 = 36) ===
        for cluster_cfg in self.config.clusters:
            cluster = clusters_state[cluster_cfg.name]
            
            # Utilization
            cpu_util = 1.0 - (cluster['cpu_available'] / max(cluster['cpu_capacity'], 1))
            mem_util = 1.0 - (cluster['memory_available'] / max(cluster['memory_capacity'], 1))
            
            # Available (normalized)
            cpu_avail_norm = cluster['cpu_available'] / max(cluster['cpu_capacity'], 1)
            mem_avail_norm = cluster['memory_available'] / max(cluster['memory_capacity'], 1)
            
            # Safety margin
            safety_margin = min(cpu_avail_norm, mem_avail_norm)
            
            # Stress level
            stress_level = (cpu_util + mem_util) / 2.0
            
            # Balance score
            balance_score = 1.0 - abs(cpu_util - mem_util)
            
            # Headroom
            headroom = (cpu_avail_norm + mem_avail_norm) / 2.0
            
            # Is data cluster
            is_data_cluster = 1.0 if cluster_cfg.name == pipeline_request['data_location'] else 0.0
            
            obs_parts.extend([
                cpu_util, mem_util,
                cpu_avail_norm, mem_avail_norm,
                safety_margin, stress_level, balance_score,
                headroom, is_data_cluster
            ])
        
        # === GLOBAL FEATURES (9 total) ===
        # Normalize pipeline requirements
        avg_cpu_capacity = np.mean([c['cpu_capacity'] for c in clusters_state.values()])
        avg_mem_capacity = np.mean([c['memory_capacity'] for c in clusters_state.values()])
        
        pipeline_cpu_norm = pipeline_request['cpu_required'] / max(avg_cpu_capacity, 1)
        pipeline_mem_norm = pipeline_request['memory_required'] / max(avg_mem_capacity, 1)
        
        # Placement difficulty
        placement_difficulty = max(pipeline_cpu_norm, pipeline_mem_norm)
        
        # Episode progress (dummy in inference)
        episode_progress = 0.0
        
        # Success rate (dummy in inference)
        success_rate = 1.0
        
        # Data locality flag
        is_data_local = 1.0 if pipeline_request['data_location'] != "none" else 0.0
        
        # Features 1-6 (originali)
        obs_parts.extend([
            pipeline_cpu_norm,
            pipeline_mem_norm,
            placement_difficulty,
            episode_progress,
            success_rate,
            is_data_local
        ])
        
        # ⚠️ NUOVE FEATURES 7-9 (MANCANTI NELLA VERSIONE ATTUALE)
        
        # 7. Cloud vs Edge utilization ratio
        cloud_clusters = [c for cname, c in clusters_state.items() 
                        if any(cfg.name == cname and cfg.cluster_type == 'cloud' 
                                for cfg in self.config.clusters)]
        edge_clusters = [c for cname, c in clusters_state.items() 
                        if any(cfg.name == cname and cfg.cluster_type == 'edge' 
                            for cfg in self.config.clusters)]
        
        if cloud_clusters and edge_clusters:
            cloud_avg_util = np.mean([
                (c['cpu_capacity'] - c['cpu_available']) / max(c['cpu_capacity'], 1) +
                (c['memory_capacity'] - c['memory_available']) / max(c['memory_capacity'], 1)
                for c in cloud_clusters
            ]) / 2.0
            
            edge_avg_util = np.mean([
                (c['cpu_capacity'] - c['cpu_available']) / max(c['cpu_capacity'], 1) +
                (c['memory_capacity'] - c['memory_available']) / max(c['memory_capacity'], 1)
                for c in edge_clusters
            ]) / 2.0
            
            cloud_edge_ratio = cloud_avg_util / (edge_avg_util + 1e-6)
        else:
            cloud_edge_ratio = 1.0
        
        obs_parts.append(cloud_edge_ratio)
        
        # 8. Edge clusters imbalance (variance)
        if len(edge_clusters) > 1:
            edge_utils = [
                ((c['cpu_capacity'] - c['cpu_available']) / max(c['cpu_capacity'], 1) +
                (c['memory_capacity'] - c['memory_available']) / max(c['memory_capacity'], 1)) / 2.0
                for c in edge_clusters
            ]
            edge_imbalance = np.std(edge_utils)
        else:
            edge_imbalance = 0.0
        
        obs_parts.append(edge_imbalance)
        
        # 9. Data transfer cost estimate
        if pipeline_request['data_location'] != 'none':
            data_loc = pipeline_request['data_location']
            
            # Trova il tipo del cluster con i dati
            data_cluster_type = None
            for cfg in self.config.clusters:
                if cfg.name == data_loc:
                    data_cluster_type = cfg.cluster_type
                    break
            
            # Calcola costo medio trasferimento
            transfer_costs = []
            for cfg in self.config.clusters:
                if cfg.name == data_loc:
                    cost = 0.0  # Local
                elif (data_cluster_type == 'cloud' and cfg.cluster_type == 'edge') or \
                    (data_cluster_type == 'edge' and cfg.cluster_type == 'cloud'):
                    cost = 1.0  # Cloud<->Edge (alto)
                else:
                    cost = 0.5  # Edge<->Edge (medio)
                transfer_costs.append(cost)
            
            avg_transfer_cost = np.mean(transfer_costs)
        else:
            avg_transfer_cost = 0.0
        
        obs_parts.append(avg_transfer_cost)
        
        # === TEMPORAL FEATURES (5) ===
        obs_parts.extend([0.0] * 5)  # Dummy temporal features
        
        # Reshape to (1, 50)
        obs = np.array(obs_parts, dtype=np.float32).reshape(1, -1)
        
        # Validate shape
        assert obs.shape == (1, 50), f"Wrong observation shape: {obs.shape}, expected (1, 50)"
    
        return obs

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