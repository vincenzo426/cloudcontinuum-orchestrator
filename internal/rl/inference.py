#!/usr/bin/env python3
# internal/rl/inference.py
"""
Production inference module per CloudContinuum RL Agent

Fornisce API Flask per integrazione con Go controller:
- POST /predict: Riceve stato cluster e pipeline, ritorna cluster target
- GET /health: Health check endpoint

NOTA: In produzione, il controller Go chiamerà questo servizio via HTTP
"""

import os
import argparse
import numpy as np
from typing import Dict, List, Optional
from flask import Flask, request, jsonify
import logging

from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import DummyVecEnv

from .environment import CloudContinuumEnv
from .config import EnvironmentConfig, ClusterConfig, DEFAULT_CLUSTERS


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RLPlacementAgent:
    """
    RL Agent per placement inference in produzione.
    
    Carica modello addestrato e fornisce predizioni.
    """
    
    def __init__(self, model_path: str, config: Optional[EnvironmentConfig] = None):
        """
        Inizializza agent.
        
        Args:
            model_path: Path al modello .zip addestrato
            config: Environment config (se None, usa default)
        """
        logger.info(f"Loading RL model from: {model_path}")
        
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model not found: {model_path}")
        
        # Config
        if config is None:
            config = EnvironmentConfig(
                clusters=DEFAULT_CLUSTERS,
                pipelines_per_episode=1,  # Inference: 1 pipeline alla volta
                max_steps_per_episode=1,
                master_seed=0
            )
        self.config = config
        
        # Load model
        self.model = PPO.load(model_path)
        logger.info("✅ Model loaded successfully")
        
        # Create dummy environment per normalizzazione
        def make_env():
            return CloudContinuumEnv(config=self.config, seed=0)
        
        self.env = DummyVecEnv([make_env])
        
        logger.info(f"Agent initialized with {len(config.clusters)} clusters")
    
    def predict_placement(
        self,
        pipeline_request: Dict,
        clusters_state: Dict[str, Dict],
        deterministic: bool = True
    ) -> Dict:
        """
        Predice cluster target per pipeline.
        
        Args:
            pipeline_request: Dict con:
                - cpu_required: int (millicores)
                - memory_required: int (bytes)
                - data_location: str (cluster name o "none")
                - pipeline_name: str
            
            clusters_state: Dict[cluster_name, state] con:
                - cpu_capacity: int
                - cpu_available: int
                - memory_capacity: int
                - memory_available: int
                - baseline_cpu: int
                - baseline_memory: int
        
        Returns:
            Dict con:
                - target_cluster: str
                - confidence: float (0-1)
                - reason: str
                - action_probabilities: Dict[cluster_name, float]
        """
        try:
            # Costruisci observation
            obs = self._build_observation(pipeline_request, clusters_state)
            
            # Predict con modello RL
            action, _states = self.model.predict(obs, deterministic=deterministic)
            action = int(action[0])
            
            # Get action probabilities per confidence
            obs_tensor = self.model.policy.obs_to_tensor(obs)[0]
            with np.errstate(all='ignore'):  # Ignora warning numpy
                distribution = self.model.policy.get_distribution(obs_tensor)
                probs = distribution.distribution.probs.detach().cpu().numpy()[0]
            
            # Map action index to cluster name
            target_cluster = self.config.clusters[action].name
            confidence = float(probs[action])
            
            # Build probabilities dict
            action_probs = {
                cluster.name: float(probs[idx])
                for idx, cluster in enumerate(self.config.clusters)
            }
            
            # Generate reason
            reason = self._generate_reason(
                pipeline_request,
                clusters_state,
                target_cluster,
                confidence
            )
            
            logger.info(
                f"Prediction: {target_cluster} "
                f"(confidence: {confidence:.2%}) "
                f"for pipeline {pipeline_request.get('pipeline_name', 'unknown')}"
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
    
    def _build_observation(
        self,
        pipeline_request: Dict,
        clusters_state: Dict[str, Dict]
    ) -> np.ndarray:
        """
        Costruisce observation vector per RL model.
        
        Deve matchare esattamente il formato di environment.py
        """
        obs_parts = []
        
        # Per ogni cluster: 9 features
        for cluster_cfg in self.config.clusters:
            cluster = clusters_state[cluster_cfg.name]
            
            # CPU & Memory utilization
            cpu_util = 1.0 - (cluster['cpu_available'] / cluster['cpu_capacity'])
            mem_util = 1.0 - (cluster['memory_available'] / cluster['memory_capacity'])
            
            # Normalized availability (0-1)
            cpu_avail_norm = cluster['cpu_available'] / cluster['cpu_capacity']
            mem_avail_norm = cluster['memory_available'] / cluster['memory_capacity']
            
            # Safety margin (quanto sopra baseline)
            cpu_above_baseline = max(0, cluster['cpu_available'] - cluster['baseline_cpu'])
            mem_above_baseline = max(0, cluster['memory_available'] - cluster['baseline_memory'])
            
            cpu_margin = cpu_above_baseline / cluster['cpu_capacity']
            mem_margin = mem_above_baseline / cluster['memory_capacity']
            safety_margin = (cpu_margin + mem_margin) / 2.0
            
            # Stress level (quanto vicino a saturazione)
            stress_level = max(cpu_util, mem_util)
            
            # Balance score (quanto bilanciato CPU vs Memory)
            balance_score = 1.0 - abs(cpu_util - mem_util)
            
            # Headroom (capacità futura)
            headroom = min(cpu_avail_norm, mem_avail_norm)
            
            # Is data cluster flag
            is_data_cluster = 1.0 if cluster_cfg.name == pipeline_request['data_location'] else 0.0
            
            obs_parts.extend([
                cpu_util,
                mem_util,
                cpu_avail_norm,
                mem_avail_norm,
                safety_margin,
                stress_level,
                balance_score,
                headroom,
                is_data_cluster
            ])
        
        # Global features: 6 features
        # Pipeline resource requirements (normalized)
        max_cpu = max(c.cpu_capacity for c in self.config.clusters)
        max_mem = max(c.memory_capacity for c in self.config.clusters)
        
        pipeline_cpu_norm = pipeline_request['cpu_required'] / max_cpu
        pipeline_mem_norm = pipeline_request['memory_required'] / max_mem
        
        # Placement difficulty (pesantezza relativa pipeline)
        placement_difficulty = (pipeline_cpu_norm + pipeline_mem_norm) / 2.0
        
        # Episode progress (in inference = 0, siamo sempre all'inizio)
        episode_progress = 0.0
        
        # Success rate so far (in inference = 1.0, assumiamo tutto ok finora)
        success_rate_so_far = 1.0
        
        # Is data local flag (overall)
        is_data_local = 1.0 if pipeline_request['data_location'] != "none" else 0.0
        
        obs_parts.extend([
            pipeline_cpu_norm,
            pipeline_mem_norm,
            placement_difficulty,
            episode_progress,
            success_rate_so_far,
            is_data_local
        ])
        
        # Temporal features: 5 features (in inference, tutti a 0)
        obs_parts.extend([0.0, 0.0, 0.0, 0.0, 0.0])
        
        return np.array(obs_parts, dtype=np.float32).reshape(1, -1)
    
    def _generate_reason(
        self,
        pipeline_request: Dict,
        clusters_state: Dict[str, Dict],
        target_cluster: str,
        confidence: float
    ) -> str:
        """Genera spiegazione human-readable della decisione"""
        reasons = []
        
        # Check data locality
        if target_cluster == pipeline_request['data_location']:
            reasons.append("data locality satisfied")
        
        # Check resource availability
        target_state = clusters_state[target_cluster]
        cpu_util = 1.0 - (target_state['cpu_available'] / target_state['cpu_capacity'])
        
        if cpu_util < 0.5:
            reasons.append("low utilization")
        elif cpu_util < 0.75:
            reasons.append("optimal utilization")
        else:
            reasons.append("high utilization")
        
        # Check pipeline size
        pipeline_cpu = pipeline_request['cpu_required']
        if pipeline_cpu >= 3000:
            reasons.append("heavy pipeline")
        elif pipeline_cpu <= 1500:
            reasons.append("light pipeline")
        
        # Confidence level
        if confidence > 0.8:
            reasons.append(f"high confidence ({confidence:.1%})")
        elif confidence > 0.5:
            reasons.append(f"medium confidence ({confidence:.1%})")
        else:
            reasons.append(f"low confidence ({confidence:.1%})")
        
        return f"RL-based: {', '.join(reasons)}"


# Flask API
app = Flask(__name__)
agent: Optional[RLPlacementAgent] = None


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'model_loaded': agent is not None
    })


@app.route('/predict', methods=['POST'])
def predict():
    """
    Predict cluster placement.
    
    Request JSON:
    {
        "pipeline": {
            "cpu_required": 2000,
            "memory_required": 4294967296,
            "data_location": "edge_cluster_1",
            "pipeline_name": "my-pipeline"
        },
        "clusters": {
            "cloud_cluster": {
                "cpu_capacity": 16000,
                "cpu_available": 12000,
                "memory_capacity": 34359738368,
                "memory_available": 25769803776,
                "baseline_cpu": 4000,
                "baseline_memory": 8589934592
            },
            "edge_cluster_1": { ... },
            ...
        }
    }
    
    Response JSON:
    {
        "target_cluster": "edge_cluster_1",
        "confidence": 0.87,
        "reason": "RL-based: data locality satisfied, optimal utilization, high confidence (87.3%)",
        "action_probabilities": {
            "cloud_cluster": 0.05,
            "edge_cluster_1": 0.87,
            "edge_cluster_2": 0.06,
            "edge_cluster_3": 0.02
        }
    }
    """
    if agent is None:
        return jsonify({'error': 'Model not loaded'}), 500
    
    try:
        data = request.get_json()
        
        if not data or 'pipeline' not in data or 'clusters' not in data:
            return jsonify({
                'error': 'Invalid request format. Required: pipeline, clusters'
            }), 400
        
        result = agent.predict_placement(
            pipeline_request=data['pipeline'],
            clusters_state=data['clusters'],
            deterministic=True
        )
        
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Prediction failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


def main():
    """Main entry point per servizio inference"""
    parser = argparse.ArgumentParser(description="CloudContinuum RL Inference Service")
    parser.add_argument("--model-path", type=str, required=True,
                        help="Path to trained model .zip")
    parser.add_argument("--host", type=str, default="0.0.0.0",
                        help="Host to bind to")
    parser.add_argument("--port", type=int, default=5000,
                        help="Port to bind to")
    
    args = parser.parse_args()
    
    # Initialize agent
    global agent
    logger.info("="*60)
    logger.info("CloudContinuum RL Inference Service")
    logger.info("="*60)
    
    agent = RLPlacementAgent(model_path=args.model_path)
    
    logger.info(f"Starting Flask server on {args.host}:{args.port}")
    logger.info("="*60)
    
    # Run Flask
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()