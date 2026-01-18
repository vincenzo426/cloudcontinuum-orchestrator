#!/usr/bin/env python3
# internal/rl/train.py
"""
Training script con Curriculum Learning per CloudContinuum RL Agent
VERSIONE 2.2 - FIX SCHEDULES

CHANGELOG v2.2:
- Fix TypeError: clip_range must be callable (wrapped in get_schedule_fn)
- Fix learning_rate schedule update
- Includes v2.1 fixes (Buffer Resize, Eval Env Normalization)
"""

import os
import argparse
import numpy as np
from typing import Dict, List
from datetime import datetime

from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize
from stable_baselines3.common.callbacks import EvalCallback, CheckpointCallback
from stable_baselines3.common.monitor import Monitor
from stable_baselines3.common.buffers import RolloutBuffer
# FIX: Import necessario per convertire float in schedule function
from stable_baselines3.common.utils import get_schedule_fn

from .environment import CloudContinuumEnv
from .config import EnvironmentConfig, get_config_for_difficulty


# ========== TRAINING HYPERPARAMETERS V2.0 ==========
TRAINING_CONFIG = {
    "stage1_easy": {
        "timesteps": 1_000_000,
        "learning_rate": 1e-4,
        "n_steps": 4096,
        "batch_size": 128,
        "n_epochs": 20,
        "gamma": 0.99,
        "gae_lambda": 0.98,
        "clip_range": 0.15,
        "clip_range_vf": 0.15,
        "ent_coef": 0.005,
        "vf_coef": 0.5,
        "max_grad_norm": 0.5,
        "normalize_advantage": True,
        "target_kl": 0.01,
    },
    "stage2_medium": {
        "timesteps": 3_000_000,
        "learning_rate": 5e-5,
        "n_steps": 4096,
        "batch_size": 128,
        "n_epochs": 20,
        "gamma": 0.99,
        "gae_lambda": 0.98,
        "clip_range": 0.1,
        "clip_range_vf": 0.1,
        "ent_coef": 0.003,
        "vf_coef": 0.5,
        "max_grad_norm": 0.5,
        "normalize_advantage": True,
        "target_kl": 0.01,
    },
    "stage3_hard": {
        "timesteps": 5_000_000,
        "learning_rate": 3e-5,
        "n_steps": 4096,
        "batch_size": 256,
        "n_epochs": 25,
        "gamma": 0.995,
        "gae_lambda": 0.99,
        "clip_range": 0.1,
        "clip_range_vf": 0.1,
        "ent_coef": 0.001,
        "vf_coef": 0.5,
        "max_grad_norm": 0.5,
        "normalize_advantage": True,
        "target_kl": 0.008,
    }
}

POLICY_KWARGS = {
    "net_arch": [256, 256, 128],
    "activation_fn": "tanh",
}


def create_training_env(difficulty: str, seed: int) -> DummyVecEnv:
    """Crea environment vettorizzato per training"""
    config = get_config_for_difficulty(difficulty)
    config.master_seed = seed
    
    def make_env():
        env = CloudContinuumEnv(config=config, seed=seed)
        env = Monitor(env)
        return env
    
    return DummyVecEnv([make_env])


def train_stage(
    stage_name: str,
    difficulty: str,
    config: Dict,
    model: PPO = None,
    save_dir: str = "./models"
) -> PPO:
    """Addestra singolo stage del curriculum."""
    print("\n" + "=" * 70)
    print(f"TRAINING STAGE: {stage_name.upper()}")
    print(f"Difficulty: {difficulty}")
    print(f"Timesteps: {config['timesteps']:,}")
    print("=" * 70)
    
    # Create environment
    env = create_training_env(difficulty=difficulty, seed=42)
    
    # Normalize observations & rewards
    env = VecNormalize(
        env,
        norm_obs=True,
        norm_reward=True,
        clip_obs=10.0,
        clip_reward=10.0
    )
    
    # Initialize or update model
    if model is None:
        print("Initializing NEW PPO model...")
        model = PPO(
            "MlpPolicy",
            env,
            verbose=1,
            tensorboard_log=f"./tensorboard/{stage_name}",
            learning_rate=config['learning_rate'],
            n_steps=config['n_steps'],
            batch_size=config['batch_size'],
            n_epochs=config['n_epochs'],
            gamma=config['gamma'],
            gae_lambda=config['gae_lambda'],
            clip_range=config['clip_range'],
            clip_range_vf=config.get('clip_range_vf', None),
            ent_coef=config['ent_coef'],
            vf_coef=config['vf_coef'],
            max_grad_norm=config['max_grad_norm'],
            policy_kwargs=POLICY_KWARGS,
            target_kl=config.get('target_kl', None),
        )
    else:
        print(f"Continuing training from previous stage...")
        model.set_env(env)
        
        # =============================================================================
        # FIX CRITICO: Usa get_schedule_fn per convertire float in callable
        # =============================================================================
        model.lr_schedule = get_schedule_fn(config['learning_rate'])
        model.clip_range = get_schedule_fn(config['clip_range'])
        
        if config.get('clip_range_vf'):
            model.clip_range_vf = get_schedule_fn(config['clip_range_vf'])
            
        # Update altri parametri
        model.batch_size = config['batch_size']
        model.n_epochs = config['n_epochs']
        model.ent_coef = config['ent_coef']
        
        # FIX BUFFER RESIZE: Aggiorna n_steps e RIDIMENSIONA IL BUFFER
        if model.n_steps != config['n_steps']:
            print(f"⚠️ Resizing rollout buffer: {model.n_steps} -> {config['n_steps']}")
            model.n_steps = config['n_steps']
            
            # Re-inizializza il buffer con la nuova dimensione
            model.rollout_buffer = RolloutBuffer(
                buffer_size=model.n_steps,
                observation_space=env.observation_space,
                action_space=env.action_space,
                device=model.device,
                gamma=model.gamma,
                gae_lambda=model.gae_lambda,
                n_envs=env.num_envs,
            )
    
    # Setup callbacks
    stage_dir = f"{save_dir}/{stage_name}"
    os.makedirs(stage_dir, exist_ok=True)
    
    # FIX: Eval env deve essere normalizzato come il training env
    eval_env = create_training_env(difficulty=difficulty, seed=9999)
    eval_env = VecNormalize(
        eval_env,
        norm_obs=True, 
        norm_reward=False, # Non normalizzare reward per metriche reali
        training=False,    # Non aggiornare statistiche durante eval
        clip_obs=10.0
    )
    
    # Sincronizza statistiche iniziali
    eval_env.obs_rms = env.obs_rms
    
    eval_callback = EvalCallback(
        eval_env,
        best_model_save_path=f"{stage_dir}/best_model",
        log_path=f"{stage_dir}/eval_logs",
        eval_freq=50_000,
        n_eval_episodes=20,
        deterministic=True,
        render=False
    )
    
    # Checkpoint callback
    checkpoint_callback = CheckpointCallback(
        save_freq=100_000,
        save_path=f"{stage_dir}/checkpoints",
        name_prefix="ppo_checkpoint"
    )
    
    # Train
    print(f"\n🚀 Starting training for {config['timesteps']:,} timesteps...")
    start_time = datetime.now()
    
    model.learn(
        total_timesteps=config['timesteps'],
        callback=[eval_callback, checkpoint_callback],
        progress_bar=True
    )
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds() / 3600
    
    print(f"\n✅ {stage_name} completed in {duration:.2f} hours")
    
    # Save final model
    final_path = f"{stage_dir}/final_model.zip"
    model.save(final_path)
    print(f"📦 Model saved to: {final_path}")
    
    # Save VecNormalize stats
    env.save(f"{stage_dir}/vec_normalize.pkl")
    
    return model


def train_curriculum(
    pretrained_model_path: str = None,
    save_dir: str = "./models"
) -> PPO:
    """Training completo con Curriculum Learning"""
    print("\n" + "=" * 70)
    print(" " * 20 + "CURRICULUM LEARNING PIPELINE")
    print("=" * 70)
    print("Total training: 9,000,000 timesteps")
    
    # Load pretrained model
    model = None
    if pretrained_model_path and os.path.exists(pretrained_model_path):
        print(f"\n🔄 Loading pre-trained model from: {pretrained_model_path}")
        # Usa dummy env temporaneo per loading
        temp_env = create_training_env(difficulty="easy", seed=42)
        model = PPO.load(pretrained_model_path, env=temp_env)
        print("✅ Pre-trained model loaded successfully")
    
    # Stages
    for stage_name, difficulty, config_key in [
        ("stage1_easy", "easy", "stage1_easy"),
        ("stage2_medium", "medium", "stage2_medium"),
        ("stage3_hard", "hard", "stage3_hard")
    ]:
        model = train_stage(
            stage_name=stage_name,
            difficulty=difficulty,
            config=TRAINING_CONFIG[config_key],
            model=model,
            save_dir=save_dir
        )
    
    print("\n" + "=" * 70)
    print(" " * 15 + "CURRICULUM TRAINING COMPLETED ✅")
    print("=" * 70)
    
    return model


def evaluate_final_model(
    model_path: str,
    num_episodes: int = 100,
    difficulty: str = "hard"
) -> Dict:
    """Valutazione rigorosa del modello finale"""
    print("\n" + "=" * 70)
    print(" " * 20 + "FINAL MODEL EVALUATION")
    print("=" * 70)
    
    # Load model & Env
    env = create_training_env(difficulty=difficulty, seed=7777)
    # Importante: Normalizzare obs anche in test
    env = VecNormalize(env, norm_obs=True, norm_reward=False, training=False)
    
    model = PPO.load(model_path, env=env)
    
    # Run evaluation
    results = {
        'success_rates': [],
        'episode_rewards': [],
        'avg_exec_times': [],
        'failure_reasons': []
    }
    
    print("\nRunning evaluation...")
    for ep in range(num_episodes):
        obs = env.reset()
        done = False
        ep_reward = 0
        
        while not done:
            action, _states = model.predict(obs, deterministic=True)
            obs, reward, done, info = env.step(action)
            ep_reward += reward[0]
        
        ep_info = info[0]
        results['success_rates'].append(ep_info['success_rate'])
        results['episode_rewards'].append(ep_reward)
        results['avg_exec_times'].append(ep_info.get('avg_exec_time', 0))
        
        if ep_info.get('failure_reason'):
            results['failure_reasons'].append(ep_info['failure_reason'])
        
        if (ep + 1) % 20 == 0:
            print(f"  Progress: {ep+1}/{num_episodes} episodes completed")
    
    # Stats
    mean_success = np.mean(results['success_rates'])
    mean_reward = np.mean(results['episode_rewards'])
    
    print("\n" + "=" * 70)
    print("EVALUATION RESULTS")
    print("=" * 70)
    print(f"Success Rate: {mean_success*100:.2f}%")
    print(f"Mean Reward: {mean_reward:.2f}")
    
    return results


def main():
    parser = argparse.ArgumentParser(description="Train CloudContinuum RL Agent")
    parser.add_argument("--pretrained-model", type=str, default=None)
    parser.add_argument("--save-dir", type=str, default="./models")
    parser.add_argument("--eval-only", action="store_true")
    parser.add_argument("--model-path", type=str, default=None)
    
    args = parser.parse_args()
    
    if args.eval_only:
        evaluate_final_model(model_path=args.model_path)
    else:
        train_curriculum(pretrained_model_path=args.pretrained_model, save_dir=args.save_dir)
        evaluate_final_model(
            model_path=f"{args.save_dir}/stage3_hard/best_model/best_model.zip"
        )

if __name__ == "__main__":
    main()