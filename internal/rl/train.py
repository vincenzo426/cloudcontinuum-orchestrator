#!/usr/bin/env python3
# internal/rl/train.py
"""
Training script con MaskablePPO, TensorBoard e RESUME Capability
VERSIONE 2.8 - RESUME TRAINING

CHANGELOG v2.8:
- Aggiunto supporto --resume-path per caricare modelli esistenti
- Aggiunto supporto --start-stage per saltare fasi del curriculum
- Fix Logger & Schedule (v2.7)
"""

import os
import argparse
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime

import torch.nn as nn

# SB3 & CONTRIB
from sb3_contrib import MaskablePPO
from sb3_contrib.common.wrappers import ActionMasker
from sb3_contrib.common.maskable.callbacks import MaskableEvalCallback
from sb3_contrib.common.maskable.utils import get_action_masks
from sb3_contrib.common.maskable.buffers import MaskableRolloutBuffer

from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize
from stable_baselines3.common.callbacks import CheckpointCallback, BaseCallback
from stable_baselines3.common.monitor import Monitor
from stable_baselines3.common.logger import configure

from .environment import CloudContinuumEnv
from .config import EnvironmentConfig, get_config_for_difficulty


# =============================================================================
# CALLBACKS & UTILS
# =============================================================================
class TensorboardCallback(BaseCallback):
    def __init__(self, verbose=0):
        super().__init__(verbose)

    def _on_step(self) -> bool:
        if self.logger is None:
            return True
        dones = self.locals.get("dones", [])
        infos = self.locals.get("infos", [])
        for idx, done in enumerate(dones):
            if done:
                info = infos[idx]
                if 'success_rate' in info:
                    self.logger.record('custom/success_rate', info['success_rate'])
                if 'placements_failed' in info:
                    self.logger.record('custom/placements_failed', info['placements_failed'])
                if 'avg_exec_time' in info:
                    self.logger.record('custom/avg_exec_time', info['avg_exec_time'])
        return True

def mask_fn(env: CloudContinuumEnv) -> np.ndarray:
    return env.get_action_mask()

# ========== TRAINING CONFIG ==========
TRAINING_CONFIG = {
    "stage1_easy": {
        "timesteps": 500_000,
        "learning_rate": 3e-4,
        "n_steps": 2048,
        "batch_size": 64,
        "n_epochs": 10,
        "gamma": 0.99,
        "gae_lambda": 0.95,
        "clip_range": 0.2,
        "ent_coef": 0.0,
        "vf_coef": 0.5,
        "max_grad_norm": 0.5,
        "target_kl": 0.01,
    },
    "stage2_medium": {
        "timesteps": 1_000_000,
        "learning_rate": 1e-4,
        "n_steps": 4096,
        "batch_size": 128,
        "n_epochs": 10,
        "gamma": 0.99,
        "gae_lambda": 0.95,
        "clip_range": 0.1,
        "ent_coef": 0.001,
        "vf_coef": 0.5,
        "max_grad_norm": 0.5,
        "target_kl": 0.01,
    },
    "stage3_hard": {
        "timesteps": 2_000_000, # Puoi aumentare questo valore se vuoi allenarlo di più
        "learning_rate": 5e-5,
        "n_steps": 4096,
        "batch_size": 256,
        "n_epochs": 20,
        "gamma": 0.995,
        "gae_lambda": 0.98,
        "clip_range": 0.1,
        "ent_coef": 0.005,
        "vf_coef": 0.5,
        "max_grad_norm": 0.5,
        "target_kl": 0.01,
    }
}

POLICY_KWARGS = {
    "net_arch": [256, 256, 128],
    "activation_fn": nn.Tanh,
}

def create_training_env(difficulty: str, seed: int) -> DummyVecEnv:
    config = get_config_for_difficulty(difficulty)
    config.master_seed = seed
    def make_env():
        env = CloudContinuumEnv(config=config, seed=seed)
        env = Monitor(env)
        env = ActionMasker(env, mask_fn)
        return env
    return DummyVecEnv([make_env])

def train_stage(
    stage_name: str,
    difficulty: str,
    config: Dict,
    model: MaskablePPO = None,
    save_dir: str = "./models"
) -> MaskablePPO:
    print("\n" + "=" * 70)
    print(f"TRAINING STAGE: {stage_name.upper()}")
    print("=" * 70)
    
    env = create_training_env(difficulty=difficulty, seed=42)
    env = VecNormalize(env, norm_obs=True, norm_reward=True, clip_obs=10.0)
    
    log_path = f"./tensorboard/{stage_name}"
    
    if model is None:
        print(f"Initializing NEW MaskablePPO (Logs: {log_path})...")
        model = MaskablePPO(
            "MlpPolicy",
            env,
            verbose=1,
            tensorboard_log=log_path,
            learning_rate=config['learning_rate'],
            n_steps=config['n_steps'],
            batch_size=config['batch_size'],
            n_epochs=config['n_epochs'],
            gamma=config['gamma'],
            gae_lambda=config['gae_lambda'],
            clip_range=config['clip_range'],
            ent_coef=config['ent_coef'],
            vf_coef=config['vf_coef'],
            max_grad_norm=config['max_grad_norm'],
            policy_kwargs=POLICY_KWARGS,
            target_kl=config.get('target_kl', None),
        )
    else:
        print(f"Continuing with LOADED model (Logs: {log_path})...")
        model.set_env(env)
        
        # Configure Logger manually
        new_logger = configure(log_path, ["stdout", "tensorboard"])
        model.set_logger(new_logger)
        
        # Update Hyperparameters
        model.lr_schedule = lambda _: config['learning_rate']
        model.clip_range = lambda _: config['clip_range']
        model.batch_size = config['batch_size']
        model.n_epochs = config['n_epochs']
        model.ent_coef = config['ent_coef']
        
        # Buffer Resize if needed
        if model.n_steps != config['n_steps']:
            print(f"⚠️ Resizing rollout buffer: {model.n_steps} -> {config['n_steps']}")
            model.n_steps = config['n_steps']
            model.rollout_buffer = MaskableRolloutBuffer(
                buffer_size=model.n_steps,
                observation_space=env.observation_space,
                action_space=env.action_space,
                device=model.device,
                gamma=model.gamma,
                gae_lambda=model.gae_lambda,
                n_envs=env.num_envs,
            )
    
    stage_dir = f"{save_dir}/{stage_name}"
    os.makedirs(stage_dir, exist_ok=True)
    
    # Callbacks (Eval + Checkpoint + Tensorboard)
    eval_env = create_training_env(difficulty=difficulty, seed=9999)
    eval_env = VecNormalize(eval_env, norm_obs=True, norm_reward=False, training=False, clip_obs=10.0)
    eval_env.obs_rms = env.obs_rms # Sync stats
    
    eval_callback = MaskableEvalCallback(
        eval_env,
        best_model_save_path=f"{stage_dir}/best_model",
        log_path=f"{stage_dir}/eval_logs",
        eval_freq=50_000,
        n_eval_episodes=20,
        deterministic=True
    )
    checkpoint_callback = CheckpointCallback(save_freq=100_000, save_path=f"{stage_dir}/checkpoints", name_prefix="ppo_checkpoint")
    tb_callback = TensorboardCallback()
    
    print(f"\n🚀 Starting training for {config['timesteps']:,} timesteps...")
    model.learn(
        total_timesteps=config['timesteps'],
        callback=[eval_callback, checkpoint_callback, tb_callback],
        progress_bar=True,
        reset_num_timesteps=False # Importante per resume
    )
    
    final_path = f"{stage_dir}/final_model.zip"
    model.save(final_path)
    env.save(f"{stage_dir}/vec_normalize.pkl")
    
    return model

def train_curriculum(
    save_dir: str = "./models",
    resume_path: Optional[str] = None,
    start_stage: str = "stage1_easy"
) -> MaskablePPO:
    print("\n" + "=" * 70)
    print(" " * 20 + "CURRICULUM LEARNING (MASKABLE + RESUME)")
    print("=" * 70)
    
    model = None
    
    # 1. Caricamento Modello Esistente (se richiesto)
    if resume_path:
        print(f"\n🔄 Resuming from model: {resume_path}")
        if not os.path.exists(resume_path):
            raise FileNotFoundError(f"Model file not found: {resume_path}")
        
        # Carica il modello usando un ambiente dummy temporaneo per l'inizializzazione
        dummy_env = create_training_env("easy", 42) # Difficulty doesn't matter for loading
        model = MaskablePPO.load(resume_path, env=dummy_env)
        print("✅ Model loaded successfully!")

    # 2. Definizione Ordine Stage
    stages = [
        ("stage1_easy", "easy", "stage1_easy"),
        ("stage2_medium", "medium", "stage2_medium"),
        ("stage3_hard", "hard", "stage3_hard")
    ]
    
    # 3. Filtra gli stage in base a start_stage
    start_index = 0
    for i, (name, _, _) in enumerate(stages):
        if name == start_stage:
            start_index = i
            break
    
    stages_to_run = stages[start_index:]
    print(f"📅 Scheduled stages: {[s[0] for s in stages_to_run]}")

    # 4. Esecuzione Curriculum
    for stage_name, difficulty, config_key in stages_to_run:
        model = train_stage(
            stage_name=stage_name,
            difficulty=difficulty,
            config=TRAINING_CONFIG[config_key],
            model=model,
            save_dir=save_dir
        )
    
    return model

def evaluate_final_model(model_path: str, difficulty: str = "hard") -> Dict:
    print("\n" + "=" * 70)
    print(" " * 20 + "FINAL MODEL EVALUATION")
    print("=" * 70)
    env = create_training_env(difficulty=difficulty, seed=7777)
    env = VecNormalize(env, norm_obs=True, norm_reward=False, training=False)
    print(f"Loading model: {model_path}")
    model = MaskablePPO.load(model_path, env=env)
    
    results = {'success_rates': [], 'episode_rewards': []}
    print("Running evaluation (100 episodes)...")
    for ep in range(100):
        obs = env.reset()
        done = False
        ep_rew = 0
        while not done:
            action, _ = model.predict(obs, deterministic=True)
            obs, reward, done, info = env.step(action)
            ep_rew += reward[0]
        results['success_rates'].append(info[0]['success_rate'])
        results['episode_rewards'].append(ep_rew)
        if (ep+1)%20==0: print(f"  Progress: {ep+1}/100")
        
    print(f"\nSuccess Rate: {np.mean(results['success_rates'])*100:.2f}%")
    print(f"Mean Reward: {np.mean(results['episode_rewards']):.2f}")
    return {}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--save-dir", type=str, default="./models_masked")
    parser.add_argument("--resume-path", type=str, default=None, help="Path to .zip model to resume from")
    parser.add_argument("--start-stage", type=str, default="stage1_easy", choices=["stage1_easy", "stage2_medium", "stage3_hard"])
    parser.add_argument("--eval-only", action="store_true")
    parser.add_argument("--model-path", type=str, default=None)
    args = parser.parse_args()
    
    if args.eval_only:
        evaluate_final_model(model_path=args.model_path)
    else:
        train_curriculum(
            save_dir=args.save_dir,
            resume_path=args.resume_path,
            start_stage=args.start_stage
        )
        # Valuta il modello finale dello stage hard
        evaluate_final_model(f"{args.save_dir}/stage3_hard/final_model.zip")

if __name__ == "__main__":
    main()