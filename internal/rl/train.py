# internal/rl/train.py
"""CloudContinuum RL - Training script with curriculum learning."""

import os
import argparse
import numpy as np
from typing import Dict, Optional, Any
from collections import defaultdict

import torch.nn as nn
from sb3_contrib import MaskablePPO
from sb3_contrib.common.wrappers import ActionMasker
from sb3_contrib.common.maskable.callbacks import MaskableEvalCallback
from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize
from stable_baselines3.common.callbacks import BaseCallback, CheckpointCallback, CallbackList
from stable_baselines3.common.monitor import Monitor
from stable_baselines3.common.logger import configure

from .environment import CloudContinuumEnv
from .config import get_config_for_difficulty
from .training_logger import TrainingDataLogger, EpisodeDataLogger


# === TRAINING CONFIG ===

TRAINING_CONFIG = {
    "stage1_easy": {
        "difficulty": "easy", "timesteps": 750_000,
        "learning_rate": 3e-4, "n_steps": 1024, "batch_size": 64,
        "n_epochs": 10, "gamma": 0.99, "gae_lambda": 0.95,
        "clip_range": 0.2, "ent_coef": 0.05, "vf_coef": 0.5, "max_grad_norm": 0.5,
    },
    "stage2_medium": {
        "difficulty": "medium", "timesteps": 1_500_000,
        "learning_rate": 1e-4, "n_steps": 2048, "batch_size": 128,
        "n_epochs": 10, "gamma": 0.99, "gae_lambda": 0.95,
        "clip_range": 0.15, "ent_coef": 0.03, "vf_coef": 0.5, "max_grad_norm": 0.5,
    },
    "stage3_hard": {
        "difficulty": "hard", "timesteps": 2_000_000,
        "learning_rate": 5e-5, "n_steps": 2048, "batch_size": 256,
        "n_epochs": 15, "gamma": 0.995, "gae_lambda": 0.98,
        "clip_range": 0.1, "ent_coef": 0.02, "vf_coef": 0.5, "max_grad_norm": 0.5,
    },
}

POLICY_KWARGS = {
    "net_arch": dict(pi=[128, 128], vf=[128, 128]),
    "activation_fn": nn.ReLU,
}


# === CALLBACKS ===

class MetricsCallback(BaseCallback):
    """Callback for logging anti-myopic metrics to TensorBoard."""
    
    def __init__(self, verbose: int = 0):
        super().__init__(verbose)
        self.metrics = defaultdict(list)
    
    def _on_step(self) -> bool:
        for idx, done in enumerate(self.locals.get("dones", [])):
            if done and idx < len(self.locals.get("infos", [])):
                info = self.locals["infos"][idx]
                for key in ['success_rate', 'avg_exec_time', 'locality_rate', 'avg_efficiency']:
                    if key in info:
                        self.metrics[key].append(info[key])
                
                successful = info.get('placements_successful', 0)
                if successful > 0:
                    self.metrics['small_on_cloud'].append(info.get('small_on_cloud', 0))
                    self.metrics['large_on_cloud'].append(info.get('large_on_cloud', 0))
        
        if len(self.metrics.get('success_rate', [])) >= 100:
            self._log_metrics()
        return True
    
    def _log_metrics(self):
        if self.logger is None:
            return
        for key, values in self.metrics.items():
            if values:
                self.logger.record(f"metrics/{key}", np.mean(values))
        
        small = sum(self.metrics.get('small_on_cloud', []))
        large = sum(self.metrics.get('large_on_cloud', []))
        if small + large > 0:
            self.logger.record("metrics/strategic_ratio", large / (small + large))
        self.metrics.clear()


class ProgressCallback(BaseCallback):
    """Callback for progress display."""
    
    def __init__(self, total_timesteps: int, stage_name: str, verbose: int = 1):
        super().__init__(verbose)
        self.total = total_timesteps
        self.stage = stage_name
        self.last_log = 0
    
    def _on_step(self) -> bool:
        if self.num_timesteps - self.last_log >= 10000:
            pct = (self.num_timesteps / self.total) * 100
            print(f"  [{self.stage}] {pct:.1f}% ({self.num_timesteps:,}/{self.total:,})")
            self.last_log = self.num_timesteps
        return True


# === ENVIRONMENT FACTORY ===

def mask_fn(env: CloudContinuumEnv) -> np.ndarray:
    return env.action_masks()


def make_env(difficulty: str, seed: int) -> CloudContinuumEnv:
    config = get_config_for_difficulty(difficulty)
    config.master_seed = seed
    return CloudContinuumEnv(config=config, seed=seed)


def create_training_env(difficulty: str, seed: int, log_dir: str = None) -> DummyVecEnv:
    """Create wrapped environment for training."""
    def _make():
        env = make_env(difficulty, seed)
        env = Monitor(env, log_dir) if log_dir else Monitor(env)
        return ActionMasker(env, mask_fn)
    return DummyVecEnv([_make])


# === TRAINING ===

def train_stage(stage_name: str, config: Dict, model: Optional[MaskablePPO] = None,
                save_dir: str = "./models", seed: int = 42,
                log_training_data: bool = False) -> MaskablePPO:
    """Train a single curriculum stage."""
    print(f"\n{'='*60}\n  STAGE: {stage_name.upper()}\n{'='*60}")
    print(f"  Difficulty: {config['difficulty']} | Timesteps: {config['timesteps']:,}")
    
    stage_dir = os.path.join(save_dir, stage_name)
    os.makedirs(stage_dir, exist_ok=True)
    log_dir = os.path.join(stage_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    # Create environments
    train_env = VecNormalize(
        create_training_env(config['difficulty'], seed, log_dir),
        norm_obs=True, norm_reward=True, clip_obs=10.0, clip_reward=10.0
    )
    eval_env = VecNormalize(
        create_training_env(config['difficulty'], seed + 1000),
        norm_obs=True, norm_reward=False, clip_obs=10.0, training=False
    )
    
    tb_log_dir = os.path.join(stage_dir, "tensorboard")
    
    # Create or update model
    if model is None:
        model = MaskablePPO(
            "MlpPolicy", train_env,
            learning_rate=config['learning_rate'], n_steps=config['n_steps'],
            batch_size=config['batch_size'], n_epochs=config['n_epochs'],
            gamma=config['gamma'], gae_lambda=config['gae_lambda'],
            clip_range=config['clip_range'], ent_coef=config['ent_coef'],
            vf_coef=config['vf_coef'], max_grad_norm=config['max_grad_norm'],
            policy_kwargs=POLICY_KWARGS, tensorboard_log=tb_log_dir,
            verbose=1, seed=seed,
        )
    else:
        model.set_env(train_env)
        model.learning_rate = config['learning_rate']
        model.ent_coef = config['ent_coef']
        model.clip_range = lambda _: config['clip_range']
        model.set_logger(configure(tb_log_dir, ["stdout", "tensorboard"]))
    
    eval_env.obs_rms = train_env.obs_rms
    
    # Callbacks
    callbacks = [
        MaskableEvalCallback(eval_env, best_model_save_path=os.path.join(stage_dir, "best_model"),
                             log_path=os.path.join(stage_dir, "eval_logs"),
                             eval_freq=10000, n_eval_episodes=20, deterministic=True, verbose=1),
        CheckpointCallback(save_freq=50000, save_path=os.path.join(stage_dir, "checkpoints"),
                          name_prefix=f"{stage_name}_ckpt", verbose=0),
        MetricsCallback(verbose=0),
        ProgressCallback(config['timesteps'], stage_name),
    ]
    
    # Add training data loggers if enabled
    if log_training_data:
        data_dir = os.path.join(stage_dir, "training_data")
        os.makedirs(data_dir, exist_ok=True)
        
        # Full transition logger (saves all observations, actions, rewards)
        training_logger = TrainingDataLogger(
            output_dir=data_dir,
            filename_prefix=f"transitions_{config['difficulty']}",
            save_frequency=10000,
            include_action_probs=True,
            verbose=1,
        )
        callbacks.append(training_logger)
        
        # Episode summary logger
        episode_logger = EpisodeDataLogger(
            output_dir=data_dir,
            filename_prefix=f"episodes_{config['difficulty']}",
            verbose=1,
        )
        callbacks.append(episode_logger)
        
        print(f"  [DataLogger] Saving training data to: {data_dir}")
    
    callback_list = CallbackList(callbacks)
    
    # Train
    model.learn(total_timesteps=config['timesteps'], callback=callback_list,
                progress_bar=True, reset_num_timesteps=(model is None))
    
    # Save
    model.save(os.path.join(stage_dir, "final_model.zip"))
    train_env.save(os.path.join(stage_dir, "vec_normalize.pkl"))
    
    print(f"  Stage {stage_name} completed!")
    return model


def train_curriculum(save_dir: str = "./models", start_stage: str = "stage1_easy",
                     resume_path: Optional[str] = None, seed: int = 42,
                     log_training_data: bool = False) -> MaskablePPO:
    """Run full curriculum training."""
    print(f"\n{'='*60}\n  CURRICULUM LEARNING\n{'='*60}")
    if log_training_data:
        print("  [DataLogger] Training data logging ENABLED")
    
    stages = [
        ("stage1_easy", TRAINING_CONFIG["stage1_easy"]),
        ("stage2_medium", TRAINING_CONFIG["stage2_medium"]),
        ("stage3_hard", TRAINING_CONFIG["stage3_hard"]),
    ]
    
    start_idx = next((i for i, (n, _) in enumerate(stages) if n == start_stage), 0)
    stages_to_run = stages[start_idx:]
    
    model = None
    if resume_path and os.path.exists(resume_path):
        model = MaskablePPO.load(resume_path, env=create_training_env("easy", seed))
    
    for stage_name, stage_config in stages_to_run:
        model = train_stage(stage_name, stage_config, model, save_dir, seed, log_training_data)
    
    print("\n  TRAINING COMPLETED!")
    return model


# === EVALUATION ===

def evaluate_model(model_path: str, difficulty: str = "hard",
                   num_episodes: int = 100, verbose: bool = True) -> Dict[str, Any]:
    """Evaluate trained model."""
    if verbose:
        print(f"\n{'='*60}\n  EVALUATION: {model_path}\n{'='*60}")
    
    model_dir = os.path.dirname(model_path)
    vec_norm_path = os.path.join(model_dir, "vec_normalize.pkl")
    
    env = create_training_env(difficulty, seed=9999)
    if os.path.exists(vec_norm_path):
        env = VecNormalize.load(vec_norm_path, env)
        env.training = False
        env.norm_reward = False
    
    model = MaskablePPO.load(model_path, env=env)
    
    # Run evaluation
    rewards, success_rates = [], []
    small_cloud, large_cloud = [], []
    cluster_dist = defaultdict(int)
    
    for _ in range(num_episodes):
        obs, done, ep_reward = env.reset(), False, 0
        while not done:
            action, _ = model.predict(obs, deterministic=True,
                                      action_masks=env.env_method("action_masks")[0])
            obs, reward, done, info = env.step(action)
            ep_reward += reward[0]
        
        info = info[0]
        rewards.append(ep_reward)
        success_rates.append(info.get('success_rate', 0))
        small_cloud.append(info.get('small_on_cloud', 0))
        large_cloud.append(info.get('large_on_cloud', 0))
        for c, n in info.get('cluster_placements', {}).items():
            cluster_dist[c] += n
    
    results = {
        'mean_reward': np.mean(rewards),
        'mean_success_rate': np.mean(success_rates),
        'total_small_on_cloud': sum(small_cloud),
        'total_large_on_cloud': sum(large_cloud),
        'cluster_distribution': dict(cluster_dist),
    }
    
    total_cloud = results['total_small_on_cloud'] + results['total_large_on_cloud']
    results['strategic_ratio'] = results['total_large_on_cloud'] / total_cloud if total_cloud else 0
    
    if verbose:
        print(f"  Reward: {results['mean_reward']:.2f}")
        print(f"  Success: {results['mean_success_rate']*100:.1f}%")
        print(f"  Strategic ratio: {results['strategic_ratio']*100:.1f}%")
    
    return results


# === MAIN ===

def main():
    parser = argparse.ArgumentParser(description="CloudContinuum RL Training")
    parser.add_argument("--save-dir", default="./models")
    parser.add_argument("--start-stage", default="stage1_easy",
                        choices=["stage1_easy", "stage2_medium", "stage3_hard"])
    parser.add_argument("--resume-path", default=None)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--eval-only", action="store_true")
    parser.add_argument("--model-path", default=None)
    parser.add_argument("--eval-episodes", type=int, default=100)
    parser.add_argument("--eval-difficulty", default="hard", choices=["easy", "medium", "hard"])
    parser.add_argument("--log-training-data", action="store_true",
                        help="Save training data to CSV for analysis")
    args = parser.parse_args()
    
    if args.eval_only:
        if not args.model_path:
            raise ValueError("--model-path required for --eval-only")
        evaluate_model(args.model_path, args.eval_difficulty, args.eval_episodes)
    else:
        train_curriculum(args.save_dir, args.start_stage, args.resume_path, args.seed,
                        args.log_training_data)
        final_path = os.path.join(args.save_dir, "stage3_hard", "final_model.zip")
        if os.path.exists(final_path):
            evaluate_model(final_path, "hard", 100)


if __name__ == "__main__":
    main()