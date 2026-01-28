# internal/rl/train.py
"""
CloudContinuum RL - Training Script
VERSIONE 4.0 - CURRICULUM LEARNING WITH ANTI-MYOPIC REWARDS

Training di agente MaskablePPO per placement intelligente di pipeline ML.

CURRICULUM:
    Stage 1 (Easy):   4 pipeline, cluster scarichi, locality chiara
    Stage 2 (Medium): 8 pipeline, carico reale, locality 50%
    Stage 3 (Hard):   12 pipeline, cluster carichi, locality rara

OBIETTIVI:
    1. Minimizzare tempo esecuzione
    2. Evitare scelte miopi (no small pipeline su cloud)
    3. Bilanciare carico tra cluster

USAGE:
    python -m internal.rl.train --save-dir ./models
    python -m internal.rl.train --eval-only --model-path ./models/stage3_hard/final_model.zip
"""

import os
import argparse
import numpy as np
from typing import Dict, List, Optional, Any
from datetime import datetime
from collections import defaultdict

import torch.nn as nn

# Stable Baselines 3
from sb3_contrib import MaskablePPO
from sb3_contrib.common.wrappers import ActionMasker
from sb3_contrib.common.maskable.callbacks import MaskableEvalCallback

from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize
from stable_baselines3.common.callbacks import (
    BaseCallback, 
    CheckpointCallback, 
    CallbackList
)
from stable_baselines3.common.monitor import Monitor
from stable_baselines3.common.logger import configure

from .environment import CloudContinuumEnv
from .config import EnvironmentConfig, get_config_for_difficulty


# =============================================================================
# TRAINING CONFIGURATION
# =============================================================================

TRAINING_CONFIG = {
    "stage1_easy": {
        "difficulty": "easy",
        "timesteps": 750_000,       # 500k - Impara basi: action mask, locality
        "learning_rate": 3e-4,
        "n_steps": 1024,
        "batch_size": 64,
        "n_epochs": 10,
        "gamma": 0.99,
        "gae_lambda": 0.95,
        "clip_range": 0.2,
        "ent_coef": 0.05,           # Alta exploration
        "vf_coef": 0.5,
        "max_grad_norm": 0.5,
    },
    "stage2_medium": {
        "difficulty": "medium",
        "timesteps": 1_500_000,     # 1M - Impara: quando usare cloud vs edge
        "learning_rate": 1e-4,
        "n_steps": 2048,
        "batch_size": 128,
        "n_epochs": 10,
        "gamma": 0.99,
        "gae_lambda": 0.95,
        "clip_range": 0.15,
        "ent_coef": 0.03,           # Era 0.02, aumentato per più exploration
        "vf_coef": 0.5,
        "max_grad_norm": 0.5,
    },
    "stage3_hard": {
        "difficulty": "hard",
        "timesteps": 2_000_000,     # 2M - Raffina: anti-miopatia, bilanciamento
        "learning_rate": 5e-5,
        "n_steps": 2048,
        "batch_size": 256,
        "n_epochs": 15,
        "gamma": 0.995,
        "gae_lambda": 0.98,
        "clip_range": 0.1,
        "ent_coef": 0.02,           # Era 0.01, raddoppiato per evitare stallo
        "vf_coef": 0.5,
        "max_grad_norm": 0.5,
    },
}

# Policy network architecture
POLICY_KWARGS = {
    "net_arch": dict(
        pi=[128, 128],   # Policy network
        vf=[128, 128]    # Value network
    ),
    "activation_fn": nn.ReLU,
}


# =============================================================================
# CALLBACKS
# =============================================================================

class AntiMyopicMetricsCallback(BaseCallback):
    """
    Callback per loggare metriche anti-miopatia su TensorBoard.
    
    Metriche:
    - success_rate: % placement riusciti
    - avg_exec_time: tempo medio esecuzione
    - avg_efficiency: efficienza media (ideal_time / actual_time)
    - small_on_cloud_rate: % pipeline piccole su cloud (da minimizzare!)
    - large_on_cloud_rate: % pipeline grandi su cloud (da massimizzare!)
    - locality_rate: % placement con data locality
    - contention_failure_rate: % fallimenti per contesa
    - resources_released: job completati per episodio
    - cluster_balance: distribuzione tra cluster
    """
    
    def __init__(self, verbose: int = 0):
        super().__init__(verbose)
        self.episode_metrics = defaultdict(list)
    
    def _on_step(self) -> bool:
        # Controlla se episodio terminato
        infos = self.locals.get("infos", [])
        dones = self.locals.get("dones", [])
        
        for idx, done in enumerate(dones):
            if done and idx < len(infos):
                info = infos[idx]
                
                # Raccogli metriche base
                if 'success_rate' in info:
                    self.episode_metrics['success_rate'].append(info['success_rate'])
                if 'avg_exec_time' in info:
                    self.episode_metrics['avg_exec_time'].append(info['avg_exec_time'])
                if 'locality_rate' in info:
                    self.episode_metrics['locality_rate'].append(info['locality_rate'])
                
                # Nuove metriche: efficiency e contention
                if 'avg_efficiency' in info:
                    self.episode_metrics['avg_efficiency'].append(info['avg_efficiency'])
                if 'placements_failed_contention' in info:
                    total_failed = info.get('placements_failed', 0)
                    contention_failed = info.get('placements_failed_contention', 0)
                    if total_failed > 0:
                        contention_rate = contention_failed / total_failed
                        self.episode_metrics['contention_failure_rate'].append(contention_rate)
                if 'resources_released' in info:
                    self.episode_metrics['resources_released'].append(info['resources_released'])
                
                # Anti-myopic metrics
                successful = info.get('placements_successful', 0)
                if successful > 0:
                    small_on_cloud = info.get('small_on_cloud', 0)
                    large_on_cloud = info.get('large_on_cloud', 0)
                    
                    self.episode_metrics['small_on_cloud'].append(small_on_cloud)
                    self.episode_metrics['large_on_cloud'].append(large_on_cloud)
                
                # Cluster distribution
                cluster_placements = info.get('cluster_placements', {})
                total = sum(cluster_placements.values())
                if total > 0:
                    cloud_pct = cluster_placements.get('cloud_cluster', 0) / total
                    self.episode_metrics['cloud_usage_pct'].append(cloud_pct)
                
                # Diagnostica categorie pipeline
                pipeline_cats = info.get('pipeline_categories', {})
                if pipeline_cats:
                    total_cats = sum(pipeline_cats.values())
                    if total_cats > 0:
                        large_pct = pipeline_cats.get('large', 0) / total_cats
                        self.episode_metrics['large_pipeline_pct'].append(large_pct)
        
        # Log ogni 100 episodi
        if len(self.episode_metrics['success_rate']) >= 100:
            self._log_metrics()
            self.episode_metrics.clear()
        
        return True
    
    def _log_metrics(self):
        """Logga metriche aggregate su TensorBoard."""
        if self.logger is None:
            return
        
        for key, values in self.episode_metrics.items():
            if len(values) > 0:
                mean_val = np.mean(values)
                self.logger.record(f"anti_myopic/{key}", mean_val)
        
        # Log metriche derivate
        if 'small_on_cloud' in self.episode_metrics:
            total_small = sum(self.episode_metrics['small_on_cloud'])
            total_large = sum(self.episode_metrics['large_on_cloud'])
            
            if total_small + total_large > 0:
                # Ratio: idealmente large >> small su cloud
                strategic_ratio = total_large / (total_small + total_large + 1e-8)
                self.logger.record("anti_myopic/strategic_cloud_ratio", strategic_ratio)


class ProgressCallback(BaseCallback):
    """Callback per mostrare progresso durante training."""
    
    def __init__(self, total_timesteps: int, stage_name: str, verbose: int = 1):
        super().__init__(verbose)
        self.total_timesteps = total_timesteps
        self.stage_name = stage_name
        self.last_log = 0
        self.log_interval = 10000
    
    def _on_step(self) -> bool:
        if self.num_timesteps - self.last_log >= self.log_interval:
            progress = (self.num_timesteps / self.total_timesteps) * 100
            print(f"  [{self.stage_name}] Progress: {progress:.1f}% "
                  f"({self.num_timesteps:,}/{self.total_timesteps:,})")
            self.last_log = self.num_timesteps
        return True


# =============================================================================
# ENVIRONMENT FACTORY
# =============================================================================

def mask_fn(env: CloudContinuumEnv) -> np.ndarray:
    """Funzione per estrarre action mask dall'environment."""
    return env.action_masks()


def make_env(difficulty: str, seed: int) -> CloudContinuumEnv:
    """Factory per creare environment con configurazione corretta."""
    config = get_config_for_difficulty(difficulty)
    config.master_seed = seed
    env = CloudContinuumEnv(config=config, seed=seed)
    return env


def create_training_env(difficulty: str, seed: int, log_dir: str = None) -> DummyVecEnv:
    """
    Crea environment per training con Monitor e ActionMasker.
    
    Args:
        difficulty: "easy", "medium", "hard"
        seed: Random seed
        log_dir: Directory per Monitor logs
    
    Returns:
        DummyVecEnv wrappato
    """
    def _make():
        env = make_env(difficulty, seed)
        if log_dir:
            env = Monitor(env, log_dir)
        else:
            env = Monitor(env)
        env = ActionMasker(env, mask_fn)
        return env
    
    return DummyVecEnv([_make])


# =============================================================================
# TRAINING FUNCTIONS
# =============================================================================

def train_stage(
    stage_name: str,
    config: Dict,
    model: Optional[MaskablePPO] = None,
    save_dir: str = "./models",
    seed: int = 42
) -> MaskablePPO:
    """
    Addestra un singolo stage del curriculum.
    
    Args:
        stage_name: Nome dello stage (es. "stage1_easy")
        config: Configurazione training per questo stage
        model: Modello esistente da continuare (o None per nuovo)
        save_dir: Directory per salvare modelli
        seed: Random seed
    
    Returns:
        Modello addestrato
    """
    print("\n" + "=" * 70)
    print(f"  TRAINING STAGE: {stage_name.upper()}")
    print("=" * 70)
    
    difficulty = config['difficulty']
    timesteps = config['timesteps']
    
    print(f"  Difficulty: {difficulty}")
    print(f"  Timesteps: {timesteps:,}")
    print(f"  Learning rate: {config['learning_rate']}")
    print(f"  Entropy coef: {config['ent_coef']}")
    print("-" * 70)
    
    # Directories
    stage_dir = os.path.join(save_dir, stage_name)
    os.makedirs(stage_dir, exist_ok=True)
    log_dir = os.path.join(stage_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    # Create training environment
    train_env = create_training_env(difficulty, seed, log_dir)
    train_env = VecNormalize(
        train_env,
        norm_obs=True,
        norm_reward=True,
        clip_obs=10.0,
        clip_reward=10.0
    )
    
    # Create evaluation environment
    eval_env = create_training_env(difficulty, seed + 1000)
    eval_env = VecNormalize(
        eval_env,
        norm_obs=True,
        norm_reward=False,  # Non normalizzare reward per eval
        clip_obs=10.0,
        training=False
    )
    
    # TensorBoard logger
    tb_log_dir = os.path.join(stage_dir, "tensorboard")
    
    # Create or update model
    if model is None:
        print("  Creating new MaskablePPO model...")
        model = MaskablePPO(
            policy="MlpPolicy",
            env=train_env,
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
            tensorboard_log=tb_log_dir,
            verbose=1,
            seed=seed,
        )
    else:
        print("  Continuing from existing model...")
        model.set_env(train_env)
        
        # Update hyperparameters
        model.learning_rate = config['learning_rate']
        model.ent_coef = config['ent_coef']
        model.clip_range = lambda _: config['clip_range']
        model.n_epochs = config['n_epochs']
        model.batch_size = config['batch_size']
        
        # Update logger
        new_logger = configure(tb_log_dir, ["stdout", "tensorboard"])
        model.set_logger(new_logger)
    
    # Sync normalization stats
    eval_env.obs_rms = train_env.obs_rms
    
    # Callbacks
    callbacks = CallbackList([
        # Evaluation callback
        MaskableEvalCallback(
            eval_env,
            best_model_save_path=os.path.join(stage_dir, "best_model"),
            log_path=os.path.join(stage_dir, "eval_logs"),
            eval_freq=10000,
            n_eval_episodes=20,
            deterministic=True,
            verbose=1,
        ),
        # Checkpoint callback
        CheckpointCallback(
            save_freq=50000,
            save_path=os.path.join(stage_dir, "checkpoints"),
            name_prefix=f"{stage_name}_ckpt",
            verbose=0,
        ),
        # Anti-myopic metrics
        AntiMyopicMetricsCallback(verbose=0),
        # Progress
        ProgressCallback(timesteps, stage_name, verbose=1),
    ])
    
    # Train
    print(f"\n🚀 Starting training for {timesteps:,} timesteps...")
    print(f"📊 TensorBoard: tensorboard --logdir {tb_log_dir}")
    print("-" * 70)
    
    model.learn(
        total_timesteps=timesteps,
        callback=callbacks,
        progress_bar=True,
        reset_num_timesteps=(model is None),  # Reset solo per nuovo modello
    )
    
    # Save final model and normalization stats
    final_model_path = os.path.join(stage_dir, "final_model.zip")
    model.save(final_model_path)
    
    vec_norm_path = os.path.join(stage_dir, "vec_normalize.pkl")
    train_env.save(vec_norm_path)
    
    print(f"\n✅ Stage {stage_name} completed!")
    print(f"   Model saved: {final_model_path}")
    print(f"   VecNormalize saved: {vec_norm_path}")
    
    return model


def train_curriculum(
    save_dir: str = "./models",
    start_stage: str = "stage1_easy",
    resume_path: Optional[str] = None,
    seed: int = 42
) -> MaskablePPO:
    """
    Esegue training completo con curriculum learning.
    
    Args:
        save_dir: Directory per salvare modelli
        start_stage: Stage da cui partire
        resume_path: Path a modello .zip per riprendere training
        seed: Random seed
    
    Returns:
        Modello finale addestrato
    """
    print("\n" + "=" * 70)
    print("  CLOUDCONTINUUM RL - CURRICULUM LEARNING v4.0")
    print("=" * 70)
    print(f"\n📁 Save directory: {save_dir}")
    print(f"🎲 Seed: {seed}")
    
    # Stages definition
    stages = [
        ("stage1_easy", TRAINING_CONFIG["stage1_easy"]),
        ("stage2_medium", TRAINING_CONFIG["stage2_medium"]),
        ("stage3_hard", TRAINING_CONFIG["stage3_hard"]),
    ]
    
    # Find start index
    start_idx = 0
    for i, (name, _) in enumerate(stages):
        if name == start_stage:
            start_idx = i
            break
    
    stages_to_run = stages[start_idx:]
    
    print(f"\n📚 Curriculum stages: {[s[0] for s in stages_to_run]}")
    total_timesteps = sum(cfg['timesteps'] for _, cfg in stages_to_run)
    print(f"⏱️  Total timesteps: {total_timesteps:,}")
    
    # Load existing model if resuming
    model = None
    if resume_path:
        print(f"\n🔄 Loading model from: {resume_path}")
        if not os.path.exists(resume_path):
            raise FileNotFoundError(f"Model not found: {resume_path}")
        
        # Create dummy env for loading
        dummy_env = create_training_env("easy", seed)
        model = MaskablePPO.load(resume_path, env=dummy_env)
        print("✅ Model loaded successfully")
    
    # Run curriculum
    for stage_name, stage_config in stages_to_run:
        model = train_stage(
            stage_name=stage_name,
            config=stage_config,
            model=model,
            save_dir=save_dir,
            seed=seed
        )
    
    print("\n" + "=" * 70)
    print("  CURRICULUM TRAINING COMPLETED!")
    print("=" * 70)
    
    return model


# =============================================================================
# EVALUATION
# =============================================================================

def evaluate_model(
    model_path: str,
    difficulty: str = "hard",
    num_episodes: int = 100,
    deterministic: bool = True,
    verbose: bool = True
) -> Dict[str, Any]:
    """
    Valuta modello addestrato.
    
    Args:
        model_path: Path al modello .zip
        difficulty: Difficoltà environment
        num_episodes: Numero episodi di valutazione
        deterministic: Se True, usa azioni deterministiche
        verbose: Se True, stampa risultati dettagliati
    
    Returns:
        Dict con metriche di valutazione
    """
    if verbose:
        print("\n" + "=" * 70)
        print("  MODEL EVALUATION")
        print("=" * 70)
        print(f"  Model: {model_path}")
        print(f"  Difficulty: {difficulty}")
        print(f"  Episodes: {num_episodes}")
        print("-" * 70)
    
    # Load model
    model_dir = os.path.dirname(model_path)
    vec_norm_path = os.path.join(model_dir, "vec_normalize.pkl")
    
    # Create environment
    env = create_training_env(difficulty, seed=9999)
    
    # Load normalization stats if available
    if os.path.exists(vec_norm_path):
        if verbose:
            print(f"  Loading VecNormalize from: {vec_norm_path}")
        env = VecNormalize.load(vec_norm_path, env)
        env.training = False
        env.norm_reward = False
    else:
        if verbose:
            print("  ⚠️  VecNormalize not found, using unnormalized env")
    
    # Load model
    model = MaskablePPO.load(model_path, env=env)
    
    # Evaluation loop
    episode_rewards = []
    episode_exec_times = []
    episode_efficiencies = []
    success_rates = []
    locality_rates = []
    small_on_cloud_counts = []
    large_on_cloud_counts = []
    contention_failures = []
    resources_released = []
    cluster_distributions = defaultdict(int)
    
    # Diagnostica categorie pipeline
    total_pipeline_categories = defaultdict(int)
    total_pipeline_categories_placed = defaultdict(int)
    
    if verbose:
        print(f"\n  Running {num_episodes} episodes...")
    
    for ep in range(num_episodes):
        obs = env.reset()
        episode_reward = 0
        done = False
        
        while not done:
            action_masks = env.env_method("action_masks")[0]
            action, _ = model.predict(
                obs, 
                deterministic=deterministic,
                action_masks=action_masks
            )
            obs, reward, done, info = env.step(action)
            episode_reward += reward[0]
        
        # Extract info from last step
        info = info[0]
        
        episode_rewards.append(episode_reward)
        success_rates.append(info.get('success_rate', 0))
        locality_rates.append(info.get('locality_rate', 0))
        episode_exec_times.append(info.get('avg_exec_time', 0))
        episode_efficiencies.append(info.get('avg_efficiency', 0))
        small_on_cloud_counts.append(info.get('small_on_cloud', 0))
        large_on_cloud_counts.append(info.get('large_on_cloud', 0))
        contention_failures.append(info.get('placements_failed_contention', 0))
        resources_released.append(info.get('resources_released', 0))
        
        # Raccogli categorie pipeline
        for cat, count in info.get('pipeline_categories', {}).items():
            total_pipeline_categories[cat] += count
        for cat, count in info.get('pipeline_categories_placed', {}).items():
            total_pipeline_categories_placed[cat] += count
        
        # Cluster distribution
        for cluster, count in info.get('cluster_placements', {}).items():
            cluster_distributions[cluster] += count
        
        if verbose and (ep + 1) % 20 == 0:
            print(f"    Completed {ep + 1}/{num_episodes} episodes...")
    
    # Calculate statistics
    results = {
        'mean_reward': np.mean(episode_rewards),
        'std_reward': np.std(episode_rewards),
        'mean_success_rate': np.mean(success_rates),
        'std_success_rate': np.std(success_rates),
        'mean_exec_time': np.mean(episode_exec_times),
        'mean_efficiency': np.mean(episode_efficiencies),
        'mean_locality_rate': np.mean(locality_rates),
        'total_small_on_cloud': sum(small_on_cloud_counts),
        'total_large_on_cloud': sum(large_on_cloud_counts),
        'total_contention_failures': sum(contention_failures),
        'total_resources_released': sum(resources_released),
        'cluster_distribution': dict(cluster_distributions),
        'pipeline_categories': dict(total_pipeline_categories),
        'pipeline_categories_placed': dict(total_pipeline_categories_placed),
    }
    
    # Calculate anti-myopic score
    total_cloud_placements = results['total_small_on_cloud'] + results['total_large_on_cloud']
    if total_cloud_placements > 0:
        strategic_ratio = results['total_large_on_cloud'] / total_cloud_placements
    else:
        strategic_ratio = 0.0
    results['strategic_cloud_ratio'] = strategic_ratio
    
    if verbose:
        print("\n" + "-" * 70)
        print("  EVALUATION RESULTS")
        print("-" * 70)
        print(f"  📊 Performance:")
        print(f"     Mean Reward:     {results['mean_reward']:.2f} ± {results['std_reward']:.2f}")
        print(f"     Success Rate:    {results['mean_success_rate']*100:.1f}%")
        print(f"     Avg Exec Time:   {results['mean_exec_time']:.1f}s")
        print(f"     Avg Efficiency:  {results['mean_efficiency']*100:.1f}%")
        print(f"     Data Locality:   {results['mean_locality_rate']*100:.1f}%")
        
        print(f"\n  🔬 Pipeline Categories (Diagnostica):")
        print(f"     Generated: {dict(total_pipeline_categories)}")
        print(f"     Placed:    {dict(total_pipeline_categories_placed)}")
        total_generated = sum(total_pipeline_categories.values())
        total_placed = sum(total_pipeline_categories_placed.values())
        if total_generated > 0:
            for cat in ['small', 'medium', 'large']:
                gen = total_pipeline_categories.get(cat, 0)
                placed = total_pipeline_categories_placed.get(cat, 0)
                pct_gen = gen / total_generated * 100
                pct_placed = placed / total_placed * 100 if total_placed > 0 else 0
                print(f"       {cat:6s}: {gen:4d} generated ({pct_gen:5.1f}%), {placed:4d} placed ({pct_placed:5.1f}%)")
        
        print(f"\n  🎯 Anti-Myopic Metrics:")
        print(f"     Small on Cloud:  {results['total_small_on_cloud']}")
        print(f"     Large on Cloud:  {results['total_large_on_cloud']}")
        print(f"     Strategic Ratio: {strategic_ratio*100:.1f}% "
              f"({'✅ GOOD' if strategic_ratio > 0.6 else '⚠️  NEEDS IMPROVEMENT'})")
        
        print(f"\n  🔥 Contention & Resources:")
        print(f"     Contention Failures: {results['total_contention_failures']}")
        print(f"     Resources Released:  {results['total_resources_released']}")
        
        print(f"\n  📍 Cluster Distribution:")
        total_placements = sum(cluster_distributions.values())
        for cluster, count in sorted(cluster_distributions.items()):
            pct = (count / total_placements * 100) if total_placements > 0 else 0
            print(f"     {cluster:15s}: {pct:5.1f}% ({count})")
        
        print("=" * 70)
    
    return results


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="CloudContinuum RL Training Script"
    )
    
    # Training arguments
    parser.add_argument(
        "--save-dir", type=str, default="./models",
        help="Directory to save models"
    )
    parser.add_argument(
        "--start-stage", type=str, default="stage1_easy",
        choices=["stage1_easy", "stage2_medium", "stage3_hard"],
        help="Stage to start from"
    )
    parser.add_argument(
        "--resume-path", type=str, default=None,
        help="Path to model .zip to resume training"
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed"
    )
    
    # Evaluation arguments
    parser.add_argument(
        "--eval-only", action="store_true",
        help="Only evaluate, don't train"
    )
    parser.add_argument(
        "--model-path", type=str, default=None,
        help="Path to model for evaluation"
    )
    parser.add_argument(
        "--eval-episodes", type=int, default=100,
        help="Number of evaluation episodes"
    )
    parser.add_argument(
        "--eval-difficulty", type=str, default="hard",
        choices=["easy", "medium", "hard"],
        help="Difficulty for evaluation"
    )
    
    args = parser.parse_args()
    
    if args.eval_only:
        # Evaluation mode
        if args.model_path is None:
            raise ValueError("--model-path required for --eval-only")
        
        evaluate_model(
            model_path=args.model_path,
            difficulty=args.eval_difficulty,
            num_episodes=args.eval_episodes,
            verbose=True
        )
    else:
        # Training mode
        model = train_curriculum(
            save_dir=args.save_dir,
            start_stage=args.start_stage,
            resume_path=args.resume_path,
            seed=args.seed
        )
        
        # Evaluate final model
        print("\n🎯 Running final evaluation...")
        final_model_path = os.path.join(
            args.save_dir, "stage3_hard", "final_model.zip"
        )
        
        if os.path.exists(final_model_path):
            evaluate_model(
                model_path=final_model_path,
                difficulty="hard",
                num_episodes=100,
                verbose=True
            )


if __name__ == "__main__":
    main()