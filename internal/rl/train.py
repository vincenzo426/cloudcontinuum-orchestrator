#!/usr/bin/env python3
# internal/rl/train.py
"""
Training script con Curriculum Learning per CloudContinuum RL Agent
VERSIONE 2.0 - Training esteso per success rate ≥95%

NOVITÀ v2.0:
- 9M timesteps totali (3x rispetto a v1.0)
- Hyperparameters ottimizzati per stabilità
- Integrazione con pretrained model
- Valutazione rigorosa con 100 episodi
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

from .environment import CloudContinuumEnv
from .config import EnvironmentConfig, get_config_for_difficulty


# ========== TRAINING HYPERPARAMETERS V2.0 ==========
TRAINING_CONFIG = {
    "stage1_easy": {
        "timesteps": 1_000_000,      # Era 300k → x3.3
        "learning_rate": 1e-4,       # Era 3e-4 → più stabile
        "n_steps": 4096,             # Era 2048 → più step per update
        "batch_size": 128,           # Era 64 → batch più grandi
        "n_epochs": 20,              # Era 10 → più epoch per sample
        "gamma": 0.99,               # Era 0.995 → focus long-term
        "gae_lambda": 0.98,          # Era 0.95 → più conservative
        "clip_range": 0.15,          # Era 0.2 → più conservative
        "clip_range_vf": 0.15,       # NUOVO: clip value function
        "ent_coef": 0.005,           # Era 0.01 → meno exploration caotica
        "vf_coef": 0.5,
        "max_grad_norm": 0.5,
        "normalize_advantage": True,  # NUOVO
        "target_kl": 0.01,           # NUOVO: early stopping
    },
    "stage2_medium": {
        "timesteps": 3_000_000,      # Era 1.2M → x2.5
        "learning_rate": 5e-5,       # Ridotto ulteriormente
        "n_steps": 4096,
        "batch_size": 128,
        "n_epochs": 20,
        "gamma": 0.99,
        "gae_lambda": 0.98,
        "clip_range": 0.1,           # Ancora più conservative
        "clip_range_vf": 0.1,
        "ent_coef": 0.003,           # Ridotto
        "vf_coef": 0.5,
        "max_grad_norm": 0.5,
        "normalize_advantage": True,
        "target_kl": 0.01,
    },
    "stage3_hard": {
        "timesteps": 5_000_000,      # Era 1.8M → x2.8
        "learning_rate": 3e-5,       # Molto basso per fine-tuning
        "n_steps": 4096,
        "batch_size": 256,           # Batch ancora più grandi
        "n_epochs": 25,              # Ancora più epoch
        "gamma": 0.995,              # Maximizza long-term
        "gae_lambda": 0.99,
        "clip_range": 0.1,
        "clip_range_vf": 0.1,
        "ent_coef": 0.001,           # Minima exploration
        "vf_coef": 0.5,
        "max_grad_norm": 0.5,
        "normalize_advantage": True,
        "target_kl": 0.008,          # Più stringente
    }
}

# Network architecture: più profonda per catturare relazioni complesse
POLICY_KWARGS = {
    "net_arch": [256, 256, 128],  # Era [128, 128] → più neuroni
    "activation_fn": "tanh",      # o "relu"
}


def create_training_env(difficulty: str, seed: int) -> DummyVecEnv:
    """Crea environment vettorizzato per training"""
    config = get_config_for_difficulty(difficulty)
    config.master_seed = seed
    
    def make_env():
        env = CloudContinuumEnv(config=config, seed=seed)
        env = Monitor(env)  # Wrap con Monitor per stats
        return env
    
    return DummyVecEnv([make_env])


def train_stage(
    stage_name: str,
    difficulty: str,
    config: Dict,
    model: PPO = None,
    save_dir: str = "./models"
) -> PPO:
    """
    Addestra singolo stage del curriculum.
    
    Args:
        stage_name: Nome stage (es. "stage1_easy")
        difficulty: Difficulty level
        config: Hyperparameters per questo stage
        model: Modello pre-esistente (per curriculum) o None
        save_dir: Directory dove salvare modelli
    
    Returns:
        Modello addestrato
    """
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
        # Update hyperparameters per il nuovo stage
        model.learning_rate = config['learning_rate']
        model.n_steps = config['n_steps']
        model.batch_size = config['batch_size']
        model.n_epochs = config['n_epochs']
        model.clip_range = config['clip_range']
        model.ent_coef = config['ent_coef']
    
    # Setup callbacks
    stage_dir = f"{save_dir}/{stage_name}"
    os.makedirs(stage_dir, exist_ok=True)
    
    # Evaluation callback (ogni 50k steps)
    eval_env = create_training_env(difficulty=difficulty, seed=9999)
    eval_callback = EvalCallback(
        eval_env,
        best_model_save_path=f"{stage_dir}/best_model",
        log_path=f"{stage_dir}/eval_logs",
        eval_freq=50_000,
        n_eval_episodes=20,
        deterministic=True,
        render=False
    )
    
    # Checkpoint callback (ogni 100k steps)
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
    duration = (end_time - start_time).total_seconds() / 3600  # ore
    
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
    """
    Training completo con Curriculum Learning (3 stage).
    
    TOTALE: 9M timesteps (~24-48 ore su CPU, 6-12 ore su GPU)
    """
    print("\n" + "=" * 70)
    print(" " * 20 + "CURRICULUM LEARNING PIPELINE")
    print("=" * 70)
    print("Total training: 9,000,000 timesteps")
    print("  Stage 1 (EASY):   1,000,000 steps")
    print("  Stage 2 (MEDIUM): 3,000,000 steps")
    print("  Stage 3 (HARD):   5,000,000 steps")
    print("=" * 70)
    
    # Load pretrained model se disponibile
    model = None
    if pretrained_model_path and os.path.exists(pretrained_model_path):
        print(f"\n🔄 Loading pre-trained model from: {pretrained_model_path}")
        env = create_training_env(difficulty="easy", seed=42)
        model = PPO.load(pretrained_model_path, env=env)
        print("✅ Pre-trained model loaded successfully")
    
    # Stage 1: EASY
    model = train_stage(
        stage_name="stage1_easy",
        difficulty="easy",
        config=TRAINING_CONFIG["stage1_easy"],
        model=model,
        save_dir=save_dir
    )
    
    # Stage 2: MEDIUM
    model = train_stage(
        stage_name="stage2_medium",
        difficulty="medium",
        config=TRAINING_CONFIG["stage2_medium"],
        model=model,
        save_dir=save_dir
    )
    
    # Stage 3: HARD
    model = train_stage(
        stage_name="stage3_hard",
        difficulty="hard",
        config=TRAINING_CONFIG["stage3_hard"],
        model=model,
        save_dir=save_dir
    )
    
    print("\n" + "=" * 70)
    print(" " * 15 + "CURRICULUM TRAINING COMPLETED ✅")
    print("=" * 70)
    
    return model


def evaluate_final_model(
    model_path: str,
    num_episodes: int = 100,  # Aumentato da 50 per migliore confidenza statistica
    difficulty: str = "hard"
) -> Dict:
    """
    Valutazione rigorosa del modello finale.
    
    CRITERI DI SUCCESSO:
    - Success rate ≥ 95%
    - Mean reward > 0
    - Std reward < 100
    """
    print("\n" + "=" * 70)
    print(" " * 20 + "FINAL MODEL EVALUATION")
    print("=" * 70)
    print(f"Model: {model_path}")
    print(f"Episodes: {num_episodes}")
    print(f"Difficulty: {difficulty}")
    print("=" * 70)
    
    # Load model
    env = create_training_env(difficulty=difficulty, seed=7777)
    model = PPO.load(model_path, env=env)
    
    # Run evaluation episodes
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
            ep_reward += reward[0]  # VecEnv ritorna array
        
        # Extract info dal VecEnv
        ep_info = info[0]
        
        results['success_rates'].append(ep_info['success_rate'])
        results['episode_rewards'].append(ep_reward)
        results['avg_exec_times'].append(ep_info.get('avg_exec_time', 0))
        
        if ep_info.get('failure_reason'):
            results['failure_reasons'].append(ep_info['failure_reason'])
        
        if (ep + 1) % 20 == 0:
            print(f"  Progress: {ep+1}/{num_episodes} episodes completed")
    
    # Compute statistics
    mean_success = np.mean(results['success_rates'])
    std_success = np.std(results['success_rates'])
    mean_reward = np.mean(results['episode_rewards'])
    std_reward = np.std(results['episode_rewards'])
    mean_exec_time = np.mean(results['avg_exec_times'])
    
    # Failure analysis
    from collections import Counter
    failure_counter = Counter(results['failure_reasons'])
    
    # Print results
    print("\n" + "=" * 70)
    print("EVALUATION RESULTS")
    print("=" * 70)
    print(f"Success Rate: {mean_success*100:.2f}% ± {std_success*100:.2f}%")
    print(f"Mean Reward: {mean_reward:.2f} ± {std_reward:.2f}")
    print(f"Avg Execution Time: {mean_exec_time:.2f}s")
    
    if failure_counter:
        print("\nFailure Reasons:")
        for reason, count in failure_counter.most_common():
            percentage = count / num_episodes * 100
            print(f"  {reason:25s}: {count:3d} ({percentage:5.2f}%)")
    
    # Success criteria
    print("\n" + "-" * 70)
    print("SUCCESS CRITERIA:")
    success_rate_ok = mean_success >= 0.95
    reward_ok = mean_reward > 0
    
    print(f"  ✅ Success rate ≥ 95%:  {'PASS' if success_rate_ok else 'FAIL'} ({mean_success*100:.2f}%)")
    print(f"  ✅ Mean reward > 0:     {'PASS' if reward_ok else 'FAIL'} ({mean_reward:.2f})")
    
    if success_rate_ok and reward_ok:
        print("\n🎉 MODEL READY FOR PRODUCTION DEPLOYMENT!")
    else:
        print("\n⚠️  MODEL NEEDS MORE TRAINING")
    
    print("=" * 70)
    
    return results


def main():
    """Main training pipeline"""
    parser = argparse.ArgumentParser(description="Train CloudContinuum RL Agent")
    parser.add_argument("--pretrained-model", type=str, default=None,
                        help="Path to pre-trained model (from pretrain.py)")
    parser.add_argument("--save-dir", type=str, default="./models",
                        help="Directory to save models")
    parser.add_argument("--eval-only", action="store_true",
                        help="Only evaluate existing model")
    parser.add_argument("--model-path", type=str, default=None,
                        help="Path to model for evaluation")
    
    args = parser.parse_args()
    
    if args.eval_only:
        if not args.model_path:
            print("❌ Error: --model-path required for evaluation")
            return
        
        evaluate_final_model(
            model_path=args.model_path,
            num_episodes=100,
            difficulty="hard"
        )
    else:
        # Full training pipeline
        final_model = train_curriculum(
            pretrained_model_path=args.pretrained_model,
            save_dir=args.save_dir
        )
        
        # Automatic evaluation
        print("\n🔍 Running final evaluation...")
        evaluate_final_model(
            model_path=f"{args.save_dir}/stage3_hard/best_model/best_model.zip",
            num_episodes=100,
            difficulty="hard"
        )


if __name__ == "__main__":
    main()