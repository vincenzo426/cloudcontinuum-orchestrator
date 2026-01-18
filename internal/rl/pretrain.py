#!/usr/bin/env python3
# internal/rl/pretrain.py
"""
Imitation Learning per accelerare training RL Agent
Pre-addestra policy copiando comportamento di expert heuristic

BENEFICI:
- Accelera convergenza di ~50%
- Parte da policy "ragionevole" invece che random
- Riduce exploration caotica nelle prime fasi
"""

import os
import argparse
import numpy as np
from typing import List, Tuple, Dict
from tqdm import tqdm
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader

from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import DummyVecEnv

from .environment import CloudContinuumEnv
from .config import EnvironmentConfig, get_config_for_difficulty


class ExpertPolicy:
    """
    Expert policy basata su simple-heuristic strategy.
    
    REGOLE:
    1. Se pipeline LIGHT (<2000m CPU) → data locality
    2. Se pipeline HEAVY (≥2000m CPU) → cloud cluster
    3. Fallback → cluster con più risorse disponibili
    """
    
    def __init__(self, env: CloudContinuumEnv):
        self.env = env
        self.light_threshold = 2000  # millicores
    
    def get_action(self, obs: np.ndarray) -> int:
        """
        Decide azione basandosi su regole expert.
        
        Args:
            obs: Observation vector (non usato - usa stato interno env)
        
        Returns:
            Action index (cluster)
        """
        # Get current pipeline from environment
        if self.env.current_pipeline_idx >= len(self.env.pipelines_queue):
            return 0  # Fallback
        
        pipeline = self.env.pipelines_queue[self.env.current_pipeline_idx]
        
        # RULE 1: Light pipeline → data locality
        if pipeline['cpu_required'] < self.light_threshold:
            if pipeline['data_location'] != "none":
                # Trova index del cluster con i dati
                for idx, cluster_cfg in enumerate(self.env.config.clusters):
                    if cluster_cfg.name == pipeline['data_location']:
                        # Check se ha risorse sufficienti
                        cluster = self.env.clusters_state[cluster_cfg.name]
                        if (cluster['cpu_available'] >= pipeline['cpu_required'] and
                            cluster['memory_available'] >= pipeline['memory_required']):
                            return idx
        
        # RULE 2: Heavy pipeline → cloud cluster
        if pipeline['cpu_required'] >= self.light_threshold:
            for idx, cluster_cfg in enumerate(self.env.config.clusters):
                if cluster_cfg.cluster_type == "cloud":
                    cluster = self.env.clusters_state[cluster_cfg.name]
                    if (cluster['cpu_available'] >= pipeline['cpu_required'] and
                        cluster['memory_available'] >= pipeline['memory_required']):
                        return idx
        
        # RULE 3: Fallback → most available
        return self._get_most_available_cluster(pipeline)
    
    def _get_most_available_cluster(self, pipeline: Dict) -> int:
        """Trova cluster con più risorse disponibili"""
        max_available = -1
        best_idx = 0
        
        for idx, cluster_cfg in enumerate(self.env.config.clusters):
            cluster = self.env.clusters_state[cluster_cfg.name]
            
            # Check risorse sufficienti
            if (cluster['cpu_available'] >= pipeline['cpu_required'] and
                cluster['memory_available'] >= pipeline['memory_required']):
                
                # Score basato su disponibilità
                cpu_score = cluster['cpu_available'] / cluster['cpu_capacity']
                mem_score = cluster['memory_available'] / cluster['memory_capacity']
                total_score = (cpu_score + mem_score) / 2.0
                
                if total_score > max_available:
                    max_available = total_score
                    best_idx = idx
        
        return best_idx


class ExpertDataset(Dataset):
    """
    Dataset di dimostrazioni expert per supervised learning.
    
    Contiene coppie (observation, action) raccolte dall'expert.
    """
    
    def __init__(self, observations: np.ndarray, actions: np.ndarray):
        self.observations = torch.FloatTensor(observations)
        self.actions = torch.LongTensor(actions)
    
    def __len__(self):
        return len(self.observations)
    
    def __getitem__(self, idx):
        return self.observations[idx], self.actions[idx]


def collect_expert_demonstrations(
    num_episodes: int = 500,
    difficulty: str = "easy",
    seed: int = 42
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Raccoglie dimostrazioni dall'expert policy.
    
    Args:
        num_episodes: Numero di episodi da raccogliere
        difficulty: Difficulty level
        seed: Random seed
    
    Returns:
        (observations, actions) arrays
    """
    print("\n" + "="*60)
    print("COLLECTING EXPERT DEMONSTRATIONS")
    print("="*60)
    print(f"Episodes: {num_episodes}")
    print(f"Difficulty: {difficulty}")
    print("="*60)
    
    config = get_config_for_difficulty(difficulty)
    config.master_seed = seed
    
    env = CloudContinuumEnv(config=config, seed=seed)
    expert = ExpertPolicy(env)
    
    observations = []
    actions = []
    episode_rewards = []
    success_rates = []
    
    print("\nCollecting demonstrations...")
    for episode in tqdm(range(num_episodes)):
        obs, info = env.reset()
        episode_reward = 0
        done = False
        
        while not done:
            # Expert decide action
            action = expert.get_action(obs)
            
            # Store demonstration
            observations.append(obs)
            actions.append(action)
            
            # Execute action
            obs, reward, terminated, truncated, info = env.step(action)
            episode_reward += reward
            done = terminated or truncated
        
        episode_rewards.append(episode_reward)
        success_rates.append(info['success_rate'])
    
    # Convert to numpy
    observations = np.array(observations)
    actions = np.array(actions)
    
    # Statistics
    mean_reward = np.mean(episode_rewards)
    mean_success = np.mean(success_rates)
    
    print("\n" + "-"*60)
    print("COLLECTION STATISTICS")
    print("-"*60)
    print(f"Total demonstrations: {len(observations):,}")
    print(f"Mean episode reward: {mean_reward:.2f}")
    print(f"Mean success rate: {mean_success*100:.1f}%")
    print("="*60)
    
    # Filter: keep only successful episodes (success_rate >= 80%)
    print("\nFiltering demonstrations (keeping success_rate >= 80%)...")
    
    episode_lengths = [info['pipeline_idx'] for info in [env.reset()[1]]]  # Get typical length
    
    # Ricostruisci demonstrations per episodio e filtra
    filtered_obs = []
    filtered_actions = []
    
    idx = 0
    for ep_success in success_rates:
        ep_length = min(config.pipelines_per_episode, len(observations) - idx)
        
        if ep_success >= 0.8:
            # Keep this episode
            filtered_obs.extend(observations[idx:idx+ep_length])
            filtered_actions.extend(actions[idx:idx+ep_length])
        
        idx += ep_length
    
    filtered_obs = np.array(filtered_obs)
    filtered_actions = np.array(filtered_actions)
    
    print(f"Filtered demonstrations: {len(filtered_obs):,} (kept {len(filtered_obs)/len(observations)*100:.1f}%)")
    
    return filtered_obs, filtered_actions


def pretrain_with_behavioral_cloning(
    observations: np.ndarray,
    actions: np.ndarray,
    model: PPO,
    num_epochs: int = 30,
    batch_size: int = 128,
    learning_rate: float = 1e-3
) -> PPO:
    """
    Pre-addestra policy usando behavioral cloning (supervised learning).
    """
    print("\n" + "="*60)
    print("BEHAVIORAL CLONING PRE-TRAINING")
    print("="*60)
    print(f"Demonstrations: {len(observations):,}")
    
    # Create dataset and dataloader
    dataset = ExpertDataset(observations, actions)
    dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)
    
    # Get policy network
    policy = model.policy
    optimizer = torch.optim.Adam(policy.parameters(), lr=learning_rate)
    criterion = nn.CrossEntropyLoss()
    
    print("\nTraining...")
    for epoch in range(num_epochs):
        total_loss = 0.0
        total_accuracy = 0.0
        num_batches = 0
        
        for batch_obs, batch_actions in dataloader:
            # Forward pass
            optimizer.zero_grad()
            
            # =================================================================
            # FIX CRITICO: Passaggio corretto attraverso MLP extractor
            # =================================================================
            # 1. Extract features (Flatten)
            features = policy.extract_features(batch_obs)
            
            # 2. Pass through MLP body (shared net or pi net)
            # mlp_extractor ritorna (latent_policy, latent_value)
            latent_pi, _ = policy.mlp_extractor(features)
            
            # 3. Action logits from latent representation
            action_logits = policy.action_net(latent_pi)
            
            # Compute loss
            loss = criterion(action_logits, batch_actions)
            
            # Backward pass
            loss.backward()
            optimizer.step()
            
            # Statistics
            total_loss += loss.item()
            
            # Accuracy
            predictions = torch.argmax(action_logits, dim=1)
            accuracy = (predictions == batch_actions).float().mean().item()
            total_accuracy += accuracy
            
            num_batches += 1
        
        # Epoch statistics
        avg_loss = total_loss / num_batches
        avg_accuracy = total_accuracy / num_batches
        
        if (epoch + 1) % 5 == 0:
            print(f"  Epoch {epoch+1}/{num_epochs}: Loss={avg_loss:.4f}, Accuracy={avg_accuracy*100:.1f}%")
    
    print("\n✅ Pre-training completed!")
    print("="*60)
    
    return model


def evaluate_pretrained_model(
    model: PPO,
    num_episodes: int = 50,
    difficulty: str = "easy"
) -> Dict:
    """
    Valuta modello pre-addestrato.
    
    Returns:
        Dict con metriche di evaluation
    """
    print("\n" + "="*60)
    print("EVALUATING PRE-TRAINED MODEL")
    print("="*60)
    print(f"Episodes: {num_episodes}")
    print(f"Difficulty: {difficulty}")
    print("="*60)
    
    config = get_config_for_difficulty(difficulty)
    env = CloudContinuumEnv(config=config, seed=9999)
    
    episode_rewards = []
    success_rates = []
    
    print("\nRunning evaluation...")
    for ep in tqdm(range(num_episodes)):
        obs, info = env.reset()
        episode_reward = 0
        done = False
        
        while not done:
            action, _states = model.predict(obs, deterministic=True)
            obs, reward, terminated, truncated, info = env.step(action)
            episode_reward += reward
            done = terminated or truncated
        
        episode_rewards.append(episode_reward)
        success_rates.append(info['success_rate'])
    
    # Statistics
    mean_reward = np.mean(episode_rewards)
    std_reward = np.std(episode_rewards)
    mean_success = np.mean(success_rates)
    std_success = np.std(success_rates)
    
    print("\n" + "-"*60)
    print("EVALUATION RESULTS")
    print("-"*60)
    print(f"Mean Reward: {mean_reward:.2f} ± {std_reward:.2f}")
    print(f"Success Rate: {mean_success*100:.1f}% ± {std_success*100:.1f}%")
    print("="*60)
    
    return {
        'mean_reward': mean_reward,
        'std_reward': std_reward,
        'mean_success_rate': mean_success,
        'std_success_rate': std_success
    }


def main():
    """Main pre-training pipeline"""
    parser = argparse.ArgumentParser(description="Pre-train CloudContinuum RL Agent")
    parser.add_argument("--num-demonstrations", type=int, default=500,
                        help="Number of expert demonstration episodes")
    parser.add_argument("--difficulty", type=str, default="easy",
                        choices=["easy", "medium", "hard"],
                        help="Difficulty for demonstrations")
    parser.add_argument("--bc-epochs", type=int, default=30,
                        help="Behavioral cloning epochs")
    parser.add_argument("--batch-size", type=int, default=128,
                        help="Batch size for BC")
    parser.add_argument("--save-path", type=str, default="./models/pretrained",
                        help="Path to save pre-trained model")
    parser.add_argument("--eval-episodes", type=int, default=50,
                        help="Episodes for evaluation")
    
    args = parser.parse_args()
    
    # Create save directory
    os.makedirs(args.save_path, exist_ok=True)
    
    # STEP 1: Collect expert demonstrations
    print("\n🔍 STEP 1/4: Collecting expert demonstrations...")
    observations, actions = collect_expert_demonstrations(
        num_episodes=args.num_demonstrations,
        difficulty=args.difficulty,
        seed=42
    )
    
    # STEP 2: Initialize PPO model
    print("\n🤖 STEP 2/4: Initializing PPO model...")
    config = get_config_for_difficulty(args.difficulty)
    
    def make_env():
        return CloudContinuumEnv(config=config, seed=42)
    
    env = DummyVecEnv([make_env])
    
    model = PPO(
        "MlpPolicy",
        env,
        verbose=0,
        learning_rate=3e-4,
        n_steps=2048,
        batch_size=64,
        policy_kwargs={"net_arch": [256, 256, 128]}
    )
    
    print("✅ Model initialized")
    
    # STEP 3: Pre-train with behavioral cloning
    print("\n📚 STEP 3/4: Pre-training with behavioral cloning...")
    model = pretrain_with_behavioral_cloning(
        observations=observations,
        actions=actions,
        model=model,
        num_epochs=args.bc_epochs,
        batch_size=args.batch_size
    )
    
    # STEP 4: Evaluate pre-trained model
    print("\n📊 STEP 4/4: Evaluating pre-trained model...")
    eval_results = evaluate_pretrained_model(
        model=model,
        num_episodes=args.eval_episodes,
        difficulty=args.difficulty
    )
    
    # Save model
    save_path = f"{args.save_path}/ppo_pretrained.zip"
    model.save(save_path)
    print(f"\n💾 Pre-trained model saved to: {save_path}")
    
    # Final summary
    print("\n" + "="*60)
    print("PRE-TRAINING PIPELINE COMPLETED ✅")
    print("="*60)
    print(f"Model path: {save_path}")
    print(f"Expected success rate: {eval_results['mean_success_rate']*100:.1f}%")
    print(f"Mean reward: {eval_results['mean_reward']:.2f}")
    print("\nNext step: Run train.py with --pretrained-model flag")
    print("="*60)


if __name__ == "__main__":
    main()