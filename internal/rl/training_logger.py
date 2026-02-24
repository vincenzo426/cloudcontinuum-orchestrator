# internal/rl/training_logger.py
"""Training Data Logger - Saves all transitions to CSV during training."""

import os
import csv
import numpy as np
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

from stable_baselines3.common.callbacks import BaseCallback


class TrainingDataLogger(BaseCallback):
    """
    Callback that logs all training transitions to CSV.
    
    Saves for each step:
      - Episode/step identifiers
      - Full observation vector (33 features)
      - Action taken
      - Reward received
      - Done flag
      - Action mask
      - Info dict fields (exec_time, target_cluster, etc.)
    
    Usage:
        logger = TrainingDataLogger(output_dir="./training_data")
        model.learn(total_timesteps=100000, callback=logger)
    """
    
    # Feature names for observation vector (33 features)
    OBSERVATION_FEATURES = [
        # Per-cluster features (5 × 4 = 20)
        "cloud_cpu_avail", "cloud_mem_avail", "cloud_is_data_local", "cloud_can_fit", "cloud_is_cloud",
        "edge1_cpu_avail", "edge1_mem_avail", "edge1_is_data_local", "edge1_can_fit", "edge1_is_cloud",
        "edge2_cpu_avail", "edge2_mem_avail", "edge2_is_data_local", "edge2_can_fit", "edge2_is_cloud",
        "edge3_cpu_avail", "edge3_mem_avail", "edge3_is_data_local", "edge3_can_fit", "edge3_is_cloud",
        # Pipeline features (5)
        "pipeline_cpu_ratio", "pipeline_mem_ratio", "pipeline_size_value", "pipeline_fits_edge", "pipeline_data_size_ratio",
        # Latency features (4)
        "latency_to_cloud", "latency_to_edge1", "latency_to_edge2", "latency_to_edge3",
        # Global features (4)
        "global_cloud_headroom", "global_edge_avg_util", "global_util_variance", "global_remaining_ratio",
    ]
    
    CLUSTER_NAMES = ["cloud_cluster", "edge_cluster_1", "edge_cluster_2", "edge_cluster_3"]
    
    def __init__(
        self,
        output_dir: str = "./training_data",
        filename_prefix: str = "training_data",
        save_frequency: int = 10000,
        include_action_probs: bool = False,
        verbose: int = 1
    ):
        """
        Initialize training data logger.
        
        Args:
            output_dir: Directory to save CSV files
            filename_prefix: Prefix for output filenames
            save_frequency: Flush to disk every N steps
            include_action_probs: Whether to include action probabilities (slower)
            verbose: Verbosity level
        """
        super().__init__(verbose)
        
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.filename_prefix = filename_prefix
        self.save_frequency = save_frequency
        self.include_action_probs = include_action_probs
        
        # Buffer for batch writing
        self.buffer: List[Dict[str, Any]] = []
        self.total_steps_logged = 0
        self.current_episode = 0
        
        # File handles
        self._csv_file = None
        self._csv_writer = None
        self._header_written = False
        
        # Current file path
        self.current_filepath: Optional[Path] = None
    
    def _init_callback(self) -> None:
        """Initialize callback when training starts."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.current_filepath = self.output_dir / f"{self.filename_prefix}_{timestamp}.csv"
        
        if self.verbose > 0:
            print(f"[TrainingDataLogger] Saving to: {self.current_filepath}")
    
    def _get_csv_header(self) -> List[str]:
        """Generate CSV header."""
        header = [
            "timestamp",
            "episode",
            "step",
            "global_step",
        ]
        
        # Observation features
        header.extend(self.OBSERVATION_FEATURES)
        
        # Action and reward
        header.extend([
            "action",
            "action_name",
            "reward",
            "done",
            "truncated",
        ])
        
        # Action mask
        header.extend([f"mask_{name}" for name in self.CLUSTER_NAMES])
        
        # Action probabilities (optional)
        if self.include_action_probs:
            header.extend([f"prob_{name}" for name in self.CLUSTER_NAMES])
        
        # Info fields
        header.extend([
            "placement_result",
            "exec_time",
            "target_cluster",
            "latency_ms",
            "is_edge_to_edge",
            "success_rate",
            "locality_rate",
        ])
        
        return header
    
    def _on_step(self) -> bool:
        """Called at each training step."""
        # Get data from locals
        obs = self.locals.get("obs_tensor")
        if obs is None:
            obs = self.locals.get("new_obs")
        
        actions = self.locals.get("actions")
        rewards = self.locals.get("rewards")
        dones = self.locals.get("dones")
        infos = self.locals.get("infos", [{}])
        
        # Handle vectorized environments
        if obs is not None:
            if hasattr(obs, 'cpu'):
                obs = obs.cpu().numpy()
            if len(obs.shape) > 1:
                obs = obs[0]  # Take first env
        
        if actions is not None:
            action = int(actions[0]) if hasattr(actions, '__len__') else int(actions)
        else:
            action = -1
        
        reward = float(rewards[0]) if rewards is not None and len(rewards) > 0 else 0.0
        done = bool(dones[0]) if dones is not None and len(dones) > 0 else False
        info = infos[0] if infos else {}
        
        # Get action mask from environment
        action_mask = np.ones(4, dtype=bool)
        if hasattr(self.training_env, 'env_method'):
            try:
                masks = self.training_env.env_method('action_masks')
                if masks and len(masks) > 0:
                    action_mask = np.array(masks[0], dtype=bool)
            except Exception:
                pass
        
        # Get action probabilities (optional)
        action_probs = None
        if self.include_action_probs and self.model is not None:
            try:
                obs_tensor = self.model.policy.obs_to_tensor(obs.reshape(1, -1))[0]
                distribution = self.model.policy.get_distribution(obs_tensor)
                action_probs = distribution.distribution.probs.detach().cpu().numpy()[0]
            except Exception:
                action_probs = np.zeros(4)
        
        # Track episodes
        if done:
            self.current_episode += 1
        
        # Build row
        row = self._build_row(
            obs=obs,
            action=action,
            reward=reward,
            done=done,
            truncated=info.get('TimeLimit.truncated', False),
            action_mask=action_mask,
            action_probs=action_probs,
            info=info
        )
        
        self.buffer.append(row)
        self.total_steps_logged += 1
        
        # Flush periodically
        if len(self.buffer) >= self.save_frequency:
            self._flush_buffer()
        
        return True
    
    def _build_row(
        self,
        obs: np.ndarray,
        action: int,
        reward: float,
        done: bool,
        truncated: bool,
        action_mask: np.ndarray,
        action_probs: Optional[np.ndarray],
        info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build a single CSV row."""
        row = {
            "timestamp": datetime.now().isoformat(),
            "episode": self.current_episode,
            "step": info.get('step', self.num_timesteps),
            "global_step": self.num_timesteps,
        }
        
        # Observation features
        if obs is not None and len(obs) >= len(self.OBSERVATION_FEATURES):
            for i, name in enumerate(self.OBSERVATION_FEATURES):
                row[name] = float(obs[i])
        else:
            for name in self.OBSERVATION_FEATURES:
                row[name] = 0.0
        
        # Action and reward
        row["action"] = action
        row["action_name"] = self.CLUSTER_NAMES[action] if 0 <= action < len(self.CLUSTER_NAMES) else "unknown"
        row["reward"] = reward
        row["done"] = done
        row["truncated"] = truncated
        
        # Action mask
        for i, name in enumerate(self.CLUSTER_NAMES):
            row[f"mask_{name}"] = bool(action_mask[i]) if i < len(action_mask) else True
        
        # Action probabilities
        if self.include_action_probs:
            for i, name in enumerate(self.CLUSTER_NAMES):
                row[f"prob_{name}"] = float(action_probs[i]) if action_probs is not None and i < len(action_probs) else 0.25
        
        # Info fields
        row["placement_result"] = info.get('placement_result', '')
        row["exec_time"] = info.get('exec_time', 0.0)
        row["target_cluster"] = info.get('target_cluster', '')
        row["latency_ms"] = info.get('latency_ms', 0.0)
        row["is_edge_to_edge"] = info.get('is_edge_to_edge', False)
        row["success_rate"] = info.get('success_rate', 0.0)
        row["locality_rate"] = info.get('locality_rate', 0.0)
        
        return row
    
    def _flush_buffer(self) -> None:
        """Write buffer to CSV file."""
        if not self.buffer:
            return
        
        # Open file if needed
        if self._csv_file is None:
            self._csv_file = open(self.current_filepath, 'w', newline='', encoding='utf-8')
            self._csv_writer = csv.DictWriter(self._csv_file, fieldnames=self._get_csv_header())
            self._csv_writer.writeheader()
            self._header_written = True
        
        # Write rows
        for row in self.buffer:
            self._csv_writer.writerow(row)
        
        self._csv_file.flush()
        
        if self.verbose > 0:
            print(f"[TrainingDataLogger] Flushed {len(self.buffer)} rows (total: {self.total_steps_logged})")
        
        self.buffer.clear()
    
    def _on_training_end(self) -> None:
        """Called when training ends."""
        self._flush_buffer()
        
        if self._csv_file is not None:
            self._csv_file.close()
            self._csv_file = None
        
        if self.verbose > 0:
            print(f"[TrainingDataLogger] Training complete. Total rows: {self.total_steps_logged}")
            print(f"[TrainingDataLogger] Output: {self.current_filepath}")
    
    def _on_rollout_end(self) -> None:
        """Called at the end of a rollout."""
        # Optionally flush at rollout end
        if len(self.buffer) > self.save_frequency // 2:
            self._flush_buffer()


class EpisodeDataLogger(BaseCallback):
    """
    Simpler callback that logs episode-level summaries to CSV.
    
    More lightweight than TrainingDataLogger, useful for quick analysis.
    """
    
    def __init__(
        self,
        output_dir: str = "./training_data",
        filename_prefix: str = "episodes",
        verbose: int = 1
    ):
        super().__init__(verbose)
        
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.filepath = self.output_dir / f"{filename_prefix}_{timestamp}.csv"
        
        self.episodes: List[Dict] = []
        self.current_episode_rewards: List[float] = []
        self.current_episode_start = 0
    
    def _on_step(self) -> bool:
        rewards = self.locals.get("rewards", [0.0])
        dones = self.locals.get("dones", [False])
        infos = self.locals.get("infos", [{}])
        
        self.current_episode_rewards.append(float(rewards[0]))
        
        if dones[0]:
            info = infos[0]
            episode_data = {
                "episode": len(self.episodes),
                "timestep": self.num_timesteps,
                "length": len(self.current_episode_rewards),
                "total_reward": sum(self.current_episode_rewards),
                "mean_reward": np.mean(self.current_episode_rewards),
                "success_rate": info.get('success_rate', 0.0),
                "locality_rate": info.get('locality_rate', 0.0),
                "avg_exec_time": info.get('avg_exec_time', 0.0),
                "placements_successful": info.get('placements_successful', 0),
                "placements_failed": info.get('placements_failed', 0),
            }
            self.episodes.append(episode_data)
            self.current_episode_rewards = []
        
        return True
    
    def _on_training_end(self) -> None:
        if self.episodes:
            with open(self.filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.episodes[0].keys())
                writer.writeheader()
                writer.writerows(self.episodes)
            
            if self.verbose > 0:
                print(f"[EpisodeDataLogger] Saved {len(self.episodes)} episodes to {self.filepath}")