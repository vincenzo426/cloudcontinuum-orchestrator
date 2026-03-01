# internal/rl/training_logger.py
"""Training Data Logger - Optimized with streaming and sampling to minimize RAM usage."""

import os
import csv
import gzip
import numpy as np
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

from stable_baselines3.common.callbacks import BaseCallback


class TrainingDataLogger(BaseCallback):
    """
    Callback that logs training transitions to CSV with minimal RAM usage.
    
    Optimizations:
      - Streaming: writes directly to disk without accumulating all data in RAM
      - Sampling: only logs a percentage of steps (default 10%)
      - Small buffer: flushes frequently to keep memory footprint low
      - Optional gzip compression: reduces disk space
    
    Usage:
        logger = TrainingDataLogger(
            output_dir="./training_data",
            sampling_rate=0.10,  # Log 10% of steps
            buffer_size=500,     # Flush every 500 sampled rows
        )
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
        sampling_rate: float = 0.10,
        buffer_size: int = 500,
        compress: bool = False,
        include_action_probs: bool = False,
        verbose: int = 1
    ):
        """
        Initialize optimized training data logger.
        
        Args:
            output_dir: Directory to save CSV files
            filename_prefix: Prefix for output filenames
            sampling_rate: Fraction of steps to log (0.0-1.0). Default 0.10 = 10%
            buffer_size: Number of rows to buffer before flushing to disk
            compress: If True, write gzip compressed CSV (.csv.gz)
            include_action_probs: Whether to include action probabilities
            verbose: Verbosity level
        """
        super().__init__(verbose)
        
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.filename_prefix = filename_prefix
        self.sampling_rate = np.clip(sampling_rate, 0.01, 1.0)
        self.buffer_size = buffer_size
        self.compress = compress
        self.include_action_probs = include_action_probs
        
        # Streaming buffer (small, fixed size)
        self._buffer: List[List[Any]] = []
        self._header: List[str] = []
        
        # File handle for streaming writes
        self._file = None
        self._csv_writer = None
        
        # Stats
        self.total_steps_seen = 0
        self.total_steps_logged = 0
        self.current_episode = 0
        
        # Random generator for sampling
        self._rng = np.random.default_rng()
        
        # File path
        self.current_filepath: Optional[Path] = None
    
    def _init_callback(self) -> None:
        """Initialize callback when training starts."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        ext = ".csv.gz" if self.compress else ".csv"
        self.current_filepath = self.output_dir / f"{self.filename_prefix}_{timestamp}{ext}"
        
        # Build header
        self._header = self._build_header()
        
        # Open file for streaming writes
        if self.compress:
            self._file = gzip.open(self.current_filepath, 'wt', newline='', encoding='utf-8')
        else:
            self._file = open(self.current_filepath, 'w', newline='', encoding='utf-8')
        
        self._csv_writer = csv.writer(self._file)
        self._csv_writer.writerow(self._header)
        self._file.flush()
        
        if self.verbose > 0:
            print(f"[TrainingDataLogger] Output: {self.current_filepath}")
            print(f"[TrainingDataLogger] Sampling rate: {self.sampling_rate*100:.0f}%")
            print(f"[TrainingDataLogger] Buffer size: {self.buffer_size}")
    
    def _build_header(self) -> List[str]:
        """Generate CSV header."""
        header = ["episode", "global_step"]
        header.extend(self.OBSERVATION_FEATURES)
        header.extend(["action", "reward", "done"])
        header.extend([f"mask_{i}" for i in range(4)])
        
        if self.include_action_probs:
            header.extend([f"prob_{i}" for i in range(4)])
        
        header.extend(["placement_result", "exec_time", "latency_ms", "success_rate"])
        
        return header
    
    def _on_step(self) -> bool:
        """Called at each training step - samples and logs data."""
        self.total_steps_seen += 1
        
        # Sampling: skip most steps to reduce data volume
        if self._rng.random() > self.sampling_rate:
            # Still track episode boundaries even when not logging
            dones = self.locals.get("dones", [False])
            if dones and dones[0]:
                self.current_episode += 1
            return True
        
        # Extract data from training locals
        row = self._extract_step_data()
        if row is not None:
            self._buffer.append(row)
            self.total_steps_logged += 1
        
        # Track episode boundaries
        dones = self.locals.get("dones", [False])
        if dones and dones[0]:
            self.current_episode += 1
        
        # Flush buffer when full
        if len(self._buffer) >= self.buffer_size:
            self._flush_buffer()
        
        return True
    
    def _extract_step_data(self) -> Optional[List[Any]]:
        """Extract and format data for current step. Returns row as list."""
        try:
            # Get observation
            obs = self.locals.get("obs_tensor")
            if obs is None:
                obs = self.locals.get("new_obs")
            
            if obs is not None:
                if hasattr(obs, 'cpu'):
                    obs = obs.cpu().numpy()
                if len(obs.shape) > 1:
                    obs = obs[0]
            else:
                return None
            
            # Get action, reward, done
            actions = self.locals.get("actions")
            rewards = self.locals.get("rewards", [0.0])
            dones = self.locals.get("dones", [False])
            infos = self.locals.get("infos", [{}])
            
            action = int(actions[0]) if actions is not None and hasattr(actions, '__len__') else -1
            reward = float(rewards[0]) if rewards is not None and len(rewards) > 0 else 0.0
            done = bool(dones[0]) if dones is not None and len(dones) > 0 else False
            info = infos[0] if infos else {}
            
            # Build row (as list for efficiency)
            row = [self.current_episode, self.num_timesteps]
            
            # Observation features (33 values)
            if len(obs) >= len(self.OBSERVATION_FEATURES):
                row.extend([round(float(obs[i]), 4) for i in range(len(self.OBSERVATION_FEATURES))])
            else:
                row.extend([0.0] * len(self.OBSERVATION_FEATURES))
            
            # Action, reward, done
            row.extend([action, round(reward, 4), int(done)])
            
            # Action mask
            action_mask = [1, 1, 1, 1]  # Default all valid
            if hasattr(self.training_env, 'env_method'):
                try:
                    masks = self.training_env.env_method('action_masks')
                    if masks and len(masks) > 0:
                        action_mask = [int(m) for m in masks[0][:4]]
                except Exception:
                    pass
            row.extend(action_mask)
            
            # Action probabilities (optional)
            if self.include_action_probs:
                probs = [0.25, 0.25, 0.25, 0.25]
                if self.model is not None:
                    try:
                        obs_tensor = self.model.policy.obs_to_tensor(obs.reshape(1, -1))[0]
                        dist = self.model.policy.get_distribution(obs_tensor)
                        probs = dist.distribution.probs.detach().cpu().numpy()[0].tolist()
                    except Exception:
                        pass
                row.extend([round(p, 4) for p in probs[:4]])
            
            # Info fields
            row.append(info.get('placement_result', ''))
            row.append(round(info.get('exec_time', 0.0), 2))
            row.append(round(info.get('latency_ms', 0.0), 2))
            row.append(round(info.get('success_rate', 0.0), 4))
            
            return row
            
        except Exception as e:
            if self.verbose > 0:
                print(f"[TrainingDataLogger] Error extracting step data: {e}")
            return None
    
    def _flush_buffer(self) -> None:
        """Write buffer to disk and clear it."""
        if not self._buffer or self._csv_writer is None:
            return
        
        self._csv_writer.writerows(self._buffer)
        self._file.flush()
        
        if self.verbose > 1:
            print(f"[TrainingDataLogger] Flushed {len(self._buffer)} rows")
        
        self._buffer.clear()
    
    def _on_rollout_end(self) -> None:
        """Flush at end of each rollout to prevent data loss."""
        if len(self._buffer) > 0:
            self._flush_buffer()
    
    def _on_training_end(self) -> None:
        """Final flush and cleanup."""
        self._flush_buffer()
        
        if self._file is not None:
            self._file.close()
            self._file = None
            self._csv_writer = None
        
        if self.verbose > 0:
            print(f"[TrainingDataLogger] Training complete.")
            print(f"[TrainingDataLogger] Steps seen: {self.total_steps_seen:,}")
            print(f"[TrainingDataLogger] Steps logged: {self.total_steps_logged:,} ({self.total_steps_logged/max(1,self.total_steps_seen)*100:.1f}%)")
            print(f"[TrainingDataLogger] Output: {self.current_filepath}")


class EpisodeDataLogger(BaseCallback):
    """
    Lightweight callback that logs only episode-level summaries.
    
    Much more memory efficient than step-level logging.
    Writes directly to disk at end of each episode.
    """
    
    def __init__(
        self,
        output_dir: str = "./training_data",
        filename_prefix: str = "episodes",
        buffer_size: int = 100,
        verbose: int = 1
    ):
        super().__init__(verbose)
        
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.buffer_size = buffer_size
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.filepath = self.output_dir / f"{filename_prefix}_{timestamp}.csv"
        
        # Streaming
        self._file = None
        self._csv_writer = None
        self._buffer: List[List[Any]] = []
        self._header_written = False
        
        # Episode tracking
        self.current_episode_rewards: List[float] = []
        self.total_episodes = 0
    
    def _init_callback(self) -> None:
        """Open file for streaming writes."""
        self._file = open(self.filepath, 'w', newline='', encoding='utf-8')
        self._csv_writer = csv.writer(self._file)
        
        # Write header
        header = [
            "episode", "timestep", "length", "total_reward", "mean_reward",
            "success_rate", "locality_rate", "avg_exec_time",
            "placements_successful", "placements_failed"
        ]
        self._csv_writer.writerow(header)
        self._file.flush()
        
        if self.verbose > 0:
            print(f"[EpisodeDataLogger] Output: {self.filepath}")
    
    def _on_step(self) -> bool:
        rewards = self.locals.get("rewards", [0.0])
        dones = self.locals.get("dones", [False])
        infos = self.locals.get("infos", [{}])
        
        if rewards:
            self.current_episode_rewards.append(float(rewards[0]))
        
        if dones and dones[0]:
            info = infos[0] if infos else {}
            
            row = [
                self.total_episodes,
                self.num_timesteps,
                len(self.current_episode_rewards),
                round(sum(self.current_episode_rewards), 4),
                round(np.mean(self.current_episode_rewards) if self.current_episode_rewards else 0.0, 4),
                round(info.get('success_rate', 0.0), 4),
                round(info.get('locality_rate', 0.0), 4),
                round(info.get('avg_exec_time', 0.0), 2),
                info.get('placements_successful', 0),
                info.get('placements_failed', 0),
            ]
            
            self._buffer.append(row)
            self.total_episodes += 1
            self.current_episode_rewards = []
            
            # Flush periodically
            if len(self._buffer) >= self.buffer_size:
                self._flush_buffer()
        
        return True
    
    def _flush_buffer(self) -> None:
        """Write buffer to disk."""
        if self._buffer and self._csv_writer:
            self._csv_writer.writerows(self._buffer)
            self._file.flush()
            self._buffer.clear()
    
    def _on_training_end(self) -> None:
        self._flush_buffer()
        
        if self._file:
            self._file.close()
            self._file = None
        
        if self.verbose > 0:
            print(f"[EpisodeDataLogger] Saved {self.total_episodes} episodes to {self.filepath}")