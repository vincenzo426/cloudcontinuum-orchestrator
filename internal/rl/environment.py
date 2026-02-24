# internal/rl/environment.py
"""CloudContinuum Gymnasium Environment for RL-based pipeline placement."""

import gymnasium as gym
from gymnasium import spaces
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
import random
from copy import deepcopy

from .config import (
    EnvironmentConfig, DEFAULT_CONFIG, ClusterConfig,
    PIPELINE_TEMPLATES, PipelineSizeCategory, get_config_for_difficulty
)
from .simulator import (
    ExecutionTimeSimulator, NetworkLatencyModel,
    SimulatedClusterState, ResourceContentionModel
)


class CloudContinuumEnv(gym.Env):
    """
    Gymnasium environment for intelligent ML pipeline placement.
    
    Observation Space (33 features):
      - Per-cluster (5 × 4 = 20): cpu_avail, mem_avail, is_data_local, can_fit, is_cloud
      - Pipeline (5): cpu_ratio, mem_ratio, size_value, fits_edge, data_size_ratio
      - Latency (4): normalized latency from data_location to each cluster
      - Global (4): cloud_headroom, edge_avg_util, util_variance, remaining_ratio
    
    The agent learns to:
    - Minimize execution time
    - Avoid myopic decisions (don't waste cloud on small pipelines)
    - Balance load across clusters
    - Prefer data locality when beneficial
    """
    
    metadata = {"render_modes": ["human", "ansi"]}
    
    def __init__(self, config: EnvironmentConfig = None, seed: int = None, render_mode: str = None):
        super().__init__()
        
        self.config = config or DEFAULT_CONFIG
        self.render_mode = render_mode
        
        if seed is not None:
            self.seed(seed)
        
        # Simulators
        self.exec_simulator = ExecutionTimeSimulator(
            time_per_cpu_core=self.config.exec_time_per_cpu_core,
            latency_impact_factor=self.config.latency_impact_factor,
            contention_impact_factor=self.config.contention_impact_factor,
            baseline_time=self.config.baseline_execution_time
        )
        self.network_model = NetworkLatencyModel(self.config.clusters, seed=seed)
        
        # Spaces
        self.action_space = spaces.Discrete(self.config.num_clusters)
        self.observation_space = spaces.Box(
            low=-1.0, high=2.0,
            shape=(self.config.total_state_size,),
            dtype=np.float32
        )
        
        # State
        self.clusters_state: Dict[str, SimulatedClusterState] = {}
        self.pipelines_queue: List[Dict] = []
        self.current_pipeline_idx: int = 0
        self.current_step: int = 0
        self.episode_stats: Dict = {}
        
        # Temporal state
        self.current_time: float = 0.0
        self.active_jobs: List[Dict] = []
        
        # Current pipeline cache
        self._current_pipeline: Optional[Dict] = None
        self._current_action_mask: Optional[np.ndarray] = None
    
    def seed(self, seed: int = None):
        """Set random seed."""
        if seed is not None:
            random.seed(seed)
            np.random.seed(seed)
            self.config.master_seed = seed
        return [seed]
    
    # === RESET ===
    
    def reset(self, seed: int = None, options: Dict = None) -> Tuple[np.ndarray, Dict]:
        """Reset environment for new episode."""
        super().reset(seed=seed)
        if seed is not None:
            self.seed(seed)
        
        self.current_step = 0
        self.current_pipeline_idx = 0
        self.current_time = 0.0
        self.active_jobs = []
        
        self.episode_stats = {
            'placements_successful': 0, 'placements_failed': 0,
            'placements_failed_contention': 0, 'total_reward': 0.0,
            'execution_times': [], 'ideal_execution_times': [],
            'data_locality_hits': 0, 'small_on_cloud': 0, 'large_on_cloud': 0,
            'cluster_placements': {c.name: 0 for c in self.config.clusters},
            'resources_released': 0,
            'pipeline_categories': {'small': 0, 'medium': 0, 'large': 0},
            'pipeline_categories_placed': {'small': 0, 'medium': 0, 'large': 0},
        }
        
        self.clusters_state = self._initialize_clusters()
        self.pipelines_queue = self._generate_pipeline_queue()
        self._current_pipeline = self.pipelines_queue[0] if self.pipelines_queue else None
        
        return self._get_observation(), self._get_info()
    
    def _initialize_clusters(self) -> Dict[str, SimulatedClusterState]:
        """Initialize cluster states with curriculum variation."""
        states = {}
        variation = self.config.baseline_load_variation
        
        for cluster in self.config.clusters:
            cpu_adj = int(cluster.baseline_cpu_requested * (1 + variation))
            mem_adj = int(cluster.baseline_memory_requested * (1 + variation))
            cpu_adj = min(max(0, cpu_adj), int(cluster.cpu_capacity * 0.95))
            mem_adj = min(max(0, mem_adj), int(cluster.memory_capacity * 0.95))
            
            states[cluster.name] = SimulatedClusterState(
                name=cluster.name, cluster_type=cluster.cluster_type,
                cpu_capacity=cluster.cpu_capacity, memory_capacity=cluster.memory_capacity,
                cpu_used=cpu_adj, memory_used=mem_adj,
                placements_count=0, latency_to_cloud=cluster.latency_to_cloud
            )
        return states
    
    def _generate_pipeline_queue(self) -> List[Dict]:
        """Generate pipeline queue for episode."""
        queue = []
        weights = np.cumsum([t['weight'] for t in PIPELINE_TEMPLATES])
        edge_names = [c.name for c in self.config.clusters if c.cluster_type == "edge"]
        all_names = [c.name for c in self.config.clusters]
        mult = self.config.pipeline_size_multiplier
        
        for i in range(self.config.pipelines_per_episode):
            idx = np.searchsorted(weights, random.random() * weights[-1])
            template = PIPELINE_TEMPLATES[idx]
            
            cpu = int(random.uniform(template['cpu_range'][0] * mult, template['cpu_range'][1] * mult))
            memory = int(random.uniform(template['memory_range'][0] * mult, template['memory_range'][1] * mult))
            
            # Generate data size from template range
            data_size_range = template.get('data_size_range', (100*1024*1024, 500*1024*1024))
            data_size = int(random.uniform(data_size_range[0] * mult, data_size_range[1] * mult))
            
            # Data location
            if random.random() < self.config.data_locality_probability:
                data_location = random.choice(edge_names) if edge_names else "cloud_cluster"
            else:
                data_location = "distributed" if random.random() < 0.3 else random.choice(all_names)
            
            size_cat, size_val = PipelineSizeCategory.classify(cpu, memory)
            self.episode_stats['pipeline_categories'][size_cat] += 1
            
            queue.append({
                'id': f"pipeline_{i}", 'name': f"{template['name']}_{i}",
                'cpu_required': cpu, 'memory_required': memory,
                'data_size': data_size,
                'data_location': data_location, 'size_category': size_cat,
                'size_value': size_val, 'template': template['name'],
            })
        return queue
    
    # === STEP ===
    
    def step(self, action: int) -> Tuple[np.ndarray, float, bool, bool, Dict]:
        """Execute placement action."""
        self.current_step += 1
        time_elapsed = self._simulate_time_advance()
        self._update_active_jobs()
        
        # Check termination
        if self._current_pipeline is None or self.current_pipeline_idx >= len(self.pipelines_queue):
            return self._get_observation(), 0.0, True, False, self._get_info()
        
        pipeline = self._current_pipeline
        cluster_name = self.config.clusters[action].name
        cluster_state = self.clusters_state[cluster_name]
        
        # Validate action
        action_mask = self.action_masks()
        if not action_mask[action]:
            return self._handle_failure('invalid_action', self.config.reward_invalid_action, time_elapsed)
        
        # Check stochastic contention failure
        if self.config.enable_stochastic_failure:
            if cluster_state.cpu_utilization > self.config.contention_activation_threshold or \
               cluster_state.memory_utilization > self.config.contention_activation_threshold:
                failure_prob = ResourceContentionModel.get_failure_probability(
                    cluster_state.cpu_utilization, cluster_state.memory_utilization,
                    pipeline['cpu_required'] / cluster_state.cpu_capacity,
                    pipeline['memory_required'] / cluster_state.memory_capacity
                )
                if np.random.random() < failure_prob:
                    self.episode_stats['placements_failed_contention'] += 1
                    return self._handle_failure('contention_failure', self.config.reward_placement_failed, time_elapsed)
        
        # Execute placement
        if not cluster_state.allocate(pipeline['cpu_required'], pipeline['memory_required']):
            return self._handle_failure('failed', self.config.reward_placement_failed, time_elapsed)
        
        # Calculate execution time
        is_data_local = self._is_data_local(pipeline['data_location'], cluster_name)
        latency = 0.0 if is_data_local else self.network_model.get_latency(pipeline['data_location'], cluster_name)
        
        # Check if transfer is edge-to-edge (faster bandwidth with direct connection)
        is_edge_to_edge = False
        if not is_data_local:
            src_type = self._get_cluster_type(pipeline['data_location'])
            dst_type = cluster_state.cluster_type
            is_edge_to_edge = (src_type == "edge" and dst_type == "edge")
        
        exec_time = self.exec_simulator.simulate(
            pipeline['cpu_required'], pipeline['memory_required'],
            cluster_state.cpu_utilization, cluster_state.memory_utilization,
            latency, is_data_local,
            data_size_bytes=pipeline.get('data_size', 0),
            is_edge_to_edge=is_edge_to_edge
        )
        ideal_exec_time = self.exec_simulator.simulate(
            pipeline['cpu_required'], pipeline['memory_required'],
            0.3, 0.3, 0.0, True,
            data_size_bytes=0
        )
        
        self._add_active_job(pipeline, cluster_name, exec_time)
        
        # Calculate reward
        reward = self._calculate_reward(pipeline, cluster_state, exec_time, ideal_exec_time, is_data_local)
        
        # Update stats
        self._update_success_stats(pipeline, cluster_name, cluster_state, exec_time, ideal_exec_time, is_data_local, reward)
        
        self._advance_to_next_pipeline()
        
        terminated = self.current_pipeline_idx >= len(self.pipelines_queue)
        truncated = self.current_step >= self.config.max_episode_steps
        
        if terminated:
            reward += self._calculate_episode_bonus()
        
        info = self._get_info()
        info.update({'placement_result': 'success', 'exec_time': exec_time,
                     'target_cluster': cluster_name, 'time_elapsed': time_elapsed,
                     'latency_ms': latency, 'is_edge_to_edge': is_edge_to_edge})
        
        return self._get_observation(), reward, terminated, truncated, info
    
    def _handle_failure(self, reason: str, reward: float, time_elapsed: float):
        """Handle placement failure."""
        self.episode_stats['placements_failed'] += 1
        self.episode_stats['total_reward'] += reward
        self._advance_to_next_pipeline()
        
        terminated = self.current_pipeline_idx >= len(self.pipelines_queue)
        truncated = self.current_step >= self.config.max_episode_steps
        info = self._get_info()
        info.update({'placement_result': reason, 'time_elapsed': time_elapsed})
        
        return self._get_observation(), reward, terminated, truncated, info
    
    def _update_success_stats(self, pipeline, cluster_name, cluster_state, exec_time, ideal_exec_time, is_data_local, reward):
        """Update episode statistics for successful placement."""
        self.episode_stats['placements_successful'] += 1
        self.episode_stats['total_reward'] += reward
        self.episode_stats['execution_times'].append(exec_time)
        self.episode_stats['ideal_execution_times'].append(ideal_exec_time)
        self.episode_stats['cluster_placements'][cluster_name] += 1
        
        if is_data_local:
            self.episode_stats['data_locality_hits'] += 1
        
        if cluster_state.cluster_type == "cloud":
            if pipeline['size_category'] == PipelineSizeCategory.SMALL:
                self.episode_stats['small_on_cloud'] += 1
            elif pipeline['size_category'] == PipelineSizeCategory.LARGE:
                self.episode_stats['large_on_cloud'] += 1
        
        self.episode_stats['pipeline_categories_placed'][pipeline['size_category']] += 1
    
    def _advance_to_next_pipeline(self):
        """Advance to next pipeline in queue."""
        self.current_pipeline_idx += 1
        self._current_pipeline = self.pipelines_queue[self.current_pipeline_idx] \
            if self.current_pipeline_idx < len(self.pipelines_queue) else None
        self._current_action_mask = None
    
    def _simulate_time_advance(self) -> float:
        """Simulate time passing (Poisson process)."""
        inter_arrival = np.random.exponential(self.config.avg_inter_arrival_time)
        self.current_time += inter_arrival
        return inter_arrival
    
    def _update_active_jobs(self):
        """Release resources for completed jobs."""
        if not self.config.enable_resource_release:
            return
        
        remaining = []
        for job in self.active_jobs:
            if self.current_time >= job['finish_time']:
                self.clusters_state[job['cluster_name']].release(job['cpu'], job['memory'])
                self.episode_stats['resources_released'] += 1
            else:
                remaining.append(job)
        
        if len(remaining) != len(self.active_jobs):
            self._current_action_mask = None
        self.active_jobs = remaining
    
    def _add_active_job(self, pipeline: Dict, cluster_name: str, exec_time: float):
        """Add job to active jobs list."""
        if self.config.enable_resource_release:
            self.active_jobs.append({
                'pipeline_id': pipeline['id'], 'cluster_name': cluster_name,
                'cpu': pipeline['cpu_required'], 'memory': pipeline['memory_required'],
                'start_time': self.current_time, 'finish_time': self.current_time + exec_time,
            })
    
    def _is_data_local(self, data_location: str, cluster_name: str) -> bool:
        """Check if data is local to cluster."""
        return data_location in (cluster_name, "none") and data_location != "distributed"
    
    def _get_cluster_type(self, cluster_name: str) -> str:
        """Get cluster type by name. Returns 'unknown' for distributed/invalid."""
        for c in self.config.clusters:
            if c.name == cluster_name:
                return c.cluster_type
        return "unknown"
    
    # === REWARD ===
    
    def _calculate_reward(self, pipeline: Dict, cluster_state: SimulatedClusterState,
                          exec_time: float, ideal_exec_time: float, is_data_local: bool) -> float:
        """Calculate reward for successful placement."""
        reward = 0.0
        
        # Execution time (normalized to ideal)
        time_ratio = exec_time / max(ideal_exec_time, 1.0)
        reward += self.config.reward_time_weight * (2.0 - min(time_ratio, 3.0))
        
        # Data locality
        reward += self.config.reward_data_locality if is_data_local else self.config.penalty_remote_placement
        
        # Strategic placement (anti-myopic)
        size = pipeline['size_category']
        if cluster_state.cluster_type == "cloud":
            if size == PipelineSizeCategory.SMALL:
                reward += self.config.penalty_small_on_cloud
            elif size == PipelineSizeCategory.LARGE:
                reward += self.config.bonus_large_on_cloud
        else:
            if size == PipelineSizeCategory.SMALL:
                reward += self.config.bonus_small_on_edge
            elif size == PipelineSizeCategory.MEDIUM:
                reward += self.config.bonus_medium_on_edge
        
        # Balancing
        avg_util = (cluster_state.cpu_utilization + cluster_state.memory_utilization) / 2.0
        if self.config.utilization_optimal_min <= avg_util <= self.config.utilization_optimal_max:
            reward += self.config.reward_balanced_utilization
        elif avg_util > self.config.utilization_danger:
            reward += self.config.penalty_near_saturation
        
        return reward
    
    def _calculate_episode_bonus(self) -> float:
        """Calculate end-of-episode bonus/penalty."""
        if not self.pipelines_queue:
            return 0.0
        success_rate = self.episode_stats['placements_successful'] / len(self.pipelines_queue)
        if success_rate == 1.0:
            return self.config.reward_perfect_episode
        return self.episode_stats['placements_failed'] * self.config.penalty_per_failure
    
    # === OBSERVATION ===
    
    def _get_observation(self) -> np.ndarray:
        """
        Build observation vector (33 features).
        
        Structure:
          [0-19]  Per-cluster features (5 × 4)
          [20-24] Pipeline features (5)
          [25-28] Latency vector (4) - normalized latency from data_location to each cluster
          [29-32] Global features (4)
        """
        obs = []
        
        # === Per-cluster features (5 × 4 = 20) ===
        for cfg in self.config.clusters:
            cluster = self.clusters_state[cfg.name]
            obs.append(cluster.cpu_available / cluster.cpu_capacity)
            obs.append(cluster.memory_available / cluster.memory_capacity)
            
            if self._current_pipeline:
                obs.append(float(self._is_data_local(self._current_pipeline['data_location'], cluster.name)))
                obs.append(float(cluster.can_fit(self._current_pipeline['cpu_required'],
                                                  self._current_pipeline['memory_required'])))
            else:
                obs.extend([0.0, 0.0])
            obs.append(float(cluster.cluster_type == "cloud"))
        
        # === Pipeline features (5) ===
        if self._current_pipeline:
            p = self._current_pipeline
            obs.append(min(p['cpu_required'] / self.config.max_cpu_available, 2.0))
            obs.append(min(p['memory_required'] / self.config.max_memory_available, 2.0))
            obs.append(p['size_value'])
            obs.append(float(any(
                self.clusters_state[c.name].can_fit(p['cpu_required'], p['memory_required'])
                for c in self.config.clusters if c.cluster_type == "edge"
            )))
            data_size_ratio = p.get('data_size', 0) / self.config.max_data_size
            obs.append(min(data_size_ratio, 2.0))
        else:
            obs.extend([0.0, 0.0, 0.0, 0.0, 0.0])
        
        # === Latency vector (4) - normalized latency from data_location to each cluster ===
        if self._current_pipeline:
            data_loc = self._current_pipeline['data_location']
            if data_loc in ("none", "distributed"):
                # Distributed data: zero latency (will be averaged at transfer)
                latencies = [0.0] * self.config.num_clusters
            else:
                # Get normalized latencies from data_location to each cluster
                latencies = self.network_model.get_normalized_latency_vector(
                    data_loc, 
                    self.config.cluster_names,
                    max_latency=self.config.max_latency
                ).tolist()
            obs.extend(latencies)
        else:
            obs.extend([0.0] * self.config.num_clusters)
        
        # === Global features (4) ===
        cloud = self.clusters_state.get("cloud_cluster")
        obs.append(cloud.cpu_available / cloud.cpu_capacity if cloud else 0.0)
        
        edge_utils = [(self.clusters_state[c.name].cpu_utilization +
                       self.clusters_state[c.name].memory_utilization) / 2.0
                      for c in self.config.clusters if c.cluster_type == "edge"]
        obs.append(np.mean(edge_utils) if edge_utils else 0.0)
        
        all_utils = [(self.clusters_state[c.name].cpu_utilization +
                      self.clusters_state[c.name].memory_utilization) / 2.0
                     for c in self.config.clusters]
        obs.append(min(np.var(all_utils) * 10, 1.0) if all_utils else 0.0)
        
        remaining = (len(self.pipelines_queue) - self.current_pipeline_idx) / len(self.pipelines_queue) \
            if self.pipelines_queue else 0.0
        obs.append(remaining)
        
        return np.array(obs, dtype=np.float32)
    
    # === ACTION MASKING ===
    
    def action_masks(self) -> np.ndarray:
        """Return action mask for MaskablePPO."""
        if self._current_action_mask is not None:
            return self._current_action_mask
        
        mask = np.zeros(self.config.num_clusters, dtype=bool)
        if self._current_pipeline is None:
            return mask
        
        p = self._current_pipeline
        for i, cfg in enumerate(self.config.clusters):
            if self.clusters_state[cfg.name].can_fit(p['cpu_required'], p['memory_required']):
                mask[i] = True
        
        if not mask.any():
            mask[:] = True  # Force selection even if all fail
        
        self._current_action_mask = mask
        return mask
    
    def get_action_mask(self) -> np.ndarray:
        """Alias for action_masks()."""
        return self.action_masks()
    
    # === INFO & RENDER ===
    
    def _get_info(self) -> Dict[str, Any]:
        """Return episode statistics."""
        total = len(self.pipelines_queue)
        successful = self.episode_stats['placements_successful']
        
        return {
            'step': self.current_step,
            'pipeline_idx': self.current_pipeline_idx,
            'total_pipelines': total,
            'success_rate': successful / total if total else 0.0,
            'locality_rate': self.episode_stats['data_locality_hits'] / max(1, successful),
            'avg_exec_time': np.mean(self.episode_stats['execution_times']) if self.episode_stats['execution_times'] else 0.0,
            'avg_efficiency': np.mean([i/a for a, i in zip(self.episode_stats['execution_times'],
                                                           self.episode_stats['ideal_execution_times'])
                                       if a > 0]) if self.episode_stats['execution_times'] else 0.0,
            'total_reward': self.episode_stats['total_reward'],
            'placements_successful': successful,
            'placements_failed': self.episode_stats['placements_failed'],
            'placements_failed_contention': self.episode_stats['placements_failed_contention'],
            'small_on_cloud': self.episode_stats['small_on_cloud'],
            'large_on_cloud': self.episode_stats['large_on_cloud'],
            'cluster_placements': self.episode_stats['cluster_placements'].copy(),
            'resources_released': self.episode_stats['resources_released'],
            'active_jobs': len(self.active_jobs),
            'current_time': self.current_time,
            'pipeline_categories': self.episode_stats['pipeline_categories'].copy(),
            'pipeline_categories_placed': self.episode_stats['pipeline_categories_placed'].copy(),
        }
    
    def render(self):
        """Render environment state."""
        if self.render_mode == "human":
            self._render_human()
        elif self.render_mode == "ansi":
            return self._render_ansi()
    
    def _render_human(self):
        """Human-readable output."""
        print(f"\n{'='*60}")
        print(f"Step: {self.current_step} | Pipeline: {self.current_pipeline_idx+1}/{len(self.pipelines_queue)}")
        for cfg in self.config.clusters:
            c = self.clusters_state[cfg.name]
            print(f"  {c.name:15s} | CPU: {c.cpu_utilization*100:5.1f}% | MEM: {c.memory_utilization*100:5.1f}%")
        if self._current_pipeline:
            p = self._current_pipeline
            data_mb = p.get('data_size', 0) / (1024 * 1024)
            print(f"  Pipeline: {p['name']} | {p['cpu_required']}m | {p['size_category']} | Data: {data_mb:.0f}MB")
        print(f"{'='*60}")
    
    def _render_ansi(self) -> str:
        """ANSI string for logging."""
        info = self._get_info()
        return f"Step {self.current_step}: success={info['success_rate']*100:.0f}%"
    
    def close(self):
        """Cleanup."""
        pass