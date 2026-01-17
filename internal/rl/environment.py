# internal/rl/environment.py
"""
CloudContinuum Gymnasium Environment
VERSIONE 2.0 - Con Action Masking e Reward Shaping Avanzato

Features v2.0:
- Action masking nativo per MaskablePPO
- State representation potenziata (9 feature/cluster)
- Reward shaping aggressivo per >95% success rate
- Tracking dettagliato fallimenti
"""

import gymnasium as gym
from gymnasium import spaces
import numpy as np
from typing import Dict, List, Tuple, Optional
import random

from .config import EnvironmentConfig, DEFAULT_CONFIG, ClusterConfig, PIPELINE_TEMPLATES
from .simulator import ExecutionTimeSimulator, NetworkLatencyModel


class CloudContinuumEnv(gym.Env):
    """
    Gymnasium Environment per placement di pipeline ML su cluster distribuiti.
    
    State Space: Vector di float con info su:
        - Per-cluster features (9 per cluster)
        - Global features (6 totali)
        - Temporal features (5 totali)
    
    Action Space: Discrete(num_clusters) - selezione del cluster
    
    Reward:
        - Successo: +100 base + bonus data locality/balance
        - Fallimento: -500 (aggressivo)
        - Invalid action: -300
    """
    
    metadata = {"render_modes": ["human"]}
    
    def __init__(self, config: EnvironmentConfig = None, seed: int = None):
        super().__init__()
        
        self.config = config or DEFAULT_CONFIG
        if seed is not None:
            self.seed(seed)
        
        # Initialize simulators
        self.exec_time_sim = ExecutionTimeSimulator(
            base_time=self.config.baseline_execution_time
        )
        self.network_latency_model = NetworkLatencyModel()
        
        # Define action and observation space
        self.action_space = spaces.Discrete(self.config.num_clusters)
        
        # State space: continuous features
        self.observation_space = spaces.Box(
            low=-10.0,
            high=10.0,
            shape=(self.config.total_state_size,),
            dtype=np.float32
        )
        
        # Internal state
        self.current_step = 0
        self.current_pipeline_idx = 0
        self.pipelines_queue = []
        self.clusters_state = {}
        self.episode_stats = {}
        
        # Action mask (per MaskablePPO)
        self._current_action_mask = None
    
    def seed(self, seed=None):
        """Set random seed"""
        if seed is not None:
            random.seed(seed)
            np.random.seed(seed)
            self.config.master_seed = seed
    
    def reset(self, seed=None, options=None):
        """Reset environment per nuovo episodio"""
        super().reset(seed=seed)
        
        self.current_step = 0
        self.current_pipeline_idx = 0
        
        # Initialize clusters state from config
        self.clusters_state = self._initialize_clusters()
        
        # Generate pipeline queue per questo episodio
        self.pipelines_queue = self._generate_pipeline_queue()
        
        # Initialize episode statistics
        self.episode_stats = {
            'placements_successful': 0,
            'placements_failed': 0,
            'total_reward': 0.0,
            'execution_times': [],
            'failure_reasons': [],
            'cluster_placements_count': {c.name: 0 for c in self.config.clusters}
        }
        
        # Get initial observation
        obs = self._get_observation()
        info = self._get_info()
        
        return obs, info
    
    def _initialize_clusters(self) -> Dict:
        """
        Inizializza stato cluster con baseline realistico.
        
        Returns:
            Dict con stato di ogni cluster
        """
        clusters = {}
        
        for cluster_config in self.config.clusters:
            clusters[cluster_config.name] = {
                'name': cluster_config.name,
                'cpu_capacity': cluster_config.cpu_capacity,
                'memory_capacity': cluster_config.memory_capacity,
                'cpu_used': cluster_config.baseline_cpu_used,
                'memory_used': cluster_config.baseline_memory_used,
                'cpu_available': cluster_config.cpu_capacity - cluster_config.baseline_cpu_used,
                'memory_available': cluster_config.memory_capacity - cluster_config.baseline_memory_used,
                'placements_count': 0,  # Count placement in questo episodio
                'cluster_type': cluster_config.cluster_type
            }
        
        return clusters
    
    def _generate_pipeline_queue(self) -> List[Dict]:
        """
        Genera coda di pipeline per questo episodio.
        
        Usa template realistici con distribuzione bilanciata.
        """
        num_pipelines = self.config.pipelines_per_episode
        difficulty_params = self.config.get_difficulty_params()
        
        pipelines = []
        
        for i in range(num_pipelines):
            # Seleziona template basato su probabilità
            template_name = self._select_template_weighted()
            template = PIPELINE_TEMPLATES[template_name]
            
            # Genera risorse richieste
            cpu_min, cpu_max = template['cpu_range']
            mem_min, mem_max = template['memory_range']
            
            pipeline = {
                'id': f"pipeline_{i}",
                'cpu_required': random.randint(cpu_min, cpu_max),
                'memory_required': random.randint(int(mem_min * 1e9), int(mem_max * 1e9)),
                'template': template_name,
                'data_location': self._assign_data_location(difficulty_params)
            }
            
            pipelines.append(pipeline)
        
        return pipelines
    
    def _select_template_weighted(self) -> str:
        """Seleziona template usando probabilità definite"""
        rand = random.random()
        cumulative = 0.0
        
        for template_name, template_config in PIPELINE_TEMPLATES.items():
            cumulative += template_config['probability']
            if rand <= cumulative:
                return template_name
        
        return "medium"  # Fallback
    
    def _assign_data_location(self, difficulty_params: Dict) -> str:
        """
        Assegna data location basato su probabilità di locality.
        
        Higher difficulty = meno locality
        """
        locality_prob = difficulty_params.get('data_locality_probability', 0.5)
        
        if random.random() < locality_prob:
            # Assegna località a un cluster edge (più realistico)
            edge_clusters = [c.name for c in self.config.clusters if c.cluster_type == "edge"]
            return random.choice(edge_clusters) if edge_clusters else "cloud_cluster"
        else:
            # Nessuna località specifica
            return "none"
    
    def step(self, action: int) -> Tuple[np.ndarray, float, bool, bool, Dict]:
        """
        Esegui azione di placement.
        
        Args:
            action: Index del cluster dove fare placement
        
        Returns:
            observation, reward, terminated, truncated, info
        """
        self.current_step += 1
        
        # Get current pipeline
        if self.current_pipeline_idx >= len(self.pipelines_queue):
            # Episode completato
            terminated = True
            truncated = False
            reward = self._calculate_episode_end_bonus()
            obs = self._get_observation()
            info = self._get_info()
            return obs, reward, terminated, truncated, info
        
        pipeline = self.pipelines_queue[self.current_pipeline_idx]
        cluster_name = self.config.clusters[action].name
        
        # CRITICO: Verifica action masking
        action_mask = self._get_valid_actions_mask(pipeline)
        if not action_mask[action]:
            # INVALID ACTION - azione su cluster saturo
            reward = self.config.penalty_invalid_action
            self.episode_stats['placements_failed'] += 1
            self.episode_stats['failure_reasons'].append('invalid_action')
            
            # Move to next pipeline comunque
            self.current_pipeline_idx += 1
            
            terminated = self.current_pipeline_idx >= len(self.pipelines_queue)
            truncated = self.current_step >= self.config.max_episode_steps
            
            obs = self._get_observation()
            info = self._get_info()
            info['placement_failed'] = True
            info['failure_reason'] = 'invalid_action'
            
            return obs, reward, terminated, truncated, info
        
        # Tenta placement
        placement_success, reward, failure_reason = self._try_place_pipeline(
            pipeline, cluster_name
        )
        
        # Update stats
        if placement_success:
            self.episode_stats['placements_successful'] += 1
            self.episode_stats['cluster_placements_count'][cluster_name] += 1
        else:
            self.episode_stats['placements_failed'] += 1
            self.episode_stats['failure_reasons'].append(failure_reason or 'unknown')
        
        self.episode_stats['total_reward'] += reward
        
        # Move to next pipeline
        self.current_pipeline_idx += 1
        
        # Check termination
        terminated = self.current_pipeline_idx >= len(self.pipelines_queue)
        truncated = self.current_step >= self.config.max_episode_steps
        
        # Get new observation
        obs = self._get_observation()
        info = self._get_info()
        info['placement_success'] = placement_success
        
        return obs, reward, terminated, truncated, info
    
    def _try_place_pipeline(
        self,
        pipeline: Dict,
        cluster_name: str
    ) -> Tuple[bool, float, Optional[str]]:
        """
        Tenta placement della pipeline su cluster specificato.
        
        Returns:
            (success, reward, failure_reason)
        """
        cluster = self.clusters_state[cluster_name]
        
        # Check risorse sufficienti
        if (cluster['cpu_available'] < pipeline['cpu_required'] or
            cluster['memory_available'] < pipeline['memory_required']):
            # FALLIMENTO - risorse insufficienti
            return False, self.config.penalty_failed_placement, 'insufficient_resources'
        
        # SUCCESSO - Placement riuscito
        # Update cluster state
        cluster['cpu_used'] += pipeline['cpu_required']
        cluster['memory_used'] += pipeline['memory_required']
        cluster['cpu_available'] -= pipeline['cpu_required']
        cluster['memory_available'] -= pipeline['memory_required']
        cluster['placements_count'] += 1
        
        # Simula execution time
        is_data_local = (pipeline['data_location'] == cluster_name or
                        pipeline['data_location'] == "none")
        
        if not is_data_local:
            network_latency = self.network_latency_model.get_latency(
                pipeline['data_location'], cluster_name
            )
        else:
            network_latency = 0.0
        
        exec_time = self.exec_time_sim.simulate(
            pipeline_cpu=pipeline['cpu_required'],
            pipeline_memory=pipeline['memory_required'],
            cluster_cpu_available=cluster['cpu_available'],
            cluster_memory_available=cluster['memory_available'],
            network_latency=network_latency,
            is_data_local=is_data_local
        )
        
        self.episode_stats['execution_times'].append(exec_time)
        
        # Calcola reward
        reward = self._calculate_reward(
            pipeline=pipeline,
            cluster=cluster,
            cluster_name=cluster_name,
            exec_time=exec_time,
            is_data_local=is_data_local
        )
        
        return True, reward, None
    
    def _calculate_reward(
        self,
        pipeline: Dict,
        cluster: Dict,
        cluster_name: str,
        exec_time: float,
        is_data_local: bool
    ) -> float:
        """
        Calcola reward per placement riuscito.
        
        COMPONENTI REWARD:
        1. Base success bonus
        2. Data locality bonus
        3. Balanced utilization bonus
        4. Cluster diversity bonus
        5. Penalty per overload/underutilization
        """
        reward = self.config.bonus_successful_placement  # Base: +100
        
        # 1. DATA LOCALITY BONUS
        if is_data_local:
            reward += self.config.bonus_data_locality  # +150
        else:
            reward += self.config.penalty_remote_placement  # -30
        
        # 2. BALANCED UTILIZATION BONUS
        cpu_util = cluster['cpu_used'] / cluster['cpu_capacity']
        mem_util = cluster['memory_used'] / cluster['memory_capacity']
        avg_util = (cpu_util + mem_util) / 2.0
        
        # Sweet spot: 40-75% utilization
        if self.config.target_utilization_min <= avg_util <= self.config.target_utilization_max:
            # Dentro range ottimale
            distance_from_ideal = abs(avg_util - self.config.target_utilization_ideal)
            balance_bonus = self.config.bonus_balanced_utilization * (1.0 - distance_from_ideal)
            reward += balance_bonus
        elif avg_util < self.config.target_utilization_min:
            # Sottoutilizzo
            reward += self.config.penalty_underutilization
        elif avg_util > 0.90:
            # Pericoloso - quasi saturo
            reward += self.config.penalty_overload
        
        # 3. CLUSTER DIVERSITY BONUS
        # Incentiva distribuzione su più cluster
        if cluster['placements_count'] == 1:
            # Primo placement su questo cluster = buono
            reward += self.config.bonus_new_cluster_usage  # +50
        elif cluster['placements_count'] > len(self.pipelines_queue) * 0.6:
            # Troppi placement su singolo cluster = male
            reward += self.config.penalty_cluster_monopoly  # -150
        
        return reward
    
    def _calculate_episode_end_bonus(self) -> float:
        """
        Bonus/penalty a fine episodio basato su performance globale.
        """
        if len(self.pipelines_queue) == 0:
            return 0.0
        
        success_rate = (self.episode_stats['placements_successful'] /
                       len(self.pipelines_queue))
        
        # PERFECT EPISODE BONUS
        if success_rate == 1.0:
            return self.config.bonus_perfect_episode  # +500
        
        # Penalità proporzionale ai fallimenti
        failure_rate = 1.0 - success_rate
        penalty = failure_rate * abs(self.config.penalty_failed_placement)
        
        return -penalty
    
    def _get_valid_actions_mask(self, pipeline: Dict) -> np.ndarray:
        """
        Genera action mask: True = azione valida, False = azione invalida.
        
        CRITICO per success rate: previene azioni su cluster saturi.
        
        Returns:
            Array booleano di shape (num_clusters,)
        """
        mask = np.ones(self.config.num_clusters, dtype=bool)
        
        for i, cluster_config in enumerate(self.config.clusters):
            cluster = self.clusters_state[cluster_config.name]
            
            # Verifica risorse sufficienti
            has_cpu = cluster['cpu_available'] >= pipeline['cpu_required']
            has_memory = cluster['memory_available'] >= pipeline['memory_required']
            
            if not (has_cpu and has_memory):
                mask[i] = False  # Maschera questa azione
        
        # Fallback safety: se TUTTE le azioni sono mascherate, abilita cloud
        if not mask.any():
            # Abilita cloud_cluster come fallback
            cloud_idx = next(i for i, c in enumerate(self.config.clusters)
                           if c.cluster_type == "cloud")
            mask[cloud_idx] = True
        
        return mask
    
    def get_action_mask(self) -> np.ndarray:
        """
        Public API per MaskablePPO.
        
        Returns:
            Current action mask
        """
        if self.current_pipeline_idx >= len(self.pipelines_queue):
            # Episode finito - ritorna mask tutto true
            return np.ones(self.config.num_clusters, dtype=bool)
        
        pipeline = self.pipelines_queue[self.current_pipeline_idx]
        return self._get_valid_actions_mask(pipeline)
    
    def _get_observation(self) -> np.ndarray:
        """
        Costruisce observation vector con feature engineering avanzato.
        
        STRUTTURA STATE (POTENZIATA v2.0):
        - Per ogni cluster (9 features):
            1. cpu_utilization (0-1)
            2. memory_utilization (0-1)
            3. cpu_available_normalized (0-1)
            4. memory_available_normalized (0-1)
            5. safety_margin (headroom)
            6. stress_level (quanto è carico)
            7. balance_score (equilibrio CPU-MEM)
            8. headroom (spazio per crescita)
            9. is_data_cluster (1 se contiene i dati)
        
        - Global features (6):
            1. pipeline_cpu_normalized
            2. pipeline_memory_normalized
            3. placement_difficulty (stimato)
            4. episode_progress (0-1)
            5. success_rate_so_far
            6. is_data_local_flag
        
        - Temporal features (5):
            1-4. Last 4 actions (one-hot encoded compresso)
            5. avg_exec_time_so_far
        
        Returns:
            State vector (numpy array)
        """
        state = []
        
        # Current pipeline (se disponibile)
        if self.current_pipeline_idx < len(self.pipelines_queue):
            pipeline = self.pipelines_queue[self.current_pipeline_idx]
        else:
            # Episode finito - usa valori dummy
            pipeline = {
                'cpu_required': 0,
                'memory_required': 0,
                'data_location': "none"
            }
        
        # === PER-CLUSTER FEATURES ===
        for cluster_config in self.config.clusters:
            cluster = self.clusters_state[cluster_config.name]
            
            # 1-2. Utilization
            cpu_util = cluster['cpu_used'] / max(cluster['cpu_capacity'], 1)
            mem_util = cluster['memory_used'] / max(cluster['memory_capacity'], 1)
            
            # 3-4. Available (normalized)
            cpu_avail_norm = cluster['cpu_available'] / max(cluster['cpu_capacity'], 1)
            mem_avail_norm = cluster['memory_available'] / max(cluster['memory_capacity'], 1)
            
            # 5. Safety margin (quanto margine prima di saturo)
            safety_margin = min(cpu_avail_norm, mem_avail_norm)
            
            # 6. Stress level (quanto è carico - combina utilization)
            stress_level = (cpu_util + mem_util) / 2.0
            
            # 7. Balance score (equilibrio tra CPU e MEM)
            balance_score = 1.0 - abs(cpu_util - mem_util)
            
            # 8. Headroom (capacità di assorbire workload futuri)
            headroom = (cpu_avail_norm + mem_avail_norm) / 2.0
            
            # 9. Is data cluster (pipeline corrente ha dati qui?)
            is_data_cluster = 1.0 if cluster['name'] == pipeline['data_location'] else 0.0
            
            state.extend([
                cpu_util, mem_util,
                cpu_avail_norm, mem_avail_norm,
                safety_margin, stress_level, balance_score,
                headroom, is_data_cluster
            ])
        
        # === GLOBAL FEATURES ===
        # Normalize pipeline requirements rispetto a capacità media
        avg_cpu_capacity = np.mean([c['cpu_capacity'] for c in self.clusters_state.values()])
        avg_mem_capacity = np.mean([c['memory_capacity'] for c in self.clusters_state.values()])
        
        pipeline_cpu_norm = pipeline['cpu_required'] / avg_cpu_capacity
        pipeline_mem_norm = pipeline['memory_required'] / avg_mem_capacity
        
        # Placement difficulty (quanto è difficile questo placement)
        placement_difficulty = max(pipeline_cpu_norm, pipeline_mem_norm)
        
        # Episode progress
        episode_progress = self.current_pipeline_idx / max(len(self.pipelines_queue), 1)
        
        # Success rate so far
        total_placements = self.episode_stats['placements_successful'] + self.episode_stats['placements_failed']
        success_rate = (self.episode_stats['placements_successful'] / total_placements
                       if total_placements > 0 else 0.0)
        
        # Data locality flag per pipeline corrente
        is_data_local = 1.0 if pipeline['data_location'] != "none" else 0.0
        
        state.extend([
            pipeline_cpu_norm,
            pipeline_mem_norm,
            placement_difficulty,
            episode_progress,
            success_rate,
            is_data_local
        ])
        
        # === TEMPORAL FEATURES ===
        # Media execution time fino ad ora
        avg_exec_time = (np.mean(self.episode_stats['execution_times'])
                        if self.episode_stats['execution_times'] else 0.0)
        avg_exec_time_norm = avg_exec_time / self.config.baseline_execution_time
        
        state.append(avg_exec_time_norm)
        
        # Padding per temporal features (4 slot riservati per history)
        state.extend([0.0] * 4)  # Placeholder per future features
        
        return np.array(state, dtype=np.float32)
    
    def _get_info(self) -> Dict:
        """Ritorna info dict con statistiche episodio"""
        if len(self.pipelines_queue) > 0:
            success_rate = (self.episode_stats['placements_successful'] /
                           len(self.pipelines_queue))
        else:
            success_rate = 0.0
        
        avg_exec_time = (np.mean(self.episode_stats['execution_times'])
                        if self.episode_stats['execution_times'] else 0.0)
        
        return {
            'step': self.current_step,
            'pipeline_idx': self.current_pipeline_idx,
            'success_rate': success_rate,
            'placements_successful': self.episode_stats['placements_successful'],
            'placements_failed': self.episode_stats['placements_failed'],
            'avg_exec_time': avg_exec_time,
            'total_reward': self.episode_stats['total_reward'],
            'cluster_placements': self.episode_stats['cluster_placements_count'],
            'failure_reasons': self.episode_stats.get('failure_reasons', [])
        }
    
    def render(self):
        """Render environment state (human readable)"""
        if self.render_mode == "human":
            print("\n" + "="*60)
            print(f"Step: {self.current_step} | Pipeline: {self.current_pipeline_idx}/{len(self.pipelines_queue)}")
            print("-"*60)
            
            for cluster_name, cluster in self.clusters_state.items():
                cpu_util = cluster['cpu_used'] / cluster['cpu_capacity'] * 100
                mem_util = cluster['memory_used'] / cluster['memory_capacity'] * 100
                print(f"{cluster_name:20s} | CPU: {cpu_util:5.1f}% | MEM: {mem_util:5.1f}% | "
                      f"Placements: {cluster['placements_count']}")
            
            print("-"*60)
            info = self._get_info()
            print(f"Success Rate: {info['success_rate']*100:.1f}% | "
                  f"Avg Exec Time: {info['avg_exec_time']:.2f}s")
            print("="*60)
    
    def close(self):
        """Cleanup"""
        pass


# ========== TESTING ==========
if __name__ == "__main__":
    print("Testing CloudContinuumEnv...")
    
    env = CloudContinuumEnv()
    
    print("\n1. Reset environment:")
    obs, info = env.reset(seed=42)
    print(f"  Observation shape: {obs.shape}")
    print(f"  Action space: {env.action_space}")
    print(f"  Pipelines in queue: {len(env.pipelines_queue)}")
    
    print("\n2. Testing action masking:")
    mask = env.get_action_mask()
    print(f"  Action mask: {mask}")
    print(f"  Valid actions: {np.where(mask)[0]}")
    
    print("\n3. Running 5 random steps with masking:")
    for i in range(5):
        mask = env.get_action_mask()
        valid_actions = np.where(mask)[0]
        
        if len(valid_actions) > 0:
            action = np.random.choice(valid_actions)
        else:
            action = 0  # Fallback
        
        obs, reward, terminated, truncated, info = env.step(action)
        print(f"  Step {i+1}: action={action}, reward={reward:.1f}, "
              f"success_rate={info['success_rate']*100:.1f}%")
        
        if terminated or truncated:
            break
    
    print("\n✅ Environment test completed!")