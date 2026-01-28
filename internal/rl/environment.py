# internal/rl/environment.py
"""
CloudContinuum Gymnasium Environment
VERSIONE 4.0 - ANTI-MYOPIC ARCHITECTURE

Environment per training di agente RL che piazza pipeline ML su cluster distribuiti.

OBIETTIVI (in ordine di priorità):
1. Minimizzare tempo di esecuzione
2. Evitare scelte miopi (non sprecare cloud con pipeline piccole)
3. Bilanciare carico tra cluster
4. Rispettare data locality quando possibile

STATE SPACE: 28 features
ACTION SPACE: Discrete(4) - scelta del cluster
REWARD: Range ~[-1, +1], orientato al tempo con penalità strategiche
"""

import gymnasium as gym
from gymnasium import spaces
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
import random
from copy import deepcopy

from .config import (
    EnvironmentConfig, 
    DEFAULT_CONFIG, 
    ClusterConfig,
    PIPELINE_TEMPLATES,
    PipelineSizeCategory,
    get_config_for_difficulty
)
from .simulator import (
    ExecutionTimeSimulator,
    NetworkLatencyModel,
    SimulatedClusterState,
    ResourceContentionModel
)


# =============================================================================
# GYMNASIUM ENVIRONMENT
# =============================================================================

class CloudContinuumEnv(gym.Env):
    """
    Gymnasium Environment per placement intelligente di pipeline ML.
    
    L'agente deve imparare a:
    - Piazzare pipeline dove il tempo di esecuzione è minimo
    - NON sprecare il cloud con pipeline piccole (anti-miopatia)
    - Bilanciare il carico tra i cluster
    - Preferire data locality quando non compromette gli altri obiettivi
    
    Attributes:
        config: Configurazione ambiente
        action_space: Discrete(num_clusters)
        observation_space: Box(28,) con features normalizzate
    """
    
    metadata = {"render_modes": ["human", "ansi"]}
    
    def __init__(
        self, 
        config: EnvironmentConfig = None, 
        seed: int = None,
        render_mode: str = None
    ):
        """
        Inizializza environment.
        
        Args:
            config: Configurazione (default: DEFAULT_CONFIG)
            seed: Random seed per riproducibilità
            render_mode: "human" per output leggibile
        """
        super().__init__()
        
        self.config = config or DEFAULT_CONFIG
        self.render_mode = render_mode
        
        if seed is not None:
            self.seed(seed)
        
        # === SIMULATORS ===
        self.exec_simulator = ExecutionTimeSimulator(
            time_per_cpu_core=self.config.exec_time_per_cpu_core,
            latency_impact_factor=self.config.latency_impact_factor,
            contention_impact_factor=self.config.contention_impact_factor,
            baseline_time=self.config.baseline_execution_time
        )
        self.network_model = NetworkLatencyModel(self.config.clusters)
        
        # === SPACES ===
        self.action_space = spaces.Discrete(self.config.num_clusters)
        
        # Observation: 28 features normalizzate in [-1, 1] o [0, 1]
        self.observation_space = spaces.Box(
            low=-1.0,
            high=2.0,  # Alcune features possono superare 1.0
            shape=(self.config.total_state_size,),
            dtype=np.float32
        )
        
        # === INTERNAL STATE ===
        self.clusters_state: Dict[str, SimulatedClusterState] = {}
        self.pipelines_queue: List[Dict] = []
        self.current_pipeline_idx: int = 0
        self.current_step: int = 0
        self.episode_stats: Dict = {}
        
        # === TEMPORAL STATE (Poisson Process & Resource Release) ===
        self.current_time: float = 0.0  # Tempo simulato corrente (secondi)
        self.active_jobs: List[Dict] = []  # Job in esecuzione con finish_time
        
        # Current pipeline (per action masking)
        self._current_pipeline: Optional[Dict] = None
        self._current_action_mask: Optional[np.ndarray] = None
    
    def seed(self, seed: int = None):
        """Imposta random seed."""
        if seed is not None:
            random.seed(seed)
            np.random.seed(seed)
            self.config.master_seed = seed
        return [seed]
    
    # =========================================================================
    # RESET
    # =========================================================================
    
    def reset(
        self, 
        seed: int = None, 
        options: Dict = None
    ) -> Tuple[np.ndarray, Dict]:
        """
        Reset environment per nuovo episodio.
        
        Returns:
            (observation, info)
        """
        super().reset(seed=seed)
        
        if seed is not None:
            self.seed(seed)
        
        # Reset counters
        self.current_step = 0
        self.current_pipeline_idx = 0
        
        # Reset temporal state (Poisson process & resource release)
        self.current_time = 0.0
        self.active_jobs = []
        
        # Reset episode statistics PRIMA di generare pipeline queue
        # (perché _generate_pipeline_queue usa episode_stats per contare le categorie)
        self.episode_stats = {
            'placements_successful': 0,
            'placements_failed': 0,
            'placements_failed_contention': 0,  # Fallimenti per contesa stocastica
            'total_reward': 0.0,
            'execution_times': [],
            'ideal_execution_times': [],  # Per calcolo efficienza
            'data_locality_hits': 0,
            'small_on_cloud': 0,
            'large_on_cloud': 0,
            'cluster_placements': {c.name: 0 for c in self.config.clusters},
            'resources_released': 0,  # Contatore job completati
            # Diagnostica: traccia quante pipeline per categoria
            'pipeline_categories': {'small': 0, 'medium': 0, 'large': 0},
            'pipeline_categories_placed': {'small': 0, 'medium': 0, 'large': 0},
        }
        
        # Initialize cluster states (con variazione da curriculum)
        self.clusters_state = self._initialize_clusters()
        
        # Generate pipeline queue per questo episodio
        self.pipelines_queue = self._generate_pipeline_queue()
        
        # Set current pipeline
        if len(self.pipelines_queue) > 0:
            self._current_pipeline = self.pipelines_queue[0]
        else:
            self._current_pipeline = None
        
        # Build observation
        obs = self._get_observation()
        info = self._get_info()
        
        return obs, info
    
    def _initialize_clusters(self) -> Dict[str, SimulatedClusterState]:
        """
        Inizializza stato cluster con variazione da curriculum.
        
        baseline_load_variation:
            -0.3 = cluster con 30% meno carico (più spazio)
            0.0 = carico reale
            +0.15 = cluster con 15% più carico (meno spazio)
        """
        states = {}
        variation = self.config.baseline_load_variation
        
        for cluster in self.config.clusters:
            # Applica variazione al baseline
            cpu_variation = int(cluster.baseline_cpu_requested * variation)
            mem_variation = int(cluster.baseline_memory_requested * variation)
            
            adjusted_cpu = max(0, cluster.baseline_cpu_requested + cpu_variation)
            adjusted_mem = max(0, cluster.baseline_memory_requested + mem_variation)
            
            # Assicura che non superi la capacità
            adjusted_cpu = min(adjusted_cpu, int(cluster.cpu_capacity * 0.95))
            adjusted_mem = min(adjusted_mem, int(cluster.memory_capacity * 0.95))
            
            states[cluster.name] = SimulatedClusterState(
                name=cluster.name,
                cluster_type=cluster.cluster_type,
                cpu_capacity=cluster.cpu_capacity,
                memory_capacity=cluster.memory_capacity,
                cpu_used=adjusted_cpu,
                memory_used=adjusted_mem,
                placements_count=0,
                latency_to_cloud=cluster.latency_to_cloud
            )
        
        return states
    
    def _generate_pipeline_queue(self) -> List[Dict]:
        """
        Genera coda di pipeline per l'episodio.
        
        Rispetta:
        - pipeline_size_multiplier: scala dimensioni
        - data_locality_probability: probabilità che dati siano su edge specifico
        """
        queue = []
        
        # Calcola pesi cumulativi per selezione template
        weights = [t['weight'] for t in PIPELINE_TEMPLATES]
        cumulative_weights = np.cumsum(weights)
        
        # Edge clusters per data locality
        edge_names = [c.name for c in self.config.clusters if c.cluster_type == "edge"]
        all_cluster_names = [c.name for c in self.config.clusters]
        
        for i in range(self.config.pipelines_per_episode):
            # Seleziona template
            r = random.random() * cumulative_weights[-1]
            template_idx = np.searchsorted(cumulative_weights, r)
            template = PIPELINE_TEMPLATES[template_idx]
            
            # Genera dimensioni con multiplier
            mult = self.config.pipeline_size_multiplier
            cpu_min, cpu_max = template['cpu_range']
            mem_min, mem_max = template['memory_range']
            
            cpu = int(random.uniform(cpu_min * mult, cpu_max * mult))
            memory = int(random.uniform(mem_min * mult, mem_max * mult))
            
            # Determina data location
            if random.random() < self.config.data_locality_probability:
                # Dati su un edge specifico
                data_location = random.choice(edge_names) if edge_names else "cloud_cluster"
            else:
                # Dati distribuiti (random cluster o "distributed")
                if random.random() < 0.3:
                    data_location = "distributed"  # Nessuna locality chiara
                else:
                    data_location = random.choice(all_cluster_names)
            
            # Classifica dimensione pipeline
            size_category, size_value = PipelineSizeCategory.classify(
                cpu, memory, self.config.clusters
            )
            
            pipeline = {
                'id': f"pipeline_{i}",
                'name': f"{template['name']}_{i}",
                'cpu_required': cpu,
                'memory_required': memory,
                'data_location': data_location,
                'size_category': size_category,
                'size_value': size_value,
                'template': template['name'],
            }
            
            queue.append(pipeline)
        
        # Conta le categorie generate (per diagnostica)
        for p in queue:
            cat = p['size_category']
            if cat in self.episode_stats['pipeline_categories']:
                self.episode_stats['pipeline_categories'][cat] += 1
        
        return queue
    
    # =========================================================================
    # STEP
    # =========================================================================
    
    def step(self, action: int) -> Tuple[np.ndarray, float, bool, bool, Dict]:
        """
        Esegue azione (placement su cluster).
        
        FLUSSO AGGIORNATO:
        1. Simula passaggio tempo (Poisson process)
        2. Rilascia risorse dei job completati
        3. Verifica action mask
        4. Verifica contesa stocastica (cluster molto carichi possono fallire)
        5. Esegue placement se tutto ok
        6. Calcola reward con normalizzazione dinamica
        
        Args:
            action: Index del cluster target
        
        Returns:
            (observation, reward, terminated, truncated, info)
        """
        self.current_step += 1
        
        # === 1. SIMULA PASSAGGIO TEMPO (Poisson Process) ===
        time_elapsed = self._simulate_time_advance()
        
        # === 2. RILASCIA RISORSE DEI JOB COMPLETATI ===
        self._update_active_jobs()
        
        # Check se episodio già terminato
        if self._current_pipeline is None or self.current_pipeline_idx >= len(self.pipelines_queue):
            obs = self._get_observation()
            info = self._get_info()
            return obs, 0.0, True, False, info
        
        pipeline = self._current_pipeline
        cluster_name = self.config.clusters[action].name
        cluster_state = self.clusters_state[cluster_name]
        
        # === 3. VERIFICA ACTION MASK ===
        action_mask = self.action_masks()
        if not action_mask[action]:
            # INVALID ACTION - cluster non può ospitare la pipeline
            reward = self.config.reward_invalid_action
            self.episode_stats['placements_failed'] += 1
            self.episode_stats['total_reward'] += reward
            
            # Avanza alla prossima pipeline
            self._advance_to_next_pipeline()
            
            terminated = self.current_pipeline_idx >= len(self.pipelines_queue)
            truncated = self.current_step >= self.config.max_episode_steps
            
            obs = self._get_observation()
            info = self._get_info()
            info['placement_result'] = 'invalid_action'
            info['time_elapsed'] = time_elapsed
            
            return obs, reward, terminated, truncated, info
        
        # === 4. VERIFICA CONTESA STOCASTICA ===
        # Un cluster molto carico può far fallire il placement stocasticamente
        if self.config.enable_stochastic_failure:
            cpu_util = cluster_state.cpu_utilization
            mem_util = cluster_state.memory_utilization
            
            if cpu_util > self.config.contention_activation_threshold or \
               mem_util > self.config.contention_activation_threshold:
                # Calcola probabilità di fallimento
                failure_prob = ResourceContentionModel.get_failure_probability(
                    cpu_utilization=cpu_util,
                    memory_utilization=mem_util,
                    pipeline_cpu_ratio=pipeline['cpu_required'] / cluster_state.cpu_capacity,
                    pipeline_memory_ratio=pipeline['memory_required'] / cluster_state.memory_capacity
                )
                
                # Lancia il dado
                if np.random.random() < failure_prob:
                    # FALLIMENTO STOCASTICO - contesa ha causato failure
                    reward = self.config.reward_placement_failed
                    self.episode_stats['placements_failed'] += 1
                    self.episode_stats['placements_failed_contention'] += 1
                    self.episode_stats['total_reward'] += reward
                    
                    self._advance_to_next_pipeline()
                    
                    terminated = self.current_pipeline_idx >= len(self.pipelines_queue)
                    truncated = self.current_step >= self.config.max_episode_steps
                    
                    obs = self._get_observation()
                    info = self._get_info()
                    info['placement_result'] = 'contention_failure'
                    info['failure_probability'] = failure_prob
                    info['time_elapsed'] = time_elapsed
                    
                    return obs, reward, terminated, truncated, info
        
        # === 5. ESEGUI PLACEMENT ===
        success = cluster_state.allocate(
            pipeline['cpu_required'],
            pipeline['memory_required']
        )
        
        if not success:
            # Fallimento imprevisto (non dovrebbe accadere con action mask)
            reward = self.config.reward_placement_failed
            self.episode_stats['placements_failed'] += 1
            self.episode_stats['total_reward'] += reward
            
            self._advance_to_next_pipeline()
            
            terminated = self.current_pipeline_idx >= len(self.pipelines_queue)
            truncated = self.current_step >= self.config.max_episode_steps
            
            obs = self._get_observation()
            info = self._get_info()
            info['placement_result'] = 'failed'
            info['time_elapsed'] = time_elapsed
            
            return obs, reward, terminated, truncated, info
        
        # === 6. PLACEMENT RIUSCITO ===
        
        # Calcola tempo di esecuzione
        is_data_local = self._is_data_local(pipeline['data_location'], cluster_name)
        latency = 0.0 if is_data_local else self.network_model.get_latency(
            pipeline['data_location'], cluster_name
        )
        
        exec_time = self.exec_simulator.simulate(
            pipeline_cpu=pipeline['cpu_required'],
            pipeline_memory=pipeline['memory_required'],
            cluster_cpu_utilization=cluster_state.cpu_utilization,
            cluster_memory_utilization=cluster_state.memory_utilization,
            network_latency_ms=latency,
            is_data_local=is_data_local
        )
        
        # Calcola tempo IDEALE per questa pipeline (per normalizzazione reward)
        ideal_exec_time = self.exec_simulator.simulate(
            pipeline_cpu=pipeline['cpu_required'],
            pipeline_memory=pipeline['memory_required'],
            cluster_cpu_utilization=0.3,  # Cluster scarico ideale
            cluster_memory_utilization=0.3,
            network_latency_ms=0.0,  # Dati locali
            is_data_local=True
        )
        
        # Aggiungi job alla lista attivi (per rilascio risorse futuro)
        self._add_active_job(pipeline, cluster_name, exec_time)
        
        # Calcola reward con normalizzazione dinamica
        reward = self._calculate_reward(
            pipeline=pipeline,
            cluster_state=cluster_state,
            exec_time=exec_time,
            ideal_exec_time=ideal_exec_time,
            is_data_local=is_data_local
        )
        
        # Aggiorna statistiche
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
        
        # Traccia categoria pipeline piazzata (diagnostica)
        cat = pipeline['size_category']
        if cat in self.episode_stats['pipeline_categories_placed']:
            self.episode_stats['pipeline_categories_placed'][cat] += 1
        
        # Avanza alla prossima pipeline
        self._advance_to_next_pipeline()
        
        # Check terminazione
        terminated = self.current_pipeline_idx >= len(self.pipelines_queue)
        truncated = self.current_step >= self.config.max_episode_steps
        
        # Bonus fine episodio
        if terminated:
            reward += self._calculate_episode_bonus()
        
        obs = self._get_observation()
        info = self._get_info()
        info['placement_result'] = 'success'
        info['exec_time'] = exec_time
        info['ideal_exec_time'] = ideal_exec_time
        info['is_data_local'] = is_data_local
        info['target_cluster'] = cluster_name
        info['time_elapsed'] = time_elapsed
        info['active_jobs'] = len(self.active_jobs)
        
        return obs, reward, terminated, truncated, info
    
    def _advance_to_next_pipeline(self):
        """Avanza alla prossima pipeline nella coda."""
        self.current_pipeline_idx += 1
        
        if self.current_pipeline_idx < len(self.pipelines_queue):
            self._current_pipeline = self.pipelines_queue[self.current_pipeline_idx]
        else:
            self._current_pipeline = None
        
        # Invalida action mask cache
        self._current_action_mask = None
    
    def _simulate_time_advance(self):
        """
        Simula il passaggio del tempo tra arrivi di pipeline usando Poisson process.
        
        Il tempo tra arrivi segue una distribuzione esponenziale con media
        avg_inter_arrival_time. Questo crea pattern realistici:
        - A volte burst di richieste ravvicinate
        - A volte periodi di calma che permettono ai cluster di svuotarsi
        """
        # Genera tempo inter-arrivo con distribuzione esponenziale
        inter_arrival_time = np.random.exponential(self.config.avg_inter_arrival_time)
        
        # Avanza il tempo simulato
        self.current_time += inter_arrival_time
        
        return inter_arrival_time
    
    def _update_active_jobs(self):
        """
        Controlla i job attivi e rilascia risorse per quelli completati.
        
        Questa funzione implementa il rilascio risorse:
        - Ogni job ha un finish_time calcolato quando viene piazzato
        - Quando current_time supera finish_time, il job è completato
        - Le risorse (CPU, memoria) vengono restituite al cluster
        """
        if not self.config.enable_resource_release:
            return
        
        completed_jobs = []
        remaining_jobs = []
        
        for job in self.active_jobs:
            if self.current_time >= job['finish_time']:
                # Job completato - rilascia risorse
                cluster = self.clusters_state[job['cluster_name']]
                cluster.release(job['cpu'], job['memory'])
                completed_jobs.append(job)
                self.episode_stats['resources_released'] += 1
            else:
                remaining_jobs.append(job)
        
        self.active_jobs = remaining_jobs
        
        # Invalida action mask cache perché lo spazio disponibile è cambiato
        if completed_jobs:
            self._current_action_mask = None
    
    def _add_active_job(self, pipeline: Dict, cluster_name: str, exec_time: float):
        """
        Aggiunge un job alla lista dei job attivi.
        
        Args:
            pipeline: Pipeline piazzata
            cluster_name: Cluster su cui è stata piazzata
            exec_time: Tempo di esecuzione stimato
        """
        if not self.config.enable_resource_release:
            return
        
        job = {
            'pipeline_id': pipeline['id'],
            'cluster_name': cluster_name,
            'cpu': pipeline['cpu_required'],
            'memory': pipeline['memory_required'],
            'start_time': self.current_time,
            'finish_time': self.current_time + exec_time,
        }
        self.active_jobs.append(job)
    
    def _is_data_local(self, data_location: str, cluster_name: str) -> bool:
        """Verifica se i dati sono locali al cluster."""
        if data_location == "distributed":
            return False
        if data_location == "none":
            return True  # Nessun dato da trasferire
        return data_location == cluster_name
    
    # =========================================================================
    # REWARD CALCULATION
    # =========================================================================
    
    def _calculate_reward(
        self,
        pipeline: Dict,
        cluster_state: SimulatedClusterState,
        exec_time: float,
        ideal_exec_time: float,
        is_data_local: bool
    ) -> float:
        """
        Calcola reward per placement riuscito.
        
        NORMALIZZAZIONE DINAMICA:
        Il tempo di esecuzione viene confrontato con il tempo IDEALE specifico
        per questa pipeline, non con una costante fissa. Questo evita che:
        - Pipeline piccole (10s) ricevano bonus enormi anche se piazzate male
        - Pipeline grandi (80s) vengano penalizzate anche se piazzate ottimamente
        
        COMPONENTI:
        1. Tempo di esecuzione (normalizzato dinamicamente)
        2. Data locality (secondario)
        3. Strategic placement (anti-miopatia)
        4. Bilanciamento cluster
        """
        reward = 0.0
        
        # === 1. TEMPO DI ESECUZIONE (Normalizzazione Dinamica) ===
        # Ratio: quanto peggio del tempo ideale?
        # ratio = 1.0 → perfetto (tempo == ideale)
        # ratio = 1.5 → 50% più lento del possibile
        # ratio = 2.0 → doppio del tempo ideale
        
        time_ratio = exec_time / max(ideal_exec_time, 1.0)
        
        # Reward: più vicino a 1.0 = meglio
        # time_ratio = 1.0 → reward = 0.3 * (2.0 - 1.0) = 0.3
        # time_ratio = 1.2 → reward = 0.3 * (2.0 - 1.2) = 0.24
        # time_ratio = 1.5 → reward = 0.3 * (2.0 - 1.5) = 0.15
        # time_ratio = 2.0 → reward = 0.3 * (2.0 - 2.0) = 0.0
        # time_ratio > 2.0 → reward negativo
        time_reward = self.config.reward_time_weight * (2.0 - min(time_ratio, 3.0))
        reward += time_reward
        
        # === 2. DATA LOCALITY ===
        if is_data_local:
            reward += self.config.reward_data_locality
        else:
            reward += self.config.penalty_remote_placement
        
        # === 3. STRATEGIC PLACEMENT (Anti-Miopatia) ===
        size_category = pipeline['size_category']
        
        if cluster_state.cluster_type == "cloud":
            if size_category == PipelineSizeCategory.SMALL:
                # PENALITÀ FORTE: stai sprecando il cloud con pipeline piccola!
                reward += self.config.penalty_small_on_cloud
            elif size_category == PipelineSizeCategory.LARGE:
                # BONUS: uso appropriato del cloud
                reward += self.config.bonus_large_on_cloud
            # Medium su cloud: nessun bonus/penalità (neutro)
        else:  # Edge cluster
            if size_category == PipelineSizeCategory.SMALL:
                # BONUS: pipeline piccola su edge è la scelta giusta
                reward += self.config.bonus_small_on_edge
            elif size_category == PipelineSizeCategory.MEDIUM:
                # BONUS: anche medium dovrebbe preferire edge se entra
                reward += self.config.bonus_medium_on_edge
        
        # === 4. BILANCIAMENTO ===
        avg_util = (cluster_state.cpu_utilization + cluster_state.memory_utilization) / 2.0
        
        if self.config.utilization_optimal_min <= avg_util <= self.config.utilization_optimal_max:
            reward += self.config.reward_balanced_utilization
        elif avg_util > self.config.utilization_danger:
            reward += self.config.penalty_near_saturation
        
        return reward
    
    def _calculate_episode_bonus(self) -> float:
        """Calcola bonus/penalità a fine episodio."""
        if len(self.pipelines_queue) == 0:
            return 0.0
        
        success_rate = self.episode_stats['placements_successful'] / len(self.pipelines_queue)
        
        if success_rate == 1.0:
            # Perfect episode!
            return self.config.reward_perfect_episode
        else:
            # Penalità per ogni fallimento
            failures = self.episode_stats['placements_failed']
            return failures * self.config.penalty_per_failure
    
    # =========================================================================
    # OBSERVATION
    # =========================================================================
    
    def _get_observation(self) -> np.ndarray:
        """
        Costruisce observation vector (28 features).
        
        LAYOUT:
        [0-19]  Per-cluster features (5 × 4 cluster)
        [20-23] Pipeline features (4)
        [24-27] Global features (4)
        """
        obs = []
        
        # === PER-CLUSTER FEATURES (20 features) ===
        for cluster_config in self.config.clusters:
            cluster = self.clusters_state[cluster_config.name]
            
            # [0] CPU available ratio (0-1)
            cpu_avail_ratio = cluster.cpu_available / cluster.cpu_capacity
            obs.append(cpu_avail_ratio)
            
            # [1] Memory available ratio (0-1)
            mem_avail_ratio = cluster.memory_available / cluster.memory_capacity
            obs.append(mem_avail_ratio)
            
            # [2] Is data local (0 or 1)
            if self._current_pipeline:
                is_local = float(self._is_data_local(
                    self._current_pipeline['data_location'], 
                    cluster.name
                ))
            else:
                is_local = 0.0
            obs.append(is_local)
            
            # [3] Can fit pipeline (0 or 1)
            if self._current_pipeline:
                can_fit = float(cluster.can_fit(
                    self._current_pipeline['cpu_required'],
                    self._current_pipeline['memory_required']
                ))
            else:
                can_fit = 0.0
            obs.append(can_fit)
            
            # [4] Is cloud (0 or 1)
            is_cloud = float(cluster.cluster_type == "cloud")
            obs.append(is_cloud)
        
        # === PIPELINE FEATURES (4 features) ===
        if self._current_pipeline:
            pipeline = self._current_pipeline
            
            # [20] Pipeline CPU ratio (normalized by max available)
            max_cpu = self.config.max_cpu_available
            cpu_ratio = pipeline['cpu_required'] / max_cpu if max_cpu > 0 else 0.0
            obs.append(min(cpu_ratio, 2.0))  # Cap at 2.0
            
            # [21] Pipeline memory ratio
            max_mem = self.config.max_memory_available
            mem_ratio = pipeline['memory_required'] / max_mem if max_mem > 0 else 0.0
            obs.append(min(mem_ratio, 2.0))
            
            # [22] Pipeline size category (0=small, 0.5=medium, 1=large)
            obs.append(pipeline['size_value'])
            
            # [23] Fits on any edge (0 or 1)
            fits_any_edge = any(
                self.clusters_state[c.name].can_fit(
                    pipeline['cpu_required'],
                    pipeline['memory_required']
                )
                for c in self.config.clusters if c.cluster_type == "edge"
            )
            obs.append(float(fits_any_edge))
        else:
            # No pipeline: zeros
            obs.extend([0.0, 0.0, 0.0, 0.0])
        
        # === GLOBAL FEATURES (4 features) ===
        
        # [24] Cloud CPU headroom (quanto spazio strategico ha il cloud)
        cloud = self.clusters_state.get("cloud_cluster")
        if cloud:
            cloud_headroom = cloud.cpu_available / cloud.cpu_capacity
        else:
            cloud_headroom = 0.0
        obs.append(cloud_headroom)
        
        # [25] Average edge utilization
        edge_utils = [
            (self.clusters_state[c.name].cpu_utilization + 
             self.clusters_state[c.name].memory_utilization) / 2.0
            for c in self.config.clusters if c.cluster_type == "edge"
        ]
        avg_edge_util = np.mean(edge_utils) if edge_utils else 0.0
        obs.append(avg_edge_util)
        
        # [26] Utilization variance (sbilanciamento)
        all_utils = [
            (self.clusters_state[c.name].cpu_utilization + 
             self.clusters_state[c.name].memory_utilization) / 2.0
            for c in self.config.clusters
        ]
        util_variance = np.var(all_utils) if all_utils else 0.0
        # Normalizza variance (tipicamente 0-0.1)
        obs.append(min(util_variance * 10, 1.0))
        
        # [27] Pipelines remaining ratio
        if len(self.pipelines_queue) > 0:
            remaining = (len(self.pipelines_queue) - self.current_pipeline_idx) / len(self.pipelines_queue)
        else:
            remaining = 0.0
        obs.append(remaining)
        
        return np.array(obs, dtype=np.float32)
    
    # =========================================================================
    # ACTION MASKING
    # =========================================================================
    
    def action_masks(self) -> np.ndarray:
        """
        Ritorna action mask per MaskablePPO.
        
        True = azione valida (cluster può ospitare pipeline)
        False = azione invalida (cluster saturo)
        """
        if self._current_action_mask is not None:
            return self._current_action_mask
        
        mask = np.zeros(self.config.num_clusters, dtype=bool)
        
        if self._current_pipeline is None:
            # Nessuna pipeline: tutte le azioni invalide
            return mask
        
        pipeline = self._current_pipeline
        
        for i, cluster_config in enumerate(self.config.clusters):
            cluster = self.clusters_state[cluster_config.name]
            if cluster.can_fit(pipeline['cpu_required'], pipeline['memory_required']):
                mask[i] = True
        
        # Se nessun cluster può ospitare, abilita tutti (l'agente fallirà ma deve scegliere)
        if not mask.any():
            mask[:] = True
        
        self._current_action_mask = mask
        return mask
    
    # Alias per compatibilità con sb3-contrib
    def get_action_mask(self) -> np.ndarray:
        """Alias per action_masks()."""
        return self.action_masks()
    
    # =========================================================================
    # INFO & RENDER
    # =========================================================================
    
    def _get_info(self) -> Dict[str, Any]:
        """Ritorna info dict con statistiche episodio."""
        total_pipelines = len(self.pipelines_queue)
        
        if total_pipelines > 0:
            success_rate = self.episode_stats['placements_successful'] / total_pipelines
            locality_rate = self.episode_stats['data_locality_hits'] / max(1, self.episode_stats['placements_successful'])
        else:
            success_rate = 0.0
            locality_rate = 0.0
        
        avg_exec_time = (
            np.mean(self.episode_stats['execution_times'])
            if self.episode_stats['execution_times'] else 0.0
        )
        
        # Calcola efficienza media (actual_time / ideal_time)
        if self.episode_stats['execution_times'] and self.episode_stats['ideal_execution_times']:
            efficiencies = [
                ideal / actual if actual > 0 else 0.0
                for actual, ideal in zip(
                    self.episode_stats['execution_times'],
                    self.episode_stats['ideal_execution_times']
                )
            ]
            avg_efficiency = np.mean(efficiencies)
        else:
            avg_efficiency = 0.0
        
        return {
            'step': self.current_step,
            'pipeline_idx': self.current_pipeline_idx,
            'total_pipelines': total_pipelines,
            'success_rate': success_rate,
            'locality_rate': locality_rate,
            'avg_exec_time': avg_exec_time,
            'avg_efficiency': avg_efficiency,  # 1.0 = perfetto, <1.0 = overhead
            'total_reward': self.episode_stats['total_reward'],
            'placements_successful': self.episode_stats['placements_successful'],
            'placements_failed': self.episode_stats['placements_failed'],
            'placements_failed_contention': self.episode_stats['placements_failed_contention'],
            'small_on_cloud': self.episode_stats['small_on_cloud'],
            'large_on_cloud': self.episode_stats['large_on_cloud'],
            'cluster_placements': self.episode_stats['cluster_placements'].copy(),
            'resources_released': self.episode_stats['resources_released'],
            'active_jobs': len(self.active_jobs),
            'current_time': self.current_time,
            # Diagnostica categorie pipeline
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
        """Output leggibile per debugging."""
        print("\n" + "=" * 70)
        print(f"Step: {self.current_step} | Pipeline: {self.current_pipeline_idx + 1}/{len(self.pipelines_queue)} | "
              f"Time: {self.current_time:.1f}s | Active Jobs: {len(self.active_jobs)}")
        print("-" * 70)
        
        # Stato cluster
        for cluster_config in self.config.clusters:
            cluster = self.clusters_state[cluster_config.name]
            cpu_pct = cluster.cpu_utilization * 100
            mem_pct = cluster.memory_utilization * 100
            placements = cluster.placements_count
            
            bar_len = 20
            cpu_bar = "█" * int(cpu_pct / 100 * bar_len) + "░" * (bar_len - int(cpu_pct / 100 * bar_len))
            
            # Conta job attivi su questo cluster
            active_on_cluster = sum(1 for j in self.active_jobs if j['cluster_name'] == cluster.name)
            
            print(f"  {cluster.name:15s} | CPU: [{cpu_bar}] {cpu_pct:5.1f}% | "
                  f"MEM: {mem_pct:5.1f}% | Jobs: {active_on_cluster}")
        
        print("-" * 70)
        
        # Pipeline corrente
        if self._current_pipeline:
            p = self._current_pipeline
            print(f"  Current: {p['name']} | {p['cpu_required']}m CPU | "
                  f"{p['memory_required']/(1024**2):.0f}MB | "
                  f"Data: {p['data_location']} | Size: {p['size_category']}")
        
        # Stats
        info = self._get_info()
        print(f"\n  Success: {info['success_rate']*100:.1f}% | "
              f"Efficiency: {info['avg_efficiency']*100:.1f}% | "
              f"Contention Fails: {info['placements_failed_contention']} | "
              f"Released: {info['resources_released']}")
        print("=" * 70)
    
    def _render_ansi(self) -> str:
        """Ritorna stringa per logging."""
        info = self._get_info()
        return (f"Step {self.current_step}: "
                f"success={info['success_rate']*100:.0f}% "
                f"reward={info['total_reward']:.2f}")
    
    def close(self):
        """Cleanup."""
        pass


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("CloudContinuum Environment Test")
    print("=" * 70)
    
    # Test con configurazione default
    env = CloudContinuumEnv(render_mode="human")
    
    print(f"\n📐 Environment Specs:")
    print(f"  Observation space: {env.observation_space.shape}")
    print(f"  Action space: {env.action_space.n} clusters")
    
    print("\n🔄 Testing reset...")
    obs, info = env.reset(seed=42)
    print(f"  Observation shape: {obs.shape}")
    print(f"  Pipelines in queue: {info['total_pipelines']}")
    
    print("\n🎮 Running episode with random valid actions...")
    env.render()
    
    total_reward = 0
    step = 0
    
    while True:
        # Get valid actions
        mask = env.action_masks()
        valid_actions = np.where(mask)[0]
        
        if len(valid_actions) == 0:
            print("  No valid actions!")
            break
        
        # Random valid action
        action = np.random.choice(valid_actions)
        
        obs, reward, terminated, truncated, info = env.step(action)
        total_reward += reward
        step += 1
        
        if step <= 3 or terminated:
            env.render()
        
        if terminated or truncated:
            break
    
    print(f"\n📊 Episode Summary:")
    print(f"  Total steps: {step}")
    print(f"  Total reward: {total_reward:.2f}")
    print(f"  Success rate: {info['success_rate']*100:.1f}%")
    print(f"  Avg exec time: {info['avg_exec_time']:.1f}s")
    print(f"  Data locality: {info['locality_rate']*100:.1f}%")
    print(f"  Small on cloud: {info['small_on_cloud']}")
    print(f"  Large on cloud: {info['large_on_cloud']}")
    print(f"  Cluster distribution: {info['cluster_placements']}")
    
    # Test action masking
    print("\n🎭 Testing Action Masking...")
    obs, _ = env.reset(seed=123)
    
    for i in range(3):
        mask = env.action_masks()
        print(f"  Pipeline {i+1}: mask = {mask}, valid = {np.where(mask)[0]}")
        
        action = np.random.choice(np.where(mask)[0])
        obs, reward, done, _, _ = env.step(action)
        
        if done:
            break
    
    print("\n" + "=" * 70)
    print("✅ Environment Test Completed!")
    print("=" * 70)