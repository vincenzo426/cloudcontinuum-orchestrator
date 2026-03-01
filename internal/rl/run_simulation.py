# run_simulation.py
import os
import numpy as np
from internal.rl.environment import CloudContinuumEnv
from internal.rl.config import EnvironmentConfig

from sb3_contrib import MaskablePPO
from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize

def simulate_with_custom_model(model_path: str, vec_norm_path: str = None):
    # 1. Configurazione della simulazione
    custom_config = EnvironmentConfig(
        pipelines_per_episode=100,
        max_episode_steps=120,
        avg_inter_arrival_time=35.0,
        enable_stochastic_failure=True
    )

    # 2. Creiamo l'ambiente base
    # Per usare VecNormalize e MaskablePPO al meglio, dobbiamo "vettorizzare" l'ambiente
    def make_env():
        return CloudContinuumEnv(config=custom_config, render_mode="human", seed=42)
    
    env = DummyVecEnv([make_env])

    # 3. Carichiamo la normalizzazione (CRITICO se hai usato VecNormalize nel training)
    if vec_norm_path and os.path.exists(vec_norm_path):
        print(f"Caricamento normalizzazione da: {vec_norm_path}...")
        env = VecNormalize.load(vec_norm_path, env)
        env.training = False      # Disabilita l'aggiornamento delle medie/varianze
        env.norm_reward = False   # Non normalizzare le reward per una lettura più chiara in console
    else:
        print("Nessun file vec_normalize trovato. Procedo senza normalizzazione.")

    # 4. Carichiamo il tuo modello RL
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Modello non trovato nel percorso: {model_path}")
        
    print(f"Caricamento modello RL da: {model_path}...")
    model = MaskablePPO.load(model_path, env=env)

    print("\n" + "="*60)
    print("🚀 INIZIO SIMULAZIONE CON MODELLO CUSTOM")
    print("="*60)

    # 5. Eseguiamo il loop di simulazione
    obs = env.reset()
    done = False
    
    while not done:
        # Recuperiamo la maschera delle azioni (cluster validi)
        # Dato che usiamo DummyVecEnv, dobbiamo estrarla con env_method
        action_masks = env.env_method("action_masks")[0]
        
        # Il modello predice l'azione (deterministic=True per la massima performance in inferenza)
        action, _states = model.predict(obs, action_masks=action_masks, deterministic=True)
        
        # Step nell'ambiente
        obs, rewards, dones, infos = env.step(action)
        
        # Stampa a schermo (render)
        env.render()
        
        # In un ambiente vettorizzato, "dones" è un array
        done = dones[0]

    # 6. Stampiamo il resoconto finale usando le metriche raccolte dall'ambiente
    info = infos[0] # Prendiamo le info dell'ultimo step
    
    print("\n" + "="*60)
    print("📊 RISULTATI FINALI SIMULAZIONE")
    print("="*60)
    print(f"Pipeline Totali processate: {info.get('total_pipelines', 0)}")
    print(f"Piazzamenti completati: {info.get('placements_successful', 0)}")
    print(f"Success Rate Globale: {info.get('success_rate', 0)*100:.1f}%")
    print(f"Data Locality Rate: {info.get('locality_rate', 0)*100:.1f}%")
    print(f"Tempo di esecuzione medio: {info.get('avg_exec_time', 0):.2f} secondi")
    print(f"\nDistribuzione sui Cluster:")
    for cluster_name, count in info.get('cluster_placements', {}).items():
        print(f"  - {cluster_name}: {count} pipeline")
    print("="*60)

if __name__ == "__main__":
    # Inserisci qui i percorsi effettivi del tuo modello
    # Assicurati che puntino ai file corretti (.zip per il modello, .pkl per la normalizzazione)
    MY_MODEL_PATH = "./models_with_latencies/stage3_hard/final_model.zip" 
    MY_VEC_NORM_PATH = "./models_with_latencies/stage3_hard/vec_normalize.pkl"

    simulate_with_custom_model(MY_MODEL_PATH, MY_VEC_NORM_PATH)