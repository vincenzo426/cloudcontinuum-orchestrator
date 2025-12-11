from kfp import dsl
from kfp import compiler

# 1. Definizione del Primo Componente (Leggero)
# Questo componente simula la preparazione dei dati.
# Non richiede molta potenza, quindi assegneremo risorse basse.
@dsl.component
def preprocess_data_op() -> str:
    import time
    print("Inizio pre-processing dei dati...")
    # Simuliamo un lavoro veloce
    time.sleep(2)
    print("Dati puliti e pronti.")
    return "dataset_clean.csv"

# 2. Definizione del Secondo Componente (Pesante)
# Questo componente simula il training di un modello.
# Richiede più calcoli, quindi assegneremo più CPU e RAM.
@dsl.component
def train_model_op(dataset_path: str):
    import time
    print(f"Lettura del dataset da: {dataset_path}")
    print("Inizio training del modello (simulazione carico pesante)...")
    
    # Simuliamo un carico di lavoro più lungo
    # In un caso reale, qui ci sarebbe codice PyTorch/TensorFlow
    time.sleep(5)
    
    print("Training completato.")

# 3. Definizione della Pipeline
@dsl.pipeline(
    name='pipeline-con-risorse-custom',
    description='Esempio di assegnazione CPU/RAM differenziata per componente'
)
def resource_pipeline():
    
    # --- STEP 1: Pre-processing (Risorse Basse) ---
    task_preprocess = preprocess_data_op()
    
    # Assegniamo risorse minime
    task_preprocess.set_cpu_request("250m")      # 0.25 CPU
    task_preprocess.set_cpu_limit("500m")        # Max 0.5 CPU
    task_preprocess.set_memory_request("256Mi")  # 256 Megabytes
    task_preprocess.set_memory_limit("512Mi")    # Max 512 Megabytes
    
    
    # --- STEP 2: Training (Risorse Alte) ---
    # Passiamo l'output del primo task come input al secondo
    task_train = train_model_op(dataset_path=task_preprocess.output)
    
    # Assegniamo risorse più elevate
    task_train.set_cpu_request("1")              # 1 CPU intera garantita
    task_train.set_cpu_limit("2")                # Max 2 CPU
    task_train.set_memory_request("2Gi")         # 2 Gigabytes garantiti
    task_train.set_memory_limit("4Gi")           # Max 4 Gigabytes

# 4. Compilazione
if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=resource_pipeline,
        package_path='pipeline_resources.yaml'
    )
    print("Pipeline compilata con successo in 'pipeline_resources.yaml'")