FROM python:3.9-slim

WORKDIR /app

# Installa dipendenze
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Crea struttura package rl/
RUN mkdir -p /app/rl
COPY __init__.py /app/rl/
COPY inference.py /app/rl/
COPY environment.py /app/rl/
COPY simulator.py /app/rl/
COPY config.py /app/rl/

# Esponi porta Flask
EXPOSE 5000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:5000/health')"

# Avvia come modulo nel package rl
ENTRYPOINT ["python", "-m", "rl.inference"]
CMD ["--model-path", "/models/final_model.zip", \
     "--vec-normalize-path", "/models/vec_normalize.pkl", \
     "--host", "0.0.0.0", \
     "--port", "5000"]