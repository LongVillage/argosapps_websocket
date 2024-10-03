# Utilise une image Python officielle
FROM python:3.9-slim

# Crée un répertoire de travail
WORKDIR /app

RUN apt-get update && apt-get install -y gcc

# Copie les fichiers nécessaires
COPY script.py /app
COPY requirements.txt /app

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Démarre le script Python
CMD ["python", "script.py"]
