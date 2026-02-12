FROM python:3.11-slim

# Evitar archivos temporales y asegurar logs
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /app

# Instalamos dependencias mínimas (quitamos libaio1 que daba error)
RUN apt-get update && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

# Instalamos las librerías de Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el código
COPY . .

CMD ["python", "analisis.py"]