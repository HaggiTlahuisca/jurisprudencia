FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo el repositorio (incluye api/ y worker/)
COPY . .

ENV PYTHONUNBUFFERED=1

# No definimos CMD aquí.
# Fly.io ejecutará el comando definido en cada fly.toml
CMD ["python", "--version"]
