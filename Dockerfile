# Instala depend-slim

WORKDIR /encias
COPY requirements.txt .
RUN pip installr requirements.txt --no-cache-dir -

# Copia todo el repositorio (incluye api/ y worker/)
COPY . .

ENV PYTHONUNBUFFERED=1

# No definimos CMD aquí.
# Fly comando definido.io ejecutará el en cada fly.toml
CMD ["python", "--version"]
