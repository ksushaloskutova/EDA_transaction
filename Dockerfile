FROM python:3.11-slim

WORKDIR /app

# System deps (optional; slim usually ok for wheels)
RUN pip install --no-cache-dir --upgrade pip

# Copy requirements first for better layer caching
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY src ./src

# Default directories (can be mounted via volumes)
RUN mkdir -p /app/data /app/cache /app/logs

# Env defaults (overridden by docker-compose or .env)
ENV DATA_DIR=/app/data \
    CACHE_DIR=/app/cache \
    LOGS_DIR=/app/logs \
    TIMEOUT=60 \
    RETRIES=3 \
    VERIFY_SSL=true \
    SKIP_IF_EXISTS=true \
    FORCE_DOWNLOAD=false

# One-off run: download and exit
CMD ["python", "-m", "src.download_data"]
