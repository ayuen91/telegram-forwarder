FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (sqlite3 for backup; gcc for tgcrypto C extension)
RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY bot/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code from bot/
COPY bot/ .

# Copy database schema (from repo root db/ dir)
RUN mkdir -p /app/db
COPY db/schema.sql /app/db/schema.sql

# Create required runtime directories
RUN mkdir -p /app/sessions /app/data /app/logs /app/media /app/config

# Healthcheck: checks heartbeat file written by health.py
# If 3 consecutive checks fail (~10 min stale), Docker auto-restarts container
HEALTHCHECK --interval=120s --timeout=10s --start-period=120s --retries=3 \
    CMD python /app/healthcheck_probe.py

CMD ["python", "-u", "main.py"]
