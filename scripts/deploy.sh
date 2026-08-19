#!/bin/bash
# ============================================================
# Deploy Script
# Validates config, rebuilds images, restarts services
# Usage: bash deploy.sh
# ============================================================

set -euo pipefail

APP_DIR="/opt/telegram-forwarder"
cd "$APP_DIR"

echo "=== Telegram Forwarder — Deploy ==="
echo ""

# ── 1. Validate .env ─────────────────────────────────────────
echo "[1/5] Validating configuration..."
if [ ! -f ".env" ]; then
    echo "ERROR: .env file not found!"
    echo "Copy .env.example to .env and fill in your credentials."
    exit 1
fi

REQUIRED_VARS=(
    "API_ID"
    "API_HASH"
    "PHONE_NUMBER"
    "BOT_TOKEN"
    "ALERT_BOT_TOKEN"
    "ALERT_CHAT_ID"
)

MISSING=0
for VAR in "${REQUIRED_VARS[@]}"; do
    if ! grep -q "^${VAR}=" .env; then
        echo "  MISSING: $VAR"
        MISSING=1
    fi
done

if [ "$MISSING" -eq 1 ]; then
    echo "ERROR: Missing required environment variables. Check .env file."
    exit 1
fi
echo "  ✓ All required variables present"

# ── 2. Validate config files ─────────────────────────────────
echo "[2/5] Checking config files..."
for FILE in "config/channels.yml" "config/replacements.yml"; do
    if [ ! -f "$FILE" ]; then
        echo "  WARNING: $FILE not found"
    else
        echo "  ✓ $FILE exists"
    fi
done

# ── 3. Pull latest code (if git repo) ────────────────────────
echo "[3/5] Updating code..."
if [ -d ".git" ]; then
    git pull origin main 2>/dev/null || echo "  Not a git repo or no remote, skipping pull"
else
    echo "  No git repo, skipping pull"
fi

# ── 4. Rebuild and restart ───────────────────────────────────
echo "[4/5] Building and starting services..."
docker compose build --no-cache
docker compose up -d

# ── 5. Health check ──────────────────────────────────────────
echo "[5/5] Waiting for services to start..."
sleep 15

echo ""
echo "Service status:"
docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

echo ""

# Check Redis
if docker compose exec -T redis redis-cli ping | grep -q "PONG"; then
    echo "  ✓ Redis: healthy"
else
    echo "  ✗ Redis: not responding"
fi

# Check bot container is running
BOT_STATUS=$(docker compose ps bot --format "{{.Status}}" 2>/dev/null)
if echo "$BOT_STATUS" | grep -qi "up"; then
    echo "  ✓ Bot: running"
else
    echo "  ✗ Bot: not running — check logs: docker compose logs bot"
fi

echo ""
echo "=== Deploy Complete ==="
echo ""
echo "Useful commands:"
echo "  docker compose logs bot --tail 50     # View bot logs"
echo "  docker compose logs bot -f            # Follow bot logs"
echo "  docker compose exec redis redis-cli LLEN queue:messages  # Queue depth"
echo "  docker compose restart bot            # Restart bot only"
echo ""
