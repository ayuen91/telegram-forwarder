#!/bin/bash
# ============================================================
# VPS Initial Setup Script
# Run once on a fresh Ubuntu/Debian VPS
# Usage: sudo bash setup_vps.sh
# ============================================================

set -euo pipefail

echo "=== Telegram Forwarder — VPS Setup ==="
echo ""

# ── 1. System updates ────────────────────────────────────────
echo "[1/7] Updating system packages..."
apt-get update && apt-get upgrade -y

# ── 2. Install Docker ────────────────────────────────────────
echo "[2/7] Installing Docker..."
if ! command -v docker &> /dev/null; then
    curl -fsSL https://get.docker.com | sh
    echo "Docker installed successfully"
else
    echo "Docker already installed"
fi

# Enable Docker to start on boot (survives VPS reboots)
systemctl enable docker
systemctl start docker

# ── 3. Install Docker Compose (v2 plugin) ────────────────────
echo "[3/7] Verifying Docker Compose..."
if docker compose version &> /dev/null; then
    echo "Docker Compose v2 available"
else
    echo "Installing Docker Compose plugin..."
    apt-get install -y docker-compose-plugin
fi

# ── 4. Create non-root user ──────────────────────────────────
echo "[4/7] Creating service user..."
USERNAME="forwarder"
if id "$USERNAME" &>/dev/null; then
    echo "User '$USERNAME' already exists"
else
    useradd -m -s /bin/bash "$USERNAME"
    usermod -aG docker "$USERNAME"
    echo "Created user '$USERNAME' with Docker access"
fi

# ── 5. Firewall setup ────────────────────────────────────────
echo "[5/7] Configuring firewall (UFW)..."
apt-get install -y ufw
ufw default deny incoming
ufw default allow outgoing
ufw allow ssh
# n8n only accessible via localhost (use Nginx reverse proxy for remote)
# ufw allow 5678/tcp  # Uncomment ONLY if you want direct n8n access
ufw --force enable
echo "Firewall configured: SSH allowed, all other incoming blocked"

# ── 6. Automatic security updates ────────────────────────────
echo "[6/7] Setting up automatic security updates..."
apt-get install -y unattended-upgrades
dpkg-reconfigure -plow unattended-upgrades

# ── 7. Setup application directory ───────────────────────────
echo "[7/7] Setting up application directory..."
APP_DIR="/opt/telegram-forwarder"
mkdir -p "$APP_DIR"/{data,logs,media,backups,config,bot/sessions}

# Set ownership
chown -R "$USERNAME":"$USERNAME" "$APP_DIR"

# ── 8. Install cron jobs ─────────────────────────────────────
echo "Installing cron jobs..."
CRON_FILE="/etc/cron.d/telegram-forwarder"
cat > "$CRON_FILE" << 'EOF'
# Backup + cleanup: daily at 3:00 AM
0 3 * * * forwarder /opt/telegram-forwarder/scripts/backup.sh >> /opt/telegram-forwarder/logs/backup.log 2>&1
EOF

chmod 644 "$CRON_FILE"
echo "Cron jobs installed"

# ── Done ─────────────────────────────────────────────────────
echo ""
echo "=== Setup Complete ==="
echo ""
echo "Next steps:"
echo "  1. Copy your project files to $APP_DIR"
echo "  2. Copy .env.example to .env and fill in your credentials"
echo "  3. Run: cd $APP_DIR && docker compose up -d"
echo "  4. First run: docker compose exec bot python main.py"
echo "     (You'll need to authenticate Pyrogram with your phone number)"
echo ""
