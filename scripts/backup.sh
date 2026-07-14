#!/bin/bash
# ============================================================
# Backup + Disk Cleanup Script
# Combines SQLite backup, Redis snapshot, and disk cleanup
# Run via cron: 0 3 * * * /opt/telegram-forwarder/scripts/backup.sh
# ============================================================

set -euo pipefail

APP_DIR="/opt/telegram-forwarder"
BACKUP_DIR="$APP_DIR/backups"
DATA_DIR="$APP_DIR/data"
MEDIA_DIR="$APP_DIR/media"
DATE=$(date +%Y%m%d_%H%M%S)

echo "[$(date)] Backup + cleanup starting..."

# ── 1. SQLite Backup ─────────────────────────────────────────
echo "  [1/4] Backing up SQLite database..."
mkdir -p "$BACKUP_DIR"

DB_FILE="$DATA_DIR/forwarder.db"
if [ -f "$DB_FILE" ]; then
    # Use SQLite's online backup (safe while bot is running)
    sqlite3 "$DB_FILE" ".backup '$BACKUP_DIR/forwarder_${DATE}.db'"
    echo "  ✓ SQLite backed up to forwarder_${DATE}.db"
else
    echo "  ⚠ No database file found at $DB_FILE"
fi

# ── 2. Redis Snapshot ────────────────────────────────────────
echo "  [2/4] Triggering Redis snapshot..."
docker compose -f "$APP_DIR/docker-compose.yml" exec -T redis redis-cli BGSAVE 2>/dev/null \
    && echo "  ✓ Redis BGSAVE triggered" \
    || echo "  ⚠ Redis snapshot failed (container may not be running)"

# ── 3. Disk Cleanup ──────────────────────────────────────────
echo "  [3/4] Cleaning up disk..."

# Delete temp album media files older than 1 hour
if [ -d "$MEDIA_DIR" ]; then
    DELETED=$(find "$MEDIA_DIR" -type f -mmin +60 -delete -print 2>/dev/null | wc -l)
    echo "  ✓ Deleted $DELETED temp media files"
fi

# Prune unused Docker images/containers (not volumes — we need those)
docker system prune -f 2>/dev/null \
    && echo "  ✓ Docker system pruned" \
    || echo "  ⚠ Docker prune failed"

# ── 4. Backup Rotation ──────────────────────────────────────
echo "  [4/4] Rotating old backups..."

# Keep only last 7 daily backups
if [ -d "$BACKUP_DIR" ]; then
    OLD_COUNT=$(find "$BACKUP_DIR" -name "forwarder_*.db" -mtime +7 -print 2>/dev/null | wc -l)
    find "$BACKUP_DIR" -name "forwarder_*.db" -mtime +7 -delete 2>/dev/null
    echo "  ✓ Removed $OLD_COUNT backups older than 7 days"
fi

# Show disk usage summary
echo ""
echo "  Disk usage:"
echo "    Data:    $(du -sh "$DATA_DIR" 2>/dev/null | cut -f1)"
echo "    Backups: $(du -sh "$BACKUP_DIR" 2>/dev/null | cut -f1)"
echo "    Media:   $(du -sh "$MEDIA_DIR" 2>/dev/null | cut -f1)"
echo "    Free:    $(df -h "$APP_DIR" | tail -1 | awk '{print $4}')"

echo "[$(date)] Backup + cleanup complete"
