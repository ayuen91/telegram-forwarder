# Telegram Message Forwarder

A production-ready system that listens to a Telegram source channel and forwards messages (with word replacements) to multiple destination channels.

## Architecture

```
Source Channel → Pyrogram user (listen + media relay) → Redis Queue → n8n (word replace) → Sender Bot API → Destinations
```

**Roles:**
- **Pyrogram user account** — member of source channel (reads messages, relays media to bot via private chat)
- **Sender bot (`BOT_TOKEN`)** — admin of **destination channels only**; receives text via `sendMessage`, media via `copyMessage` from relay chat
- **n8n** — HMAC-verified webhook that applies word replacements from `replacements.yml` and returns JSON

**Key features:**
- **Minimal bot privileges**: sender bot does not need source channel access
- **Zero-download media**: userbot copies media to relay chat; bot copies from relay to destinations
- **Reply threading**: Maps source message IDs to destination IDs so channel replies are preserved
- **Album support**: Buffers media groups via `media_group_id` with 2-second collection window
- **Word replacement**: Applied to both text and captions, regex and plain string supported
- **Backpressure control**: `asyncio.Queue` + 2 workers with randomized delay prevents FloodWait
- **Auto-recovery**: Task supervisor auto-restarts crashed tasks, Docker HEALTHCHECK restarts hung containers
- **Health monitoring**: 9 health checks, Telegram alerts, heartbeat-based Docker healthcheck
- **Config hot-reload**: Edit `replacements.yml` or `channels.yml` without restarting

## Quick Start

### 1. Prerequisites
- VPS with Docker & Docker Compose
- Telegram user account (for Pyrogram — member of source channel)
- Telegram Bot (sender) — admin of **destination channels only**; send `/start` to it from the user account
- Telegram Bot (alerts) — sends health alerts to your personal chat

### 2. Setup

```bash
# Clone/copy the project to your VPS
cd /opt/telegram-forwarder

# Copy and fill in your credentials
cp .env.example .env
nano .env

# Edit channel routing
nano config/channels.yml

# Edit word replacement rules
nano config/replacements.yml
```

### 3. First Run

```bash
# Start services
docker compose up -d

# First time: authenticate Pyrogram (enter phone number + code)
docker compose run --rm bot python main.py

# After authentication succeeds, restart normally
docker compose restart bot
```

### 4. VPS Setup (Fresh Server)

```bash
sudo bash scripts/setup_vps.sh
```

This installs Docker, enables it on boot, sets up firewall, creates a service user, and installs backup cron jobs.

## Configuration

### Environment Variables (`.env`)

| Variable | Required | Description |
|----------|----------|-------------|
| `API_ID` | ✅ | Telegram API ID (from my.telegram.org) |
| `API_HASH` | ✅ | Telegram API hash |
| `PHONE_NUMBER` | ✅ | Phone number for user account |
| `BOT_TOKEN` | ✅ | Sender bot token |
| `RELAY_CHAT_ID` | ❌ | Chat where userbot relays media for the bot (default: user account ID) |
| `ALERT_BOT_TOKEN` | ✅ | Alert bot token (separate from sender) |
| `ALERT_CHAT_ID` | ✅ | Your personal chat ID for alerts |
| `WEBHOOK_SECRET` | ✅ | HMAC secret (generate a random 64-char string) |
| `WORKER_COUNT` | ❌ | Queue workers (default: 2) |
| `WORKER_DELAY_MIN` | ❌ | Min random delay in seconds (default: 0.5) |
| `WORKER_DELAY_MAX` | ❌ | Max random delay in seconds (default: 1.5) |

### Channel Routing (`config/channels.yml`)

```yaml
source:
  chat_id: -1001234567890

destinations:
  - chat_id: -1009876543210
    name: "Destination A"
    enabled: true
```

### Word Replacements (`config/replacements.yml`)

```yaml
rules:
  - pattern: "@old_channel"
    replacement: "@new_channel"
    is_regex: false

  - pattern: "https?://old\\.com(/\\S*)?"
    replacement: "https://new.com\\1"
    is_regex: true
```

Both YAML files are **hot-reloaded** every 5 minutes — no restart needed.

## Operations

### Common Commands

```bash
# View logs
docker compose logs bot --tail 100
docker compose logs bot -f              # Follow live

# Check errors only
docker compose exec bot cat /app/logs/bot.error.log

# Queue status
docker compose exec redis redis-cli LLEN queue:messages
docker compose exec redis redis-cli LLEN queue:failed
docker compose exec redis redis-cli LLEN queue:dead_letter

# Health status
docker compose ps                       # Container health
docker compose exec bot cat /app/data/heartbeat

# Restart
docker compose restart bot              # Bot only
docker compose restart                  # All services

# Deploy update
bash scripts/deploy.sh
```

### n8n Workflow

n8n handles **word replacement only** — it does not send Telegram messages. The Python bot sends via Bot API after n8n returns the processed payload.

1. Open n8n at `http://localhost:5678` via SSH tunnel: `ssh -L 5678:localhost:5678 user@your-ec2-ip`
2. Import `n8n/workflows/message_processor.json` (use the `+` button → Import from file)
3. Activate the workflow (toggle in top-right corner)

> **Note:** `WEBHOOK_SECRET` is injected into n8n from `.env` via Docker Compose. `BOT_TOKEN` stays in the bot container only.

### Backups

Automatic daily backups run at 3:00 AM (configured by `setup_vps.sh`):
- SQLite database backup
- Redis snapshot
- Temp media cleanup
- Old backup rotation (keep 7 days)

Manual backup: `bash scripts/backup.sh`

## Reliability Features

| Feature | How It Works |
|---------|-------------|
| **Backpressure** | `asyncio.Queue(maxsize=100)` absorbs bursts; 2 workers process with random delay |
| **FloodWait** | Caught in task supervisor — auto-sleeps for requested duration |
| **Task auto-restart** | Supervisor wraps all tasks; crashes trigger alert + exponential backoff restart |
| **Docker HEALTHCHECK** | Heartbeat file checked every 2min; 3 failures = container auto-restart |
| **Deduplication** | Redis SET NX with 24h TTL prevents duplicate processing on reconnection |
| **Dead letter queue** | After 3 failed retries, messages move to dead letter queue + alert sent |
| **VPS reboot** | Docker systemd enabled; `restart: unless-stopped` on all containers |
| **Config hot-reload** | YAML file mtime checked every 5min in health cycle |

## Project Structure

```
telegram-forwarder/
├── Dockerfile             # Bot image (build context: repo root)
├── docker-compose.yml     # 3 services: bot, redis, n8n
├── bot/                   # Pyrogram user bot
│   ├── main.py            # Entry point: supervisor + self-test
│   ├── listener.py        # on_message → Queue → workers → Bot API forward
│   ├── media_relay.py     # Userbot copy to relay chat (media only)
│   ├── telegram_sender.py # Bot API sendMessage / copyMessage delivery
│   ├── album_buffer.py    # Media group collection
│   ├── queue_manager.py   # Redis queues + retry
│   ├── deduplication.py   # Message dedup
│   ├── webhook.py         # HMAC webhook sender
│   ├── health.py          # Health checks + alerts
│   ├── config.py          # Config with hot-reload
│   └── logging_config.py  # Structured JSON logging
├── config/                # Hot-reloadable YAML configs
├── db/schema.sql          # SQLite schema
├── n8n/workflows/         # n8n workflow export
└── scripts/               # VPS setup, deploy, backup
```

## Troubleshooting

| Problem | Check |
|---------|-------|
| Bot not forwarding | `docker compose logs bot --tail 50` |
| Messages in failed queue | `redis-cli LLEN queue:failed` — retry worker handles automatically |
| Messages in dead letter | Alerts only — does **not** block the listener. Inspect: `docker compose exec redis redis-cli LRANGE queue:dead_letter 0 -1`. Clear after review: `docker compose exec redis redis-cli DEL queue:dead_letter` |
| Listener not receiving | Check logs for `Workers running` — if missing, startup was blocked by a **critical** check (pyrogram/redis/sender bot/sqlite). Dead letter warnings are safe to ignore at startup |
| Media not forwarding | Relay chat not open — bot logs `Opening relay chat — userbot sending /start`. If that fails, send `/start` to the sender bot manually from the user account |
| No alerts received | Verify `ALERT_BOT_TOKEN` and `ALERT_CHAT_ID` in `.env` |
| n8n not processing | Check n8n workflow is **activated** |
| FloodWait errors | Increase `WORKER_DELAY_MIN`/`WORKER_DELAY_MAX` in `.env` |
| Account restricted | Check alerts; may need to re-authenticate or use different account |
