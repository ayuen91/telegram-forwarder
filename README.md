# Telegram Message Forwarder

A production-ready system that listens to a Telegram source channel and forwards messages (with word replacements) to multiple destination channels.

## Architecture

```
Source Channel → Hydrogram user (listen + media relay) → Priority Queue → Replacements Engine → Sender Bot API → Destinations
```

**Roles:**
- **Hydrogram user account** — member of source channel (reads messages, relays media to bot via private chat or relay channel)
- **Sender bot (`BOT_TOKEN`)** — admin of **destination channels only**; receives text via `sendMessage`, media via `copyMessage` from relay chat
- **Local Replacements Engine** — applies word replacements and regular expressions from `replacements.yml` with HTML tag preservation

**Key features:**
- **Minimal bot privileges**: sender bot does not need source channel access
- **Zero-download media**: userbot copies media to relay chat; bot copies from relay to destinations
- **Forward attribution**: when the source posts a message forwarded from a channel where the sender bot is admin, destinations get the same "Forwarded from" tag via `forwardMessage`
- **Reply threading & Quotes**: Maps source message IDs to destination IDs so channel replies and quotes are preserved across small and big channels alike (with expandable blockquote fallback for unmapped historical parent posts)
- **Album support**: Buffers media groups via `media_group_id` with 2-second collection window
- **Word replacement**: Applied to text, captions, and quotes; regex and plain string supported
- **Backpressure control**: `asyncio.PriorityQueue` + Redis overflow buffer + workers with randomized delay prevents FloodWait
- **Auto-recovery**: Task supervisor auto-restarts crashed tasks, Docker HEALTHCHECK restarts hung containers
- **Health monitoring**: 9 health checks, Telegram alerts, heartbeat-based Docker healthcheck
- **Config hot-reload**: Edit `replacements.yml` or `channels.yml` without restarting

## Quick Start

### 1. Prerequisites
- VPS with Docker & Docker Compose
- Telegram user account (for Hydrogram — member of source channel)
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

# First time: authenticate Hydrogram (enter phone number + code)
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
| `RELAY_CHANNEL_ID` | ❌ | **Recommended.** Private channel for media relay — userbot + sender bot both admin |
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

# Preserve "Forwarded from" when source posts a channel-forward and the
# sender bot is admin of that origin channel. Word replacements are skipped
# for those messages. Empty allowlist = any origin where the bot is admin.
forward_attribution:
  enabled: true
  allowed_origin_channels: []
  # - chat_id: -1001111111111
  #   name: "Original News Channel"
```

**Forward attribution setup:** Add the sender bot as admin (with post permission) to each origin channel you want attributed forwards from. If `forwardMessage` fails (protected content, deleted origin, etc.), the bot falls back to the normal copy/send path without the tag.

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
| **Backpressure** | `asyncio.PriorityQueue` absorbs bursts with oldest-first prioritization; Redis overflow list handles surges |
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
├── docker-compose.yml     # Services: bot, redis
├── bot/                   # Forwarder core
│   ├── main.py            # Entry point: supervisor + self-test + silence watchdog
│   ├── listener.py        # MTProto userbot listener + PriorityQueue + workers
│   ├── media_relay.py     # Userbot copy to relay chat (media only)
│   ├── telegram_sender.py # Bot API delivery (quotes, native forwards, copies)
│   ├── forward_attribution.py  # Detect origin + admin eligibility for forward tags
│   ├── album_buffer.py    # Media group buffering with quote propagation
│   ├── replacements.py    # Word replacement & regex engine with HTML safety
│   ├── queue_manager.py   # Redis queues + retry
│   ├── deduplication.py   # Redis message dedup
│   ├── health.py          # Centralized health checks + reactive alerts
│   ├── daily_report.py    # 24h visual health digest & QuickChart generator
│   ├── config.py          # Config with hot-reload
│   └── logging_config.py  # Structured JSON logging
├── config/                # Hot-reloadable YAML configs (channels.yml, replacements.yml)
├── db/schema.sql          # SQLite schema
└── scripts/               # VPS setup, deploy, backup
```

## Troubleshooting

| Problem | Check |
|---------|-------|
| Bot not forwarding | `docker compose logs bot --tail 50` |
| Messages in failed queue | `redis-cli LLEN queue:failed` — retry worker handles automatically |
| Messages in dead letter | Alerts only — does **not** block the listener. Inspect: `docker compose exec redis redis-cli LRANGE queue:dead_letter 0 -1`. Clear after review: `docker compose exec redis redis-cli DEL queue:dead_letter` |
| Listener not receiving | Check logs for `Workers running` — if missing, startup was blocked by a **critical** check (hydrogram/redis/sender bot/sqlite). Dead letter warnings are safe to ignore at startup |
| Media not forwarding | Set `RELAY_CHANNEL_ID` to a private channel (both userbot + sender bot as admins). Without it, DM relay is used — ensure `/start` was sent to the sender bot |
| No alerts received | Verify `ALERT_BOT_TOKEN` and `ALERT_CHAT_ID` in `.env` |
| FloodWait errors | Increase `WORKER_DELAY_MIN`/`WORKER_DELAY_MAX` in `.env` |
| Account restricted | Check alerts; may need to re-authenticate or use different account |

