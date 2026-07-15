# Sample Daily Health Report (Telegram)

This is how the daily digest appears in Telegram when sent by the alert bot
at **08:00 UTC+3**. Delivery is two messages: a QuickChart photo, then an HTML digest.

---

## Message 1 — Chart (sendPhoto)

![QuickChart bar chart — three destination bars at 100%, 94%, 87%](https://quickchart.io/chart?w=800&h=400&bkg=%231f2937&c=%7B%22type%22%3A%22bar%22%7D)

**Caption:**

```
🟢 Daily Forward Health — Tue 14 Jul 2026
Delivery success by destination (last 24h)
```

---

## Message 2 — Digest (sendMessage, HTML)

Rendered appearance (Telegram HTML / monospace):

```
📊 Daily Forward Health — Tue 14 Jul 2026 · 08:00 UTC+3

Yesterday was smooth — 847 messages forwarded with
98.2% delivery across 3 channel(s). All systems online,
queues empty. Dest C dipped slightly (87%).

🟢 OVERALL HEALTH

Pyrogram  Sender  Redis  SQLite  n8n  Disk
  🟢        🟢      🟢      🟢    🟢   🟢  12.4 GB free

QUEUES NOW          24H VOLUME
Pending     0       Received   847
Retry       0       Success    █████████░ 98.2%
Dead-letter 0       Failed     15

DESTINATIONS (24h)
──────────────────────────────────────────
Dest A          ██████████ 100%  ⚡ 2.1s
Dest B          █████████░  94%  ⚡ 3.4s  ⚠️ 3
Dest C          ████████░░  87%  ⚡ 5.8s  ⚠️ 12

7d Volume   ▃▅▄▆▇▆▇  (avg 812/day)
```

---

## Verdict colours

| Emoji | Meaning |
|-------|---------|
| 🟢 | All critical checks OK, DLQ empty, success ≥ 95%, pending &lt; 20 |
| 🟡 | High-level warning, retries/DLQ activity, or success 80–94% |
| 🔴 | Critical component down, success &lt; 80%, or pending ≥ 100 |

---

## Other example narratives

**Retry activity (yellow):**

> 847 messages processed with 91.0% delivery. Destination B had 3 failure(s) (94%). 2 awaiting retry.

**Dead-letter (yellow):**

> ⚠️ 2 message(s) stuck in dead-letter queue. Overall delivery 91.0%. Pyrogram and Redis healthy.

**Critical (red):**

> 🔴 Critical issues detected (Sender). 340 messages received; 0.0% delivery across 3 channel(s). 340 pending in queue.

---

## Configuration

```env
DAILY_REPORT_ENABLED=true
DAILY_REPORT_HOUR=8
DAILY_REPORT_TIMEZONE=Etc/GMT-3
DAILY_REPORT_RUN_ON_START=false   # set true once to test immediately
```

Charts are generated via [QuickChart.io](https://quickchart.io) (no API key). Queue depths and component status come from the live health monitor; 24h destination stats from SQLite; received volume from Redis daily counters.
