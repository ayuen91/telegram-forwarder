"""
Metrics collection for the daily health report.

Aggregates:
  - Redis queue depths (current)
  - Redis daily received counters
  - SQLite 24h / 7d message and per-destination stats
  - Overall health verdict (green / yellow / red)
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiosqlite
import redis.asyncio as aioredis

from health import HealthCheckResult

logger = logging.getLogger(__name__)

RECEIVED_KEY_PREFIX = "metrics:received"
RECEIVED_KEY_TTL = 86400 * 14  # 14 days
SPARKLINE_CHARS = "▁▂▃▄▅▆▇█"
UTC_PLUS_3 = timezone(timedelta(hours=3))


def _resolve_tz(tz_name: str):
    if not tz_name or tz_name in ("UTC+3", "Etc/GMT-3", "GMT+3"):
        return UTC_PLUS_3
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(tz_name)
    except Exception:
        return UTC_PLUS_3


@dataclass
class DestinationStats:
    chat_id: int
    name: str
    attempted: int = 0
    sent: int = 0
    failed: int = 0
    avg_latency_s: Optional[float] = None

    @property
    def success_rate(self) -> float:
        if self.attempted == 0:
            return 100.0
        return (self.sent / self.attempted) * 100.0


@dataclass
class QueueSnapshot:
    pending: int = 0  # messages + albums
    messages: int = 0
    albums: int = 0
    retry: int = 0  # failed + deferred
    failed: int = 0
    deferred: int = 0
    dead_letter: int = 0


@dataclass
class ComponentStatus:
    name: str
    label: str
    passed: bool
    message: str = ""


@dataclass
class DailyReportMetrics:
    verdict: str  # "green" | "yellow" | "red"
    components: List[ComponentStatus] = field(default_factory=list)
    disk_free_gb: Optional[float] = None
    queues: QueueSnapshot = field(default_factory=QueueSnapshot)
    received_24h: int = 0
    sent_24h: int = 0
    failed_24h: int = 0
    attempted_24h: int = 0
    success_rate_24h: float = 100.0
    destinations: List[DestinationStats] = field(default_factory=list)
    volume_7d: List[int] = field(default_factory=list)  # oldest → newest, length 7
    volume_7d_avg: float = 0.0
    report_date: str = ""
    timezone_label: str = "UTC+3"


def _date_key(d: date) -> str:
    return d.isoformat()


def received_key_for_date(d: date) -> str:
    return f"{RECEIVED_KEY_PREFIX}:{_date_key(d)}"


async def incr_received(redis_client: aioredis.Redis, when: Optional[datetime] = None) -> int:
    """Increment the daily received counter (call from on_message)."""
    now = when or datetime.now(timezone.utc)
    key = received_key_for_date(now.date())
    try:
        count = await redis_client.incr(key)
        if count == 1:
            await redis_client.expire(key, RECEIVED_KEY_TTL)
        return int(count)
    except Exception as e:
        logger.warning(f"Failed to incr received counter: {e}")
        return 0


async def get_received_for_dates(
    redis_client: aioredis.Redis, dates: List[date]
) -> List[int]:
    """Return received counts for each date (0 if key missing)."""
    if not dates:
        return []
    keys = [received_key_for_date(d) for d in dates]
    try:
        values = await redis_client.mget(keys)
        return [int(v) if v is not None else 0 for v in values]
    except Exception as e:
        logger.warning(f"Failed to read received counters: {e}")
        return [0] * len(dates)


def progress_bar(pct: float, width: int = 10) -> str:
    """Render a text progress bar, e.g. █████████░ 94%."""
    pct = max(0.0, min(100.0, pct))
    filled = int(round((pct / 100.0) * width))
    filled = max(0, min(width, filled))
    bar = "█" * filled + "░" * (width - filled)
    if pct >= 99.95:
        return f"{bar} 100%"
    if pct == int(pct):
        return f"{bar} {int(pct)}%"
    return f"{bar} {pct:.1f}%"


def sparkline(values: List[int]) -> str:
    """Unicode sparkline from a list of non-negative ints."""
    if not values:
        return "—"
    if all(v == 0 for v in values):
        return SPARKLINE_CHARS[0] * len(values)
    lo = min(values)
    hi = max(values)
    span = hi - lo
    if span == 0:
        return SPARKLINE_CHARS[4] * len(values)  # mid height
    out = []
    n = len(SPARKLINE_CHARS) - 1
    for v in values:
        idx = int(round(((v - lo) / span) * n))
        out.append(SPARKLINE_CHARS[idx])
    return "".join(out)


def compute_verdict(
    health_results: List[HealthCheckResult],
    queues: QueueSnapshot,
    success_rate_24h: float,
    has_attempts: bool,
) -> str:
    """
    GREEN  — all critical pass, DLQ=0, success >= 95%, pending < 20
    YELLOW — high warning OR success 80-94% OR retry > 0 OR DLQ > 0
    RED    — critical fail OR success < 80% OR pending >= 100
    """
    critical_failed = any(
        (not r.passed) and r.level == "critical" for r in health_results
    )
    high_failed = any(
        (not r.passed) and r.level == "high" for r in health_results
    )

    if critical_failed or queues.pending >= 100:
        return "red"
    if has_attempts and success_rate_24h < 80.0:
        return "red"

    if (
        high_failed
        or queues.dead_letter > 0
        or queues.retry > 0
        or queues.pending >= 20
        or (has_attempts and success_rate_24h < 95.0)
    ):
        return "yellow"

    return "green"


_COMPONENT_LABELS = {
    "hydrogram_session": "Hydrogram",
    "pyrogram_session": "Hydrogram",
    "sender_bot": "Sender",
    "redis": "Redis",
    "sqlite": "SQLite",
    "n8n_webhook": "n8n",
    "disk_space": "Disk",
}


def _pick_components(results: List[HealthCheckResult]) -> List[ComponentStatus]:
    wanted = (
        "hydrogram_session",
        "pyrogram_session",
        "sender_bot",
        "redis",
        "sqlite",
        "n8n_webhook",
        "disk_space",
    )
    by_name = {r.name: r for r in results}
    out: List[ComponentStatus] = []
    seen = set()
    for name in wanted:
        label = _COMPONENT_LABELS.get(name, name)
        if label in seen:
            continue
        r = by_name.get(name)
        if r is not None:
            seen.add(label)
            out.append(ComponentStatus(name=name, label=label, passed=r.passed, message=r.message))
    for name in wanted:
        label = _COMPONENT_LABELS.get(name, name)
        if label not in seen:
            seen.add(label)
            out.append(ComponentStatus(name=name, label=label, passed=False, message="missing"))
    return out


def _disk_free_gb(path: str = "/app/data") -> Optional[float]:
    try:
        usage = shutil.disk_usage(path)
        return usage.free / (1024 * 1024 * 1024)
    except Exception:
        return None


async def _queue_snapshot(queue_mgr) -> QueueSnapshot:
    depths = await queue_mgr.get_queue_depths()
    messages = int(depths.get("messages", 0))
    albums = int(depths.get("albums", 0))
    failed = int(depths.get("failed", 0))
    deferred = int(depths.get("deferred", 0))
    dead_letter = int(depths.get("dead_letter", 0))
    return QueueSnapshot(
        pending=messages + albums,
        messages=messages,
        albums=albums,
        retry=failed + deferred,
        failed=failed,
        deferred=deferred,
        dead_letter=dead_letter,
    )


async def _sql_destination_stats(
    db_path: str,
    dest_map: Dict[int, str],
    since_iso: str,
) -> Tuple[List[DestinationStats], int, int, int]:
    """
    Per-destination stats for rows with md.sent_at >= since_iso.
    Returns (dest_stats, total_sent, total_failed, total_attempted).
    """
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT
                md.destination_chat_id AS dest_id,
                SUM(CASE WHEN md.status = 'sent' THEN 1 ELSE 0 END) AS sent,
                SUM(CASE WHEN md.status = 'failed' THEN 1 ELSE 0 END) AS failed,
                COUNT(*) AS attempted,
                AVG(
                    CASE
                        WHEN md.status = 'sent'
                             AND m.processed_at IS NOT NULL
                             AND md.sent_at IS NOT NULL
                        THEN (julianday(md.sent_at) - julianday(m.processed_at)) * 86400.0
                        ELSE NULL
                    END
                ) AS avg_latency_s
            FROM message_destinations md
            JOIN messages m ON m.id = md.message_id
            WHERE md.sent_at >= ?
            GROUP BY md.destination_chat_id
            """,
            (since_iso,),
        )
        rows = await cursor.fetchall()

    by_id: Dict[int, DestinationStats] = {}
    total_sent = total_failed = total_attempted = 0

    for row in rows:
        dest_id = int(row["dest_id"])
        sent = int(row["sent"] or 0)
        failed = int(row["failed"] or 0)
        attempted = int(row["attempted"] or 0)
        latency = row["avg_latency_s"]
        name = dest_map.get(dest_id, str(dest_id))
        by_id[dest_id] = DestinationStats(
            chat_id=dest_id,
            name=name,
            attempted=attempted,
            sent=sent,
            failed=failed,
            avg_latency_s=float(latency) if latency is not None else None,
        )
        total_sent += sent
        total_failed += failed
        total_attempted += attempted

    # Ensure configured destinations appear even with zero traffic
    for dest_id, name in dest_map.items():
        if dest_id not in by_id:
            by_id[dest_id] = DestinationStats(chat_id=dest_id, name=name)

    # Stable order: configured destinations first, then extras by name
    ordered: List[DestinationStats] = []
    seen = set()
    for dest_id, name in dest_map.items():
        ordered.append(by_id[dest_id])
        seen.add(dest_id)
    extras = sorted(
        (s for d, s in by_id.items() if d not in seen),
        key=lambda s: s.name.lower(),
    )
    ordered.extend(extras)
    return ordered, total_sent, total_failed, total_attempted


async def _sql_daily_volume(db_path: str, days: int = 7) -> List[int]:
    """Message counts per calendar day for the last `days` days (oldest first)."""
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=days - 1)
    start_iso = f"{start.isoformat()} 00:00:00"

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT date(received_at) AS day, COUNT(*) AS cnt
            FROM messages
            WHERE received_at >= ?
            GROUP BY date(received_at)
            """,
            (start_iso,),
        )
        rows = await cursor.fetchall()

    by_day = {row["day"]: int(row["cnt"]) for row in rows}
    return [by_day.get((start + timedelta(days=i)).isoformat(), 0) for i in range(days)]


async def collect_report_metrics(
    health_results: List[HealthCheckResult],
    queue_mgr,
    redis_client: aioredis.Redis,
    db_path: str,
    destinations: List[Any],
    tz_name: str = "Etc/GMT-3",
    disk_path: str = "/app/data",
) -> DailyReportMetrics:
    """Assemble all metrics used by the daily report."""
    try:
        tz = _resolve_tz(tz_name)
    except Exception:
        tz = UTC_PLUS_3

    now_local = datetime.now(tz)
    report_date = now_local.strftime("%a %d %b %Y")
    timezone_label = "UTC+3"

    queues = await _queue_snapshot(queue_mgr)
    components = _pick_components(health_results)
    disk_free = _disk_free_gb(disk_path)

    utc_today = datetime.now(timezone.utc).date()
    dates_7 = [utc_today - timedelta(days=i) for i in range(6, -1, -1)]
    redis_7d = await get_received_for_dates(redis_client, dates_7)

    dest_map: Dict[int, str] = {}
    for d in destinations:
        chat_id = getattr(d, "chat_id", None)
        name = getattr(d, "name", None)
        if chat_id is None and isinstance(d, dict):
            chat_id = d.get("chat_id")
            name = d.get("name", str(chat_id))
        if chat_id is not None:
            dest_map[int(chat_id)] = str(name or chat_id)

    since = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    dest_stats, sent_24h, failed_24h, attempted_24h = await _sql_destination_stats(
        db_path, dest_map, since
    )

    # Rolling 24h received from SQLite; fall back to today's Redis counter
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM messages WHERE received_at >= ?",
            (since,),
        )
        row = await cur.fetchone()
        sql_received = int(row[0]) if row else 0

    received_24h = sql_received
    if received_24h == 0 and redis_7d:
        # Fresh counters before messages land in DB, or DB lag
        received_24h = redis_7d[-1]

    success_rate = (
        (sent_24h / attempted_24h) * 100.0 if attempted_24h > 0 else 100.0
    )

    # 7d volume: prefer Redis counters when any non-zero; else SQL
    if any(redis_7d):
        volume_7d = redis_7d
    else:
        volume_7d = await _sql_daily_volume(db_path, days=7)

    volume_avg = sum(volume_7d) / len(volume_7d) if volume_7d else 0.0

    verdict = compute_verdict(
        health_results,
        queues,
        success_rate,
        has_attempts=attempted_24h > 0,
    )

    return DailyReportMetrics(
        verdict=verdict,
        components=components,
        disk_free_gb=disk_free,
        queues=queues,
        received_24h=received_24h,
        sent_24h=sent_24h,
        failed_24h=failed_24h,
        attempted_24h=attempted_24h,
        success_rate_24h=success_rate,
        destinations=dest_stats,
        volume_7d=volume_7d,
        volume_7d_avg=volume_avg,
        report_date=report_date,
        timezone_label=timezone_label,
    )
