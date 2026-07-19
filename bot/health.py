"""
Health monitoring, heartbeat, config hot-reload, and alert bot.

Runs as a single async task (wrapped by supervised_task in main.py).
Each cycle (every 5 minutes) does four things:
  1. Run all health checks (9 checks)
  2. Write heartbeat file (for Docker HEALTHCHECK)
  3. Trigger config hot-reload (check file mtime)
  4. Send alerts for failures (with 15-min cooldown)
"""

import logging
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import aiohttp
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


@dataclass
class HealthCheckResult:
    name: str
    passed: bool
    level: str  # "critical", "high", "medium"
    message: str = ""


class HealthMonitor:
    """
    Centralized health checker with alerting.

    Usage:
        monitor = HealthMonitor(redis_client, webhook_sender, config, pyrogram_app)

        # In supervised_task loop:
        await monitor.run_cycle()

        # At startup (blocks until all pass):
        await monitor.startup_self_test()
    """

    HEARTBEAT_FILE = Path("/app/data/heartbeat")

    # Cooldown: don't repeat same alert type within this many seconds
    ALERT_COOLDOWN = 900  # 15 minutes

    def __init__(
        self,
        redis_client: aioredis.Redis,
        webhook_sender,  # WebhookSender instance
        config,  # Config instance
        pyrogram_app=None,  # Hydrogram Client instance (set after app starts)
        bot_sender=None,  # TelegramBotSender instance
        alert_bot_token: str = "",
        alert_chat_id: int = 0,
        db_path: str = "/app/data/forwarder.db",
    ):
        self.redis = redis_client
        self.webhook_sender = webhook_sender
        self.config = config
        self.pyrogram_app = pyrogram_app
        self.bot_sender = bot_sender
        self.alert_bot_token = alert_bot_token
        self.alert_chat_id = alert_chat_id
        self.db_path = db_path

        # Track last alert time per alert type (for cooldown)
        self._last_alert_times: dict = {}

    async def run_all_checks(self) -> List[HealthCheckResult]:
        """Run all health checks and return results."""
        results = []

        # 1. User account status (listen)
        results.append(await self._check_pyrogram())

        # 2. Listener liveness — session connected AND updates flowing
        results.append(await self._check_listener_liveness())

        # 3. Sender bot status (destination delivery)
        results.append(await self._check_sender_bot())

        # 4. Redis connectivity
        results.append(await self._check_redis())

        # 5. n8n webhook reachable
        results.append(await self._check_n8n())

        # 6. Queue depth (early surge warning)
        results.append(await self._check_queue_surge())

        # 7. Queue depth (critical overflow)
        results.append(await self._check_queue_depth())

        # 8. Failed queue
        results.append(await self._check_failed_queue())

        # 9. Dead letter queue
        results.append(await self._check_dead_letter())

        # 10. SQLite writable
        results.append(await self._check_sqlite())

        # 11. Disk space
        results.append(self._check_disk_space())

        # 12. Sustained delivery failure rate (live mid-day check)
        results.append(await self._check_sustained_failure_rate())

        return results

    async def run_cycle(self):
        """
        Execute one complete health cycle.
        Called by the supervised health_checker task every interval.
        """
        # Step 1: Hot-reload config if files changed
        if self.config.check_and_reload():
            try:
                await self._send_telegram_alert(
                    "⚙️ <b>Config Hot-Reloaded</b>\n\n"
                    "<blockquote>Successfully loaded configurations from YAML files. All settings applied without reboot.</blockquote>"
                )
            except Exception as e:
                logger.error(f"Failed to send config reload alert: {e}")

        # Step 2: Run health checks
        results = await self.run_all_checks()

        # Step 3: Write heartbeat file (Docker HEALTHCHECK reads this)
        self._write_heartbeat()

        # Step 4: Alert on failures
        failed = [r for r in results if not r.passed]
        if failed:
            for result in failed:
                await self._send_alert_with_cooldown(result)

        # Log summary
        passed = sum(1 for r in results if r.passed)
        total = len(results)
        if failed:
            logger.warning(
                f"Health check: {passed}/{total} passed. "
                f"Failed: {', '.join(r.name for r in failed)}"
            )
        else:
            logger.info(f"Health check: {passed}/{total} passed ✓")

    async def startup_self_test(self) -> bool:
        """
        Run health checks; only critical failures block startup.

        Operational issues (dead letter queue, failed retries, queue depth)
        are logged and alerted later — they must not stop the listener.
        """
        results = await self.run_all_checks()
        critical_failures = [r for r in results if not r.passed and r.level == "critical"]
        warnings = [r for r in results if not r.passed and r.level != "critical"]

        for result in warnings:
            logger.warning(f"Startup warning (non-blocking): {result.name}: {result.message}")

        if critical_failures:
            logger.warning(
                f"Startup self-test: {len(critical_failures)} critical checks failed: "
                f"{', '.join(f'{r.name}: {r.message}' for r in critical_failures)}"
            )
            return False

        if warnings:
            logger.info(
                f"Startup self-test: critical checks passed "
                f"({len(warnings)} non-blocking warning(s))"
            )
        else:
            logger.info("Startup self-test: all checks passed ✓")

        return True

    # ── Individual health checks ──────────────────────────────────────

    async def _check_pyrogram(self) -> HealthCheckResult:
        """Check if Pyrogram user account is accessible."""
        try:
            if self.pyrogram_app and self.pyrogram_app.is_connected:
                me = await self.pyrogram_app.get_me()
                return HealthCheckResult(
                    name="pyrogram_session",
                    passed=True,
                    level="critical",
                    message=f"Connected as {me.first_name} (ID: {me.id})",
                )
            else:
                return HealthCheckResult(
                    name="pyrogram_session",
                    passed=False,
                    level="critical",
                    message="Pyrogram client not connected",
                )
        except Exception as e:
            return HealthCheckResult(
                name="pyrogram_session",
                passed=False,
                level="critical",
                message=f"get_me() failed: {e}",
            )

    async def _check_listener_liveness(self) -> HealthCheckResult:
        """
        Check that the listener is actually receiving message updates.

        The pyrogram_session check only verifies the MTProto connection is alive.
        This check verifies that updates are FLOWING by reading the timestamp
        written by the listener's _handle() on every accepted message.

        A silent failure (connected but no updates) is the most common symptom
        of a stale session file — especially after migrating between Pyrogram
        and Hydrogram, or after a long downtime.

        Only fires during activity hours (08:00–23:59 local UTC+3) to avoid
        false positives during quiet overnight periods.
        """
        # Only check during expected activity hours (08:00–23:59 UTC+3)
        import datetime
        now_utc3 = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
        if not (8 <= now_utc3.hour < 24):
            return HealthCheckResult(
                name="listener_liveness",
                passed=True,
                level="high",
                message="Outside activity hours — liveness check skipped",
            )

        SILENCE_THRESHOLD = 1800  # 30 minutes
        try:
            from listener import LISTENER_LAST_RECEIVED_KEY
            raw = await self.redis.get(LISTENER_LAST_RECEIVED_KEY)
            if raw is None:
                # Key doesn't exist yet — either the bot just started or no
                # messages have ever been received on this session.
                return HealthCheckResult(
                    name="listener_liveness",
                    passed=True,
                    level="high",
                    message="No messages received yet (new session or quiet channel)",
                )
            last_ts = float(raw)
            silence_secs = time.time() - last_ts
            silence_min = silence_secs / 60
            if silence_secs > SILENCE_THRESHOLD:
                return HealthCheckResult(
                    name="listener_liveness",
                    passed=False,
                    level="high",
                    message=(
                        f"No messages received for {silence_min:.0f} min — "
                        "session may be connected but updates are not flowing. "
                        "Consider deleting the session file and re-authenticating."
                    ),
                )
            return HealthCheckResult(
                name="listener_liveness",
                passed=True,
                level="high",
                message=f"Last message received {silence_min:.1f} min ago",
            )
        except Exception as e:
            return HealthCheckResult(
                name="listener_liveness",
                passed=True,  # Don't block on Redis errors
                level="high",
                message=f"Liveness check skipped: {e}",
            )

    async def _check_sender_bot(self) -> HealthCheckResult:
        """Check if the sender bot token is valid."""
        try:
            if self.bot_sender:
                me = await self.bot_sender.get_me()
                return HealthCheckResult(
                    name="sender_bot",
                    passed=True,
                    level="critical",
                    message=f"Bot @{me.get('username', 'unknown')} (ID: {me.get('id')})",
                )
            return HealthCheckResult(
                name="sender_bot",
                passed=False,
                level="critical",
                message="Sender bot not configured",
            )
        except Exception as e:
            return HealthCheckResult(
                name="sender_bot",
                passed=False,
                level="critical",
                message=f"getMe() failed: {e}",
            )

    async def _check_redis(self) -> HealthCheckResult:
        """Check Redis connectivity."""
        try:
            pong = await self.redis.ping()
            return HealthCheckResult(
                name="redis", passed=bool(pong), level="critical"
            )
        except Exception as e:
            return HealthCheckResult(
                name="redis", passed=False, level="critical", message=str(e)
            )

    async def _check_n8n(self) -> HealthCheckResult:
        """Check if n8n webhook endpoint is reachable."""
        try:
            reachable = await self.webhook_sender.is_reachable()
            return HealthCheckResult(
                name="n8n_webhook",
                passed=reachable,
                level="high",
                message="" if reachable else "n8n webhook unreachable",
            )
        except Exception as e:
            return HealthCheckResult(
                name="n8n_webhook", passed=False, level="high", message=str(e)
            )

    async def _check_queue_depth(self) -> HealthCheckResult:
        """Warn if message queue is building up."""
        try:
            depth = await self.redis.llen("queue:messages")
            album_depth = await self.redis.llen("queue:albums")
            total = depth + album_depth
            passed = total < 100
            return HealthCheckResult(
                name="queue_depth",
                passed=passed,
                level="high",
                message=f"Queue depth: {total} (messages={depth}, albums={album_depth})",
            )
        except Exception as e:
            return HealthCheckResult(
                name="queue_depth", passed=False, level="high", message=str(e)
            )

    async def _check_failed_queue(self) -> HealthCheckResult:
        """Check if there are messages in the retry queue."""
        try:
            count = await self.redis.llen("queue:failed")
            return HealthCheckResult(
                name="failed_queue",
                passed=count == 0,
                level="medium",
                message=f"{count} messages awaiting retry" if count else "",
            )
        except Exception as e:
            return HealthCheckResult(
                name="failed_queue", passed=True, level="medium", message=str(e)
            )

    async def _check_dead_letter(self) -> HealthCheckResult:
        """Check dead letter queue — these need manual intervention."""
        try:
            count = await self.redis.llen("queue:dead_letter")
            return HealthCheckResult(
                name="dead_letter_queue",
                passed=count == 0,
                level="high",
                message=f"🟠 {count} messages in dead letter queue — manual review needed"
                if count
                else "",
            )
        except Exception as e:
            return HealthCheckResult(
                name="dead_letter_queue", passed=True, level="high", message=str(e)
            )

    async def _check_sqlite(self) -> HealthCheckResult:
        """Check if SQLite database is accessible."""
        try:
            import aiosqlite

            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("SELECT 1")
            return HealthCheckResult(name="sqlite", passed=True, level="critical")
        except Exception as e:
            return HealthCheckResult(
                name="sqlite", passed=False, level="critical", message=str(e)
            )

    def _check_disk_space(self) -> HealthCheckResult:
        """Check if disk has sufficient free space."""
        try:
            usage = shutil.disk_usage("/app/data")
            free_mb = usage.free / (1024 * 1024)
            passed = free_mb > 500  # Warn if less than 500MB
            return HealthCheckResult(
                name="disk_space",
                passed=passed,
                level="high",
                message=f"{free_mb:.0f}MB free" if not passed else "",
            )
        except Exception as e:
            return HealthCheckResult(
                name="disk_space", passed=True, level="high", message=str(e)
            )

    async def _check_queue_surge(self) -> HealthCheckResult:
        """
        Early warning when the pending queue climbs above 50 messages.

        _check_queue_depth fires at 100 which is already too late —
        by then the event loop is saturated. Firing at 50 gives the
        operator time to investigate before messages start dropping.
        """
        try:
            depth = await self.redis.llen("queue:messages")
            album_depth = await self.redis.llen("queue:albums")
            total = depth + album_depth
            if total >= 100:
                # Let _check_queue_depth handle the critical case
                return HealthCheckResult(
                    name="queue_surge", passed=True, level="high"
                )
            passed = total < 50
            return HealthCheckResult(
                name="queue_surge",
                passed=passed,
                level="high",
                message=(
                    f"Queue building up: {total} messages pending "
                    "(threshold=50). Delivery may be slowing down."
                ) if not passed else "",
            )
        except Exception as e:
            return HealthCheckResult(
                name="queue_surge", passed=True, level="high", message=str(e)
            )

    async def _check_sustained_failure_rate(self) -> HealthCheckResult:
        """
        Real-time delivery failure rate check.

        Queries the last 20 message delivery attempts from SQLite and fires
        a CRITICAL alert if more than 20% failed. This catches delivery
        degradation mid-day without waiting for the 08:00 daily report.

        Only runs when there has been recent activity (at least 10 attempts
        in the last 20 rows) to avoid false positives during quiet periods.
        """
        SAMPLE_SIZE = 20
        FAILURE_THRESHOLD = 0.20  # 20%
        MIN_SAMPLE = 10           # need at least 10 attempts to be meaningful
        try:
            import aiosqlite
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    """
                    SELECT status FROM message_destinations
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (SAMPLE_SIZE,),
                )
                rows = await cursor.fetchall()

            if len(rows) < MIN_SAMPLE:
                return HealthCheckResult(
                    name="delivery_failure_rate",
                    passed=True,
                    level="critical",
                    message="Not enough samples yet",
                )

            failed = sum(1 for r in rows if r["status"] == "failed")
            rate = failed / len(rows)

            if rate > FAILURE_THRESHOLD:
                return HealthCheckResult(
                    name="delivery_failure_rate",
                    passed=False,
                    level="critical",
                    message=(
                        f"{failed}/{len(rows)} recent deliveries failed "
                        f"({rate * 100:.0f}% failure rate, threshold={int(FAILURE_THRESHOLD * 100)}%). "
                        "Check destination channels and bot permissions."
                    ),
                )

            return HealthCheckResult(
                name="delivery_failure_rate",
                passed=True,
                level="critical",
                message=f"{failed}/{len(rows)} failed ({rate * 100:.0f}%)",
            )
        except Exception as e:
            return HealthCheckResult(
                name="delivery_failure_rate",
                passed=True,
                level="critical",
                message=f"Check skipped: {e}",
            )

    # ── Heartbeat ─────────────────────────────────────────────────────

    def _write_heartbeat(self):
        """Write current timestamp to heartbeat file for Docker HEALTHCHECK."""
        try:
            self.HEARTBEAT_FILE.parent.mkdir(parents=True, exist_ok=True)
            self.HEARTBEAT_FILE.write_text(str(time.time()))
        except Exception as e:
            logger.error(f"Failed to write heartbeat: {e}")

    # ── Alert delivery ────────────────────────────────────────────────

    async def _send_alert_with_cooldown(self, result: HealthCheckResult):
        """Send alert via Telegram bot, respecting cooldown per alert type."""
        now = time.time()
        last_time = self._last_alert_times.get(result.name, 0)

        if now - last_time < self.ALERT_COOLDOWN:
            logger.debug(f"Alert cooldown active for {result.name}, skipping")
            return

        level_emoji = {"critical": "🔴", "high": "🟠", "medium": "🟡"}.get(
            result.level, "⚪"
        )

        details_html = f"<code>{result.message}</code>" if result.message else "<i>No details provided.</i>"
        if result.message and len(result.message) > 100:
            import html
            details_html = f"<blockquote expandable><pre>{html.escape(result.message)}</pre></blockquote>"

        message = (
            f"{level_emoji} <b>ALERT: Telegram Forwarder</b>\n\n"
            f"<blockquote><b>Component:</b> <code>{result.name}</code>\n"
            f"<b>Status:</b> FAILED\n"
            f"<b>Severity:</b> <code>{result.level.upper()}</code></blockquote>\n"
            f"📝 <b>Details:</b>\n{details_html}\n\n"
            f"🕒 <i>{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}</i>"
        )

        try:
            await self._send_telegram_alert(message)
            self._last_alert_times[result.name] = now
            logger.info(f"Alert sent for {result.name}")
        except Exception as e:
            logger.error(f"Failed to send alert for {result.name}: {e}")

    async def _send_telegram_alert(self, message: str):
        """Send a message via the alert bot to your personal chat."""
        if not self.alert_bot_token or not self.alert_chat_id:
            logger.warning("Alert bot not configured, skipping alert")
            return

        url = f"https://api.telegram.org/bot{self.alert_bot_token}/sendMessage"
        payload = {
            "chat_id": self.alert_chat_id,
            "text": message,
            "parse_mode": "HTML",
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error(f"Alert bot API error: {resp.status} {body[:200]}")
