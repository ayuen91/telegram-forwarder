"""
Health monitoring, heartbeat, config hot-reload, and alert bot.

Runs as a single async task (wrapped by supervised_task in main.py).
Each cycle (every 5 minutes) does four things:
  1. Run all health checks (8 checks)
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
        pyrogram_app=None,  # Pyrogram Client instance (set after app starts)
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

        # 2. Sender bot status (destination delivery)
        results.append(await self._check_sender_bot())

        # 3. Redis connectivity
        results.append(await self._check_redis())

        # 4. n8n webhook reachable
        results.append(await self._check_n8n())

        # 5. Queue depth
        results.append(await self._check_queue_depth())

        # 6. Failed queue
        results.append(await self._check_failed_queue())

        # 7. Dead letter queue
        results.append(await self._check_dead_letter())

        # 8. SQLite writable
        results.append(await self._check_sqlite())

        # 9. Disk space
        results.append(self._check_disk_space())

        return results

    async def run_cycle(self):
        """
        Execute one complete health cycle.
        Called by the supervised health_checker task every interval.
        """
        # Step 1: Hot-reload config if files changed
        self.config.check_and_reload()

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
        Run health checks and return True if all pass.
        Used by main.py to block listener startup until ready.
        """
        results = await self.run_all_checks()
        all_passed = all(r.passed for r in results)

        if not all_passed:
            failed = [r for r in results if not r.passed]
            logger.warning(
                f"Startup self-test: {len(failed)} checks failed: "
                f"{', '.join(f'{r.name}: {r.message}' for r in failed)}"
            )
        else:
            logger.info("Startup self-test: all checks passed ✓")

        return all_passed

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
            return HealthCheckResult(name="sqlite", passed=True, level="high")
        except Exception as e:
            return HealthCheckResult(
                name="sqlite", passed=False, level="high", message=str(e)
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

        message = (
            f"{level_emoji} <b>ALERT: Telegram Forwarder</b>\n\n"
            f"<b>Component:</b> {result.name}\n"
            f"<b>Status:</b> FAILED\n"
            f"<b>Details:</b> {result.message}\n"
            f"<b>Time:</b> {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}"
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
