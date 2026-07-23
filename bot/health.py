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

    def __init__(
        self,
        redis_client: aioredis.Redis,
        webhook_sender,  # WebhookSender instance
        config,  # Config instance
        hydrogram_app=None,  # Hydrogram Client instance (set after app starts)
        bot_sender=None,  # TelegramBotSender instance
        alert_bot_token: str = "",
        alert_chat_id: int = 0,
        db_path: str = "/app/data/forwarder.db",
    ):
        self.redis = redis_client
        self.webhook_sender = webhook_sender
        self.config = config
        self.hydrogram_app = hydrogram_app
        self.bot_sender = bot_sender
        self.alert_bot_token = alert_bot_token
        self.alert_chat_id = alert_chat_id
        self.db_path = db_path

        # State transition tracking per health check (name -> passed)
        # Alerts fire ONCE on True -> False (failure) and ONCE on False -> True (recovery).
        # Eliminates repeated 15-minute alert spam while issues persist.
        self._last_check_status: dict = {}

        # Reactive failure rate tracking
        self._last_failure_rate: Optional[float] = None
        self._last_failure_state: str = "normal"

    async def run_all_checks(self) -> List[HealthCheckResult]:
        """Run all health checks and return results."""
        results = []

        # 1. User account status (listen)
        results.append(await self._check_hydrogram_session())

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

        # Step 4: Evaluate state transitions for alerting (fire once on change)
        await self._process_state_transition_alerts(results)

        # Log summary
        failed = [r for r in results if not r.passed]
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

    async def _check_hydrogram_session(self) -> HealthCheckResult:
        """Check if Hydrogram user account is accessible."""
        try:
            if self.hydrogram_app and self.hydrogram_app.is_connected:
                me = await self.hydrogram_app.get_me()
                return HealthCheckResult(
                    name="hydrogram_session",
                    passed=True,
                    level="critical",
                    message=f"Connected as {me.first_name} (ID: {me.id})",
                )
            else:
                return HealthCheckResult(
                    name="hydrogram_session",
                    passed=False,
                    level="critical",
                    message="Hydrogram client not connected",
                )
        except Exception as e:
            return HealthCheckResult(
                name="hydrogram_session",
                passed=False,
                level="critical",
                message=f"get_me() failed: {e}",
            )

    async def _check_listener_liveness(self) -> HealthCheckResult:
        """
        Check that the listener session is connected (monitored 24/7).

        Reads listener:last_received_at to log silence duration.
        The silence watchdog in main.py performs PTS audits via updates.GetState()
        to handle recycling — organic silence on quiet channels is not a failure.
        """
        try:
            from listener import LISTENER_LAST_RECEIVED_KEY

            # Verify client connectivity
            if self.hydrogram_app and not self.hydrogram_app.is_connected:
                return HealthCheckResult(
                    name="listener_liveness",
                    passed=False,
                    level="critical",
                    message="Hydrogram userbot client is disconnected",
                )

            raw = await self.redis.get(LISTENER_LAST_RECEIVED_KEY)
            if raw is None:
                return HealthCheckResult(
                    name="listener_liveness",
                    passed=True,
                    level="high",
                    message="No messages received yet (new session or quiet channel)",
                )

            last_ts = float(raw)
            silence_secs = time.time() - last_ts
            silence_min = silence_secs / 60

            return HealthCheckResult(
                name="listener_liveness",
                passed=True,
                level="high",
                message=f"Last message received {silence_min:.1f} min ago",
            )
        except Exception as e:
            return HealthCheckResult(
                name="listener_liveness",
                passed=False,
                level="high",
                message=f"Liveness query error: {e}",
            )

    async def _check_sender_bot(self) -> HealthCheckResult:
        """Check if the sender bot token is valid and relay channel accessible."""
        try:
            if not self.bot_sender:
                return HealthCheckResult(
                    name="sender_bot",
                    passed=False,
                    level="critical",
                    message="Sender bot not configured",
                )

            me = await self.bot_sender.get_me()
            bot_name = f"@{me.get('username', 'unknown')} (ID: {me.get('id')})"

            # Also verify relay channel permissions if configured
            relay_id = getattr(self.config.settings, "relay_channel_id", 0)
            if relay_id:
                if not await self.bot_sender.verify_bot_access(relay_id):
                    return HealthCheckResult(
                        name="sender_bot",
                        passed=False,
                        level="critical",
                        message=f"{bot_name} cannot access relay channel ({relay_id})",
                    )

            return HealthCheckResult(
                name="sender_bot",
                passed=True,
                level="critical",
                message=f"Bot {bot_name} connected & verified",
            )
        except Exception as e:
            return HealthCheckResult(
                name="sender_bot",
                passed=False,
                level="critical",
                message=f"getMe() / relay access check failed: {e}",
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
        """Warn if message queue (including overflow) is building up."""
        try:
            depth = await self.redis.llen("queue:messages")
            album_depth = await self.redis.llen("queue:albums")
            overflow_depth = await self.redis.llen("listener:overflow")
            total = depth + album_depth + overflow_depth
            passed = total < 100
            msg = f"Queue depth: {total} (messages={depth}, albums={album_depth}, overflow={overflow_depth})"
            return HealthCheckResult(
                name="queue_depth",
                passed=passed,
                level="high",
                message=msg,
            )
        except Exception as e:
            return HealthCheckResult(
                name="queue_depth", passed=False, level="high", message=f"Redis error: {e}"
            )

    async def _check_failed_queue(self) -> HealthCheckResult:
        """Check retry queue depth. Normal transient retried (<20) pass."""
        try:
            count = await self.redis.llen("queue:failed")
            # Transient retries awaiting scheduled worker attempts are normal operation (<20).
            # Only flag passed=False if retry backlog reaches severe levels (>=20).
            passed = count < 20
            msg = f"{count} messages awaiting retry" if count else ""
            if count >= 20:
                msg = f"Retry queue backlog: {count} messages pending retry (threshold=20)"
            return HealthCheckResult(
                name="failed_queue",
                passed=passed,
                level="medium",
                message=msg,
            )
        except Exception as e:
            return HealthCheckResult(
                name="failed_queue", passed=False, level="medium", message=f"Redis error: {e}"
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
                name="dead_letter_queue", passed=False, level="high", message=f"Redis error: {e}"
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
                name="disk_space", passed=False, level="high", message=f"Disk check error: {e}"
            )

    async def _check_queue_surge(self) -> HealthCheckResult:
        """
        Informational surge check for queue depth building above 50 messages.
        Returns passed=True to avoid false-positive component failures while workers drain the burst.
        """
        try:
            depth = await self.redis.llen("queue:messages")
            album_depth = await self.redis.llen("queue:albums")
            overflow_depth = await self.redis.llen("listener:overflow")
            total = depth + album_depth + overflow_depth
            msg = ""
            if total >= 50:
                msg = (
                    f"Queue surge active: {total} messages pending "
                    f"(messages={depth}, albums={album_depth}, overflow={overflow_depth}). Workers draining burst."
                )
            return HealthCheckResult(
                name="queue_surge",
                passed=True,
                level="high",
                message=msg,
            )
        except Exception as e:
            return HealthCheckResult(
                name="queue_surge", passed=False, level="high", message=f"Redis error: {e}"
            )

    async def _check_sustained_failure_rate(self) -> HealthCheckResult:
        """
        Real-time delivery failure rate check with reactive alerts.

        Queries the last 50 message delivery attempts from SQLite and calculates
        the failure percentage. Immediately sends an alert when:
          - Failure rate crosses warning (20%) or critical (50%) threshold
          - Failure rate recovers back below warning threshold (<20%)
          - Failure rate changes by 10% or more (increase or decrease)
        """
        settings = getattr(self.config, "settings", self.config)
        sample_size = getattr(settings, "failure_rate_sample_size", 50)
        min_samples = getattr(settings, "failure_rate_min_samples", 10)
        warning_thresh = getattr(settings, "failure_rate_warning_threshold", 0.20)
        critical_thresh = getattr(settings, "failure_rate_critical_threshold", 0.50)
        delta_thresh = getattr(settings, "failure_rate_delta_threshold", 0.10)

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
                    (sample_size,),
                )
                rows = await cursor.fetchall()

            if len(rows) < min_samples:
                return HealthCheckResult(
                    name="delivery_failure_rate",
                    passed=True,
                    level="high",
                    message=f"Not enough samples yet ({len(rows)}/{min_samples})",
                )

            failed = sum(1 for r in rows if r["status"] == "failed")
            rate = failed / len(rows)
            pct = rate * 100.0

            # Determine current failure severity state
            if rate >= critical_thresh:
                curr_state = "critical"
            elif rate >= warning_thresh:
                curr_state = "warning"
            else:
                curr_state = "normal"

            prev_rate = self._last_failure_rate
            prev_state = self._last_failure_state

            trigger_alert = False
            alert_msg = ""
            alert_emoji = "🟠"

            # 1. Recovery transition (degraded/critical -> normal)
            if prev_state in ("warning", "critical") and curr_state == "normal":
                trigger_alert = True
                alert_msg = (
                    f"🟢 <b>Delivery Failure Rate Recovered</b>\n\n"
                    f"<blockquote><b>Failure Rate:</b> <code>{pct:.1f}%</code> ({failed}/{len(rows)})\n"
                    f"<b>Status:</b> Recovered (Back below {warning_thresh * 100:.0f}% threshold)</blockquote>"
                )
            # 2. State transition into Warning or Critical
            elif prev_state == "normal" and curr_state in ("warning", "critical"):
                trigger_alert = True
                alert_emoji = "🔴" if curr_state == "critical" else "🟠"
                alert_msg = (
                    f"{alert_emoji} <b>Delivery Failure Rate Warning</b>\n\n"
                    f"<blockquote><b>Failure Rate:</b> <code>{pct:.1f}%</code> ({failed}/{len(rows)})\n"
                    f"<b>Threshold:</b> <code>{warning_thresh * 100:.0f}%</code>\n"
                    f"<b>Severity:</b> <code>{curr_state.upper()}</code></blockquote>"
                )
            elif prev_state == "warning" and curr_state == "critical":
                trigger_alert = True
                alert_msg = (
                    f"🔴 <b>CRITICAL: Delivery Failure Rate Spike</b>\n\n"
                    f"<blockquote><b>Failure Rate:</b> <code>{pct:.1f}%</code> ({failed}/{len(rows)})\n"
                    f"<b>Threshold:</b> <code>{critical_thresh * 100:.0f}%</code>\n"
                    f"<b>Severity:</b> CRITICAL</blockquote>"
                )
            # 3. Delta change trigger (>= 10% change increase or decrease)
            elif prev_rate is not None:
                delta = rate - prev_rate
                if abs(delta) >= delta_thresh:
                    trigger_alert = True
                    prev_pct = prev_rate * 100.0
                    if delta > 0:
                        alert_msg = (
                            f"📈 <b>Delivery Failure Rate Increased</b>\n\n"
                            f"<blockquote><b>Change:</b> <code>{prev_pct:.1f}% ➔ {pct:.1f}%</code> (+{delta * 100:.1f}%)\n"
                            f"<b>Recent Failures:</b> {failed}/{len(rows)}\n"
                            f"<b>Status:</b> <code>{curr_state.upper()}</code></blockquote>"
                        )
                    else:
                        alert_msg = (
                            f"📉 <b>Delivery Failure Rate Improved</b>\n\n"
                            f"<blockquote><b>Change:</b> <code>{prev_pct:.1f}% ➔ {pct:.1f}%</code> ({delta * 100:.1f}%)\n"
                            f"<b>Recent Failures:</b> {failed}/{len(rows)}\n"
                            f"<b>Status:</b> <code>{curr_state.upper()}</code></blockquote>"
                        )

            # Update tracked state
            self._last_failure_rate = rate
            self._last_failure_state = curr_state

            if trigger_alert:
                try:
                    await self._send_telegram_alert(alert_msg)
                except Exception as alert_err:
                    logger.error(
                        f"Failed to send reactive failure rate alert: {alert_err}"
                    )

            # Reactive failure rate monitor manages its own specific alerts (sent above on state/delta change).
            # Returns passed=True when query succeeds so operational service status remains clean.
            return HealthCheckResult(
                name="delivery_failure_rate",
                passed=True,
                level="high",
                message=f"{failed}/{len(rows)} recent deliveries failed ({pct:.1f}%) [state={curr_state.upper()}]",
            )
        except Exception as e:
            return HealthCheckResult(
                name="delivery_failure_rate",
                passed=False,
                level="high",
                message=f"Query error: {e}",
            )

    # ── Heartbeat ─────────────────────────────────────────────────────

    def _write_heartbeat(self):
        """Write current timestamp to heartbeat file for Docker HEALTHCHECK."""
        try:
            self.HEARTBEAT_FILE.parent.mkdir(parents=True, exist_ok=True)
            self.HEARTBEAT_FILE.write_text(str(time.time()))
        except Exception as e:
            logger.error(f"Failed to write heartbeat: {e}")

    # ── Alert delivery (State-Transition Based) ───────────────────────

    async def _process_state_transition_alerts(self, results: List[HealthCheckResult]):
        """
        Evaluate health check results and fire alerts ONLY on state transitions.
        
        - Fire ONCE when check transitions from Passed -> Failed (or first run failure)
        - Fire ONCE when check transitions from Failed -> Passed (Recovery)
        - Never repeat alerts every cycle / 15 min for persistent failures
        """
        for result in results:
            prev_passed = self._last_check_status.get(result.name)

            if not result.passed and (prev_passed is None or prev_passed is True):
                # State Transition: PASSED -> FAILED
                await self._send_failure_alert(result)
            elif result.passed and prev_passed is False:
                # State Transition: FAILED -> PASSED (Recovery)
                await self._send_recovery_alert(result)

            self._last_check_status[result.name] = result.passed

    async def _send_failure_alert(self, result: HealthCheckResult):
        """Send alert on health check failure transition."""
        level_emoji = {"critical": "🔴", "high": "🟠", "medium": "🟡"}.get(
            result.level, "⚪"
        )

        details_html = f"<code>{result.message}</code>" if result.message else "<i>No details provided.</i>"
        if result.message and len(result.message) > 100:
            import html
            details_html = f"<blockquote expandable><pre>{html.escape(result.message)}</pre></blockquote>"

        message = (
            f"{level_emoji} <b>ALERT: Health Check Failed</b>\n\n"
            f"<blockquote><b>Component:</b> <code>{result.name}</code>\n"
            f"<b>Status:</b> FAILED\n"
            f"<b>Severity:</b> <code>{result.level.upper()}</code></blockquote>\n"
            f"📝 <b>Details:</b>\n{details_html}\n\n"
            f"🕒 <i>{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}</i>"
        )

        try:
            await self._send_telegram_alert(message)
            logger.info(f"State-transition failure alert sent for {result.name}")
        except Exception as e:
            logger.error(f"Failed to send failure alert for {result.name}: {e}")

    async def _send_recovery_alert(self, result: HealthCheckResult):
        """Send alert on health check recovery transition."""
        message = (
            f"🟢 <b>RECOVERY: Component Restored</b>\n\n"
            f"<blockquote><b>Component:</b> <code>{result.name}</code>\n"
            f"<b>Status:</b> PASSED ✓</blockquote>\n"
            f"🕒 <i>{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}</i>"
        )

        try:
            await self._send_telegram_alert(message)
            logger.info(f"State-transition recovery alert sent for {result.name}")
        except Exception as e:
            logger.error(f"Failed to send recovery alert for {result.name}: {e}")

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
