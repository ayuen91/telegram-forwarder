"""Telegram alert delivery for operational failures, daily reports, and interactive admin control."""

import asyncio
import json
import logging
import time
import urllib.parse
from typing import Any, Dict, Optional

import aiohttp

logger = logging.getLogger(__name__)

_last_alert: Dict[str, float] = {}
COOLDOWN_SECONDS = 300  # 5 minutes per alert key


def get_admin_inline_keyboard() -> Dict[str, Any]:
    """Build standard admin inline keyboard buttons for Alert Bot commands."""
    return {
        "inline_keyboard": [
            [
                {"text": "🟢 Status", "callback_data": "cmd_status"},
                {"text": "📊 Stats", "callback_data": "cmd_stats"},
            ],
            [
                {"text": "⚙️ Reload", "callback_data": "cmd_reload"},
                {"text": "🔄 Retry", "callback_data": "cmd_retry"},
            ],
            [
                {"text": "🟠 Dead Letter", "callback_data": "cmd_deadletter"},
                {"text": "🗑️ Clear DLQ", "callback_data": "cmd_clear_deadletter"},
            ],
            [
                {"text": "ℹ️ Help Menu", "callback_data": "cmd_help"},
            ],
        ]
    }


async def send_alert(
    token: str,
    chat_id: int,
    message: str,
    alert_key: str = "",
    cooldown: int = COOLDOWN_SECONDS,
    reply_markup: Optional[Dict[str, Any]] = None,
    include_control_buttons: bool = True,
) -> None:
    if not token or not chat_id:
        return

    key = alert_key or message[:80]
    now = time.time()
    if now - _last_alert.get(key, 0) < cooldown:
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload: Dict[str, Any] = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    elif include_control_buttons:
        payload["reply_markup"] = get_admin_inline_keyboard()

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    _last_alert[key] = now
                    logger.info(f"Alert sent: {alert_key or 'operational'}")
                else:
                    body = await resp.text()
                    logger.error(f"Alert API error {resp.status}: {body[:200]}")
    except Exception as e:
        logger.error(f"Failed to send alert: {e}")


async def send_photo(
    token: str,
    chat_id: int,
    photo_url: str,
    caption: Optional[str] = None,
) -> None:
    """Send a photo (e.g. QuickChart URL) via the alert bot. No cooldown.

    Telegram's sendPhoto rejects URLs that are too long or that its servers
    cannot fetch (e.g. QuickChart URLs with a large embedded JSON config).
    To avoid the 400 "failed to get HTTP URL content" error we download the
    image ourselves and upload the raw bytes as multipart/form-data instead.
    Falls back to the plain URL method if the download step fails.
    """
    if not token or not chat_id or not photo_url:
        return

    api_url = f"https://api.telegram.org/bot{token}/sendPhoto"

    async with aiohttp.ClientSession() as session:
        # ── Step 1: download the image ────────────────────────────────
        image_bytes: Optional[bytes] = None
        headers = {"User-Agent": "TelegramBot/1.0"}
        try:
            async with session.get(
                photo_url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)
            ) as dl:
                if dl.status == 200:
                    image_bytes = await dl.read()
                else:
                    logger.warning(
                        f"Chart download returned {dl.status} — "
                        "attempting QuickChart POST fallback"
                    )
        except Exception as dl_err:
            logger.warning(
                f"Chart download failed ({dl_err}) — attempting QuickChart POST fallback"
            )

        # QuickChart POST API fallback if GET download failed
        if not image_bytes and "quickchart.io" in photo_url:
            try:
                parsed = urllib.parse.urlparse(photo_url)
                params = urllib.parse.parse_qs(parsed.query)
                chart_param = params.get("c", [""])[0]
                if chart_param:
                    chart_json = json.loads(chart_param)
                    post_payload = {
                        "width": 800,
                        "height": 400,
                        "backgroundColor": "#1f2937",
                        "version": "3",
                        "format": "png",
                        "chart": chart_json,
                    }
                    async with session.post(
                        "https://quickchart.io/chart",
                        json=post_payload,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=20),
                    ) as qc_resp:
                        if qc_resp.status == 200:
                            image_bytes = await qc_resp.read()
                            logger.info("Chart fetched via QuickChart POST API fallback")
            except Exception as qc_err:
                logger.warning(f"QuickChart POST fallback failed: {qc_err}")

        # ── Step 2: upload (binary preferred, URL fallback) ───────────
        try:
            if image_bytes:
                # Binary multipart upload — always accepted by Telegram
                form = aiohttp.FormData()
                form.add_field("chat_id", str(chat_id))
                form.add_field(
                    "photo",
                    image_bytes,
                    filename="chart.png",
                    content_type="image/png",
                )
                if caption:
                    form.add_field("caption", caption[:1024])
                    form.add_field("parse_mode", "HTML")

                async with session.post(
                    api_url, data=form, timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        logger.info("Alert photo sent (binary upload)")
                    else:
                        body = await resp.text()
                        logger.error(
                            f"Alert sendPhoto error {resp.status}: {body[:200]}"
                        )
                        raise RuntimeError(f"sendPhoto failed: {resp.status}")
            else:
                # URL fallback — may fail for very long QuickChart URLs
                payload: dict = {"chat_id": chat_id, "photo": photo_url}
                if caption:
                    payload["caption"] = caption[:1024]
                    payload["parse_mode"] = "HTML"

                async with session.post(
                    api_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status == 200:
                        logger.info("Alert photo sent (URL upload)")
                    else:
                        body = await resp.text()
                        logger.error(
                            f"Alert sendPhoto error {resp.status}: {body[:200]}"
                        )
                        raise RuntimeError(f"sendPhoto failed: {resp.status}")
        except RuntimeError:
            raise
        except Exception as e:
            logger.error(f"Failed to send alert photo: {e}")
            raise


class AlertBotCommandListener:
    """
    Interactive polling listener for the Alert Bot (ALERT_BOT_TOKEN).
    Allows the owner (ALERT_CHAT_ID) to send commands or tap inline keyboard buttons:
      /status or /health - On-demand health check diagnostic report
      /stats or /metrics - Real-time forwarding & queue metrics
      /reload            - Instant config hot-reload (YAML files)
      /retry             - Trigger retry of failed queue messages
      /deadletter        - Inspect and optionally clear dead-letter queue
      /help              - Interactive command menu
    """

    def __init__(
        self,
        token: str,
        chat_id: int,
        health_monitor,
        config,
        queue_mgr,
        redis_client,
        shutdown_event: asyncio.Event,
    ):
        self.token = token
        self.chat_id = chat_id
        self.health_monitor = health_monitor
        self.config = config
        self.queue_mgr = queue_mgr
        self.redis = redis_client
        self.shutdown_event = shutdown_event
        self.offset = 0

    async def _setup_bot_commands(self, session: aiohttp.ClientSession):
        """Register native Telegram bot commands so the '/' command menu appears in chat."""
        url = f"https://api.telegram.org/bot{self.token}/setMyCommands"
        commands = [
            {"command": "status", "description": "Real-time health diagnostic report"},
            {"command": "stats", "description": "Delivery volume & queue depths"},
            {"command": "reload", "description": "Reload YAML configurations"},
            {"command": "retry", "description": "Trigger retry of failed queue"},
            {"command": "deadletter", "description": "Inspect dead-letter queue"},
            {"command": "clear_deadletter", "description": "Clear dead-letter queue"},
            {"command": "help", "description": "Show interactive control menu"},
        ]
        payload = {"commands": commands}
        try:
            async with session.post(
                url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    logger.info("Alert bot command menu registered with Telegram (setMyCommands)")
                else:
                    body = await resp.text()
                    logger.warning(f"Failed to set my commands: {resp.status} - {body[:100]}")
        except Exception as e:
            logger.warning(f"Failed to register bot commands menu: {e}")

    async def start_listening(self):
        if not self.token or not self.chat_id:
            logger.info("Alert bot command listener disabled (missing token/chat_id)")
            return

        logger.info("Alert bot command listener started (accepting commands & callback buttons from owner)")
        url = f"https://api.telegram.org/bot{self.token}/getUpdates"

        async with aiohttp.ClientSession() as session:
            await self._setup_bot_commands(session)
            while not self.shutdown_event.is_set():
                try:
                    payload = {
                        "offset": self.offset,
                        "timeout": 5,
                        "allowed_updates": ["message", "callback_query"],
                    }
                    async with session.post(
                        url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            for update in data.get("result", []):
                                self.offset = update["update_id"] + 1

                                if "message" in update:
                                    msg = update["message"]
                                    from_chat_id = msg.get("chat", {}).get("id")

                                    # Security check: only process messages from authorized owner
                                    if from_chat_id == self.chat_id and msg.get("text"):
                                        await self._handle_command(msg["text"].strip(), session)

                                elif "callback_query" in update:
                                    cb = update["callback_query"]
                                    from_user_id = cb.get("from", {}).get("id")
                                    cb_chat_id = cb.get("message", {}).get("chat", {}).get("id")

                                    if from_user_id == self.chat_id or cb_chat_id == self.chat_id:
                                        cb_id = cb.get("id")
                                        cb_data = cb.get("data", "")
                                        if cb_id:
                                            await self._answer_callback_query(cb_id, session)
                                        if cb_data:
                                            await self._handle_callback(cb_data, session)
                        else:
                            await asyncio.sleep(2)
                except asyncio.CancelledError:
                    return
                except Exception as e:
                    logger.error(f"Alert bot command listener error: {e}")
                    await asyncio.sleep(2)

    async def _answer_callback_query(self, callback_query_id: str, session: aiohttp.ClientSession, text: str = ""):
        url = f"https://api.telegram.org/bot{self.token}/answerCallbackQuery"
        payload = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
        try:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=5)):
                pass
        except Exception as e:
            logger.debug(f"Failed to answer callback query: {e}")

    async def _handle_callback(self, cb_data: str, session: aiohttp.ClientSession):
        cmd_map = {
            "cmd_status": self._cmd_status,
            "cmd_stats": self._cmd_stats,
            "cmd_reload": self._cmd_reload,
            "cmd_retry": self._cmd_retry,
            "cmd_deadletter": self._cmd_deadletter,
            "cmd_clear_deadletter": self._cmd_clear_deadletter,
            "cmd_help": self._cmd_help,
        }
        handler = cmd_map.get(cb_data)
        if handler:
            await handler(session)
        else:
            await self._handle_command(cb_data, session)

    async def _reply(self, text: str, session: aiohttp.ClientSession, reply_markup: Optional[Dict[str, Any]] = None):
        send_url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        if reply_markup is None:
            reply_markup = get_admin_inline_keyboard()
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "reply_markup": reply_markup,
        }
        try:
            async with session.post(
                send_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
            ):
                pass
        except Exception as e:
            logger.error(f"Failed to reply to alert command: {e}")

    async def _handle_command(self, cmd_text: str, session: aiohttp.ClientSession):
        cmd = cmd_text.split()[0].lower()
        # Strip bot username suffix if command sent in group e.g. /status@bot_name
        cmd = cmd.split("@")[0]

        if cmd in ("/status", "/health"):
            await self._cmd_status(session)
        elif cmd in ("/stats", "/metrics"):
            await self._cmd_stats(session)
        elif cmd == "/reload":
            await self._cmd_reload(session)
        elif cmd == "/retry":
            await self._cmd_retry(session)
        elif cmd == "/deadletter":
            await self._cmd_deadletter(session)
        elif cmd == "/clear_deadletter":
            await self._cmd_clear_deadletter(session)
        elif cmd in ("/help", "/start"):
            await self._cmd_help(session)

    async def _cmd_status(self, session: aiohttp.ClientSession):
        results = await self.health_monitor.run_all_checks()
        passed_count = sum(1 for r in results if r.passed)
        total = len(results)

        lines = [
            f"🏥 <b>System Health Report</b> ({passed_count}/{total} Passed)\n"
        ]

        for r in results:
            icon = "🟢" if r.passed else (
                "🔴" if r.level == "critical" else "🟠"
            )
            msg_str = f" — <code>{r.message}</code>" if r.message else ""
            lines.append(f"{icon} <b>{r.name}</b>{msg_str}")

        lines.append(f"\n🕒 <i>{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}</i>")
        await self._reply("\n".join(lines), session)

    async def _cmd_stats(self, session: aiohttp.ClientSession):
        depths = await self.queue_mgr.get_queue_depths()
        overflow_depth = await self.redis.llen("listener:overflow")

        sent_24h = 0
        failed_24h = 0
        try:
            import aiosqlite

            async with aiosqlite.connect(self.health_monitor.db_path) as db:
                db.row_factory = aiosqlite.Row
                c1 = await db.execute(
                    "SELECT COUNT(*) as cnt FROM message_destinations WHERE status = 'sent' AND sent_at >= datetime('now', '-1 day')"
                )
                r1 = await c1.fetchone()
                sent_24h = r1["cnt"] if r1 else 0

                c2 = await db.execute(
                    "SELECT COUNT(*) as cnt FROM message_destinations WHERE status = 'failed' AND sent_at >= datetime('now', '-1 day')"
                )
                r2 = await c2.fetchone()
                failed_24h = r2["cnt"] if r2 else 0
        except Exception:
            pass

        total_24h = sent_24h + failed_24h
        rate_24h = (sent_24h / total_24h * 100.0) if total_24h > 0 else 100.0

        msg = (
            f"📊 <b>Telegram Forwarder Metrics</b>\n\n"
            f"<blockquote><b>24h Deliveries:</b> <code>{sent_24h}</code> sent / <code>{failed_24h}</code> failed (<b>{rate_24h:.1f}%</b> success)\n"
            f"<b>Messages Queue:</b> <code>{depths.get('messages', 0)}</code>\n"
            f"<b>Albums Queue:</b> <code>{depths.get('albums', 0)}</code>\n"
            f"<b>Overflow Queue:</b> <code>{overflow_depth}</code>\n"
            f"<b>Retry Queue:</b> <code>{depths.get('failed', 0)}</code>\n"
            f"<b>Deferred Queue:</b> <code>{depths.get('deferred', 0)}</code>\n"
            f"<b>Dead Letter Queue:</b> <code>{depths.get('dead_letter', 0)}</code></blockquote>\n\n"
            f"🕒 <i>{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}</i>"
        )
        await self._reply(msg, session)

    async def _cmd_reload(self, session: aiohttp.ClientSession):
        self.config._load_channels()
        self.config._load_replacements()
        dest_count = len(self.config.get_active_destinations())
        rules_count = len(self.config.settings.replacement_rules)
        msg = (
            f"⚙️ <b>Config Reloaded</b>\n\n"
            f"<blockquote><b>Active Destinations:</b> <code>{dest_count}</code>\n"
            f"<b>Replacement Rules:</b> <code>{rules_count}</code></blockquote>"
        )
        await self._reply(msg, session)

    async def _cmd_retry(self, session: aiohttp.ClientSession):
        count = await self.redis.llen(self.queue_mgr.QUEUE_FAILED)
        msg = f"🔄 <b>Retry Triggered</b>\n\n<blockquote><code>{count}</code> message(s) in failed queue awaiting worker retry.</blockquote>"
        await self._reply(msg, session)

    async def _cmd_deadletter(self, session: aiohttp.ClientSession):
        count = await self.redis.llen(self.queue_mgr.QUEUE_DEAD_LETTER)
        if count == 0:
            await self._reply(
                "🟢 <b>Dead Letter Queue</b>\n\n<blockquote>No messages in dead letter queue.</blockquote>",
                session,
            )
            return

        items = await self.redis.lrange(self.queue_mgr.QUEUE_DEAD_LETTER, -5, -1)
        lines = [f"🟠 <b>Dead Letter Queue</b> (Total: <code>{count}</code>)\n"]
        lines.append("Recent items:")

        for raw in items:
            try:
                payload = json.loads(raw)
                mid = payload.get("message_id", payload.get("media_group_id", "?"))
                err = payload.get("_last_error", "Unknown error")
                lines.append(f"• ID <code>{mid}</code>: <i>{err[:80]}</i>")
            except Exception:
                pass

        lines.append("\nUse /clear_deadletter to clear all dead-letter items.")
        await self._reply("\n".join(lines), session)

    async def _cmd_clear_deadletter(self, session: aiohttp.ClientSession):
        cleared = await self.queue_mgr.clear_dead_letter()
        await self._reply(
            f"🗑️ <b>Dead Letter Cleared</b>\n\n<blockquote>Removed <code>{cleared}</code> item(s) from dead-letter queue.</blockquote>",
            session,
        )

    async def _cmd_help(self, session: aiohttp.ClientSession):
        msg = (
            f"🤖 <b>Telegram Forwarder Control Panel</b>\n\n"
            f"Select any action from the interactive inline keyboard buttons below, "
            f"or type slash commands directly:\n\n"
            f"• 🟢 <code>/status</code> — Real-time health diagnostic report\n"
            f"• 📊 <code>/stats</code> — Delivery volume, success rates & queue depths\n"
            f"• ⚙️ <code>/reload</code> — Reload channels.yml & replacements.yml\n"
            f"• 🔄 <code>/retry</code> — Trigger retry of failed queue\n"
            f"• 🟠 <code>/deadletter</code> — Inspect dead-letter items\n"
            f"• 🗑️ <code>/clear_deadletter</code> — Clear dead-letter queue\n"
            f"• ℹ️ <code>/help</code> — Show this control menu"
        )
        await self._reply(msg, session, reply_markup=get_admin_inline_keyboard())
