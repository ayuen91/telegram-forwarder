"""
Main entry point for the Telegram Forwarder bot.

Pipeline: Hydrogram listen → n8n word replace → Bot API send (via relay for media)

Launched tasks:
  - message_worker × N   — queue → webhook → forward
  - album_flush_worker   — flush expired album buffers
  - retry_worker         — retry failed/deferred messages
  - health_checker       — periodic health checks + alerts
  - daily_report         — scheduled visual health digest (08:00 UTC+3)
"""

import asyncio
import logging
import os
import signal
import sys
from dataclasses import dataclass
from pathlib import Path

# Monkey patch Hydrogram to support 64-bit channel IDs (like -1002...)
from hydrogram import utils

def get_peer_type_new(peer_id: int) -> str:
    peer_id_str = str(peer_id)
    if not peer_id_str.startswith("-"):
        return "user"
    elif peer_id_str.startswith("-100"):
        return "channel"
    else:
        return "chat"

utils.get_peer_type = get_peer_type_new

import aiosqlite
import redis.asyncio as aioredis
from hydrogram import Client
from hydrogram.errors import (
    FloodWait,
    AuthKeyUnregistered,
    SessionRevoked,
    UserDeactivated,
)

from config import config
from logging_config import setup_logging
from listener import register_listener, message_worker, process_payload, retry_payload
from album_buffer import AlbumBuffer
from queue_manager import QueueManager
from deduplication import Deduplication
from webhook import WebhookSender
from health import HealthMonitor
from media_relay import RelayConfig, ensure_relay_chat, resolve_relay_config
from telegram_sender import TelegramBotSender, TelegramFloodWait
from scheduler import make_daily_report_factory

logger = logging.getLogger(__name__)

shutdown_event = asyncio.Event()
health_monitor: HealthMonitor = None


@dataclass
class WorkerContext:
    """Shared dependencies passed to all workers."""

    sender: TelegramBotSender
    webhook: WebhookSender
    queue_mgr: QueueManager
    dedup: Deduplication
    album_buf: AlbumBuffer
    pyrogram_app: Client
    relay: RelayConfig
    config: object
    db_path: str
    delay_min: float
    delay_max: float
    alert_token: str
    alert_chat_id: int


async def supervised_task(name: str, coro_factory, restart_delay: float = 5.0):
    """Run a coroutine forever with auto-restart and FloodWait handling."""
    current_delay = restart_delay

    while not shutdown_event.is_set():
        try:
            logger.info(f"Task '{name}' starting")
            current_delay = restart_delay
            await coro_factory()
        except FloodWait as e:
            wait = e.value + 1
            logger.warning(f"Task '{name}' hit FloodWait({e.value}s), sleeping {wait}s")
            try:
                await health_monitor._send_telegram_alert(
                    f"⏳ <b>FloodWait Encountered</b>\n\n"
                    f"<blockquote><b>Task:</b> <code>{name}</code>\n"
                    f"<b>Source:</b> MTProto (Hydrogram)\n"
                    f"<b>Duration:</b> <tg-spoiler>{e.value}s</tg-spoiler></blockquote>\n"
                    f"🕒 <i>System is pausing and will auto-resume.</i>"
                )
            except Exception:
                pass
            await asyncio.sleep(wait)
        except TelegramFloodWait as e:
            wait = e.retry_after + 1
            logger.warning(f"Task '{name}' hit Bot API FloodWait({e.retry_after}s), sleeping {wait}s")
            try:
                await health_monitor._send_telegram_alert(
                    f"⏳ <b>FloodWait Encountered</b>\n\n"
                    f"<blockquote><b>Task:</b> <code>{name}</code>\n"
                    f"<b>Source:</b> Bot API (Telegram)\n"
                    f"<b>Duration:</b> <tg-spoiler>{e.retry_after}s</tg-spoiler></blockquote>\n"
                    f"🕒 <i>System is pausing and will auto-resume.</i>"
                )
            except Exception:
                pass
            await asyncio.sleep(wait)
        except asyncio.CancelledError:
            logger.info(f"Task '{name}' cancelled — shutting down")
            return
        except (AuthKeyUnregistered, SessionRevoked, UserDeactivated) as e:
            logger.critical(f"Task '{name}' hit fatal auth error: {e}")
            try:
                await health_monitor._send_telegram_alert(
                    f"🔴 <b>FATAL: Userbot Authentication Failed</b>\n\n"
                    f"<blockquote><b>Task:</b> <code>{name}</code>\n"
                    f"<b>Error:</b> <code>{e.__class__.__name__}</code>\n"
                    f"<b>Details:</b> {e}</blockquote>\n"
                    f"💡 <b>Required Action:</b> Re-authenticate Hydrogram session."
                )
            except Exception:
                pass
            await asyncio.sleep(300)
        except Exception as e:
            logger.error(f"Task '{name}' crashed: {e}", exc_info=True)
            try:
                import traceback
                import html
                tb_str = traceback.format_exc()
                tb_escaped = html.escape(tb_str)
                await health_monitor._send_telegram_alert(
                    f"🟠 <b>Task Crashed: {name}</b>\n\n"
                    f"<blockquote><b>Error:</b> <code>{e.__class__.__name__}</code>\n"
                    f"<b>Message:</b> {e}</blockquote>\n"
                    f"📝 <b>Traceback:</b>\n"
                    f"<blockquote expandable><pre>{tb_escaped}</pre></blockquote>\n"
                    f"🔄 Auto-restarting in <code>{current_delay:.0f}s</code>"
                )
            except Exception:
                pass
            await asyncio.sleep(current_delay)
            current_delay = min(current_delay * 2, 300)


async def init_database(db_path: str):
    """Apply schema.sql on first run."""
    schema_path = Path("/app/db/schema.sql")
    if not schema_path.exists():
        logger.warning(f"schema.sql not found at {schema_path}")
        return

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(db_path) as db:
        await db.executescript(schema_path.read_text())
        await db.commit()
        logger.info(f"Database initialized at {db_path}")


async def ensure_source_channel_membership(app: Client, source_chat_id: int) -> bool:
    """
    Ensure the userbot is a joined member of the source channel.

    The Hydrogram MTProto client only receives UpdateNewChannelMessage push
    events for channels it is actively subscribed/joined to. For public
    channels the userbot can *read* without joining — but it will NOT receive
    new-message updates until it joins.

    Returns True if already a member or joined successfully, False on failure.
    """
    try:
        chat = await app.get_chat(source_chat_id)
        # get_chat_member raises an error if not a member; catching it lets us
        # decide whether to join.
        try:
            member = await app.get_chat_member(source_chat_id, "me")
            status = str(getattr(member, "status", "")).lower()
            if "left" in status or "banned" in status or "kicked" in status:
                raise Exception(f"Status is '{status}'")
            logger.info(
                f"Userbot is already a member of source channel '{chat.title}' "
                f"(status={status})"
            )
            return True
        except Exception:
            # Not a member — attempt to join (works for public channels)
            logger.info(
                f"Userbot is not a member of '{chat.title}' — joining now "
                f"so push updates are received..."
            )
            await app.join_chat(source_chat_id)
            logger.info(f"Joined source channel '{chat.title}' successfully")
            return True
    except Exception as e:
        logger.error(
            f"Could not verify/join source channel {source_chat_id}: {e}. "
            "For private channels the userbot must already be a member."
        )
        return False


async def wait_for_critical_checks(monitor: HealthMonitor, max_attempts: int = 10) -> bool:
    """Retry only critical health checks (hydrogram, redis, sender bot, sqlite)."""
    for attempt in range(1, max_attempts + 1):
        if await monitor.startup_self_test():
            return True
        if attempt >= max_attempts:
            break
        logger.warning(f"Critical checks failed, retrying in 15s ({attempt}/{max_attempts})...")
        await asyncio.sleep(15)
    return False


def make_worker_factory(worker_id: int, queue: asyncio.Queue, ctx: WorkerContext):
    async def worker():
        await message_worker(
            worker_id=worker_id,
            queue=queue,
            dedup=ctx.dedup,
            album_buffer=ctx.album_buf,
            queue_manager=ctx.queue_mgr,
            webhook_sender=ctx.webhook,
            sender=ctx.sender,
            db_path=ctx.db_path,
            config=ctx.config,
            pyrogram_app=ctx.pyrogram_app,
            relay=ctx.relay,
            alert_token=ctx.alert_token,
            alert_chat_id=ctx.alert_chat_id,
            delay_min=ctx.delay_min,
            delay_max=ctx.delay_max,
        )
    return worker


def make_album_flush_factory(ctx: WorkerContext):
    async def album_flush():
        while not shutdown_event.is_set():
            try:
                for album_payload in await ctx.album_buf.check_and_flush_expired():
                    await process_payload(
                        ctx.sender, ctx.queue_mgr, ctx.webhook, album_payload,
                        ctx.db_path, ctx.config, dedup=ctx.dedup,
                        pyrogram_app=ctx.pyrogram_app, relay=ctx.relay,
                        alert_token=ctx.alert_token, alert_chat_id=ctx.alert_chat_id,
                    )
            except Exception as e:
                logger.error(f"Album flush error: {e}", exc_info=True)
            await asyncio.sleep(0.5)
    return album_flush


def make_retry_factory(ctx: WorkerContext):
    async def retry():
        while not shutdown_event.is_set():
            try:
                payload = await ctx.queue_mgr.dequeue_deferred()
                if payload:
                    await retry_payload(
                        ctx.sender, ctx.queue_mgr, ctx.webhook, payload,
                        ctx.db_path, ctx.config, dedup=ctx.dedup,
                        pyrogram_app=ctx.pyrogram_app, relay=ctx.relay,
                        alert_token=ctx.alert_token, alert_chat_id=ctx.alert_chat_id,
                    )
                    await asyncio.sleep(1)
                    continue

                payload = await ctx.queue_mgr.dequeue_failed()
                if payload:
                    await retry_payload(
                        ctx.sender, ctx.queue_mgr, ctx.webhook, payload,
                        ctx.db_path, ctx.config, dedup=ctx.dedup,
                        pyrogram_app=ctx.pyrogram_app, relay=ctx.relay,
                        alert_token=ctx.alert_token, alert_chat_id=ctx.alert_chat_id,
                    )
                else:
                    await asyncio.sleep(60)
                    continue
            except Exception as e:
                logger.error(f"Retry worker error: {e}", exc_info=True)

            await asyncio.sleep(2)
    return retry


def make_health_factory(monitor: HealthMonitor, interval: int):
    async def health():
        while not shutdown_event.is_set():
            try:
                await monitor.run_cycle()
            except Exception as e:
                logger.error(f"Health check error: {e}", exc_info=True)
            await asyncio.sleep(interval)
    return health


def make_channel_keepalive_factory(
    app: Client,
    source_chat_id: int,
    interval: int,
):
    """
    Periodically calls get_chat() on the source channel to keep the MTProto
    session 'warm'. For large/popular channels Telegram can deprioritise update
    delivery to sessions it considers inactive; a lightweight read every N
    minutes is enough to prevent that without risking any rate-limit flags.

    Equivalent to the Telegram app's background channel refresh — one read
    RPC call, no write, no join, no history fetch.
    """
    async def keepalive():
        while not shutdown_event.is_set():
            await asyncio.sleep(interval)
            if shutdown_event.is_set():
                break
            try:
                chat = await app.get_chat(source_chat_id)
                logger.debug(
                    f"Keepalive ping: source channel '{chat.title}' is reachable"
                )
            except Exception as e:
                logger.warning(f"Keepalive get_chat failed: {e}")
    return keepalive


def make_silence_watchdog_factory(
    app: Client,
    source_chat_id: int,
    redis_client,
    silence_timeout: int,
):
    """
    Watches the Redis liveness key written by the on_message handler.
    If no message has arrived from the source channel for `silence_timeout`
    seconds (and the channel has previously been active), the watchdog assumes
    the MTProto update stream has silently stalled and performs a lightweight
    recovery:
      1. Re-resolve the peer (refreshes the session's access-hash cache).
      2. Re-join the channel (re-subscribes to UpdateNewChannelMessage).

    Checks every 5 minutes. Does NOT call get_chat_history — that is handled
    by the UpdatesTooLong raw-update handler in listener.py.
    Uses exponential back-off between consecutive reconnect attempts so it
    can never produce a burst of API calls.
    """
    from listener import LISTENER_LAST_RECEIVED_KEY
    CHECK_INTERVAL = 300  # check every 5 minutes
    _last_reconnect: float = 0.0
    _reconnect_backoff: float = 60.0   # start at 1 min, doubles each time
    _MAX_BACKOFF: float = 3600.0       # cap at 1 hour

    async def watchdog():
        nonlocal _last_reconnect, _reconnect_backoff

        if silence_timeout <= 0:
            logger.info("Silence watchdog disabled (LISTENER_SILENCE_TIMEOUT=0)")
            return

        while not shutdown_event.is_set():
            await asyncio.sleep(CHECK_INTERVAL)
            if shutdown_event.is_set():
                break

            try:
                last_ts_raw = await redis_client.get(LISTENER_LAST_RECEIVED_KEY)
                if last_ts_raw is None:
                    # Key not set yet — channel may not have posted anything;
                    # don't trigger a false reconnect on a fresh session.
                    continue

                import time
                last_ts = float(last_ts_raw)
                elapsed = time.time() - last_ts

                if elapsed < silence_timeout:
                    # Messages flowing normally — reset back-off
                    _reconnect_backoff = 60.0
                    continue

                # Silence detected — respect back-off between reconnects
                now = time.monotonic()
                if now - _last_reconnect < _reconnect_backoff:
                    continue

                logger.warning(
                    f"Silence watchdog: no message received for {elapsed:.0f}s "
                    f"(threshold={silence_timeout}s). Attempting recovery."
                )
                _last_reconnect = now

                try:
                    # Step 1 — re-resolve peer to refresh access-hash cache
                    await app.resolve_peer(source_chat_id)
                    logger.info("Silence watchdog: peer re-resolved")
                except Exception as rp_err:
                    logger.warning(f"Silence watchdog: resolve_peer failed: {rp_err}")

                try:
                    # Step 2 — re-join to resubscribe to update delivery
                    await app.join_chat(source_chat_id)
                    logger.info("Silence watchdog: channel re-joined")
                except Exception as join_err:
                    # Likely already a member — that's fine
                    logger.debug(f"Silence watchdog: join_chat: {join_err}")

                # Back off exponentially so repeated failures don't loop fast
                _reconnect_backoff = min(_reconnect_backoff * 2, _MAX_BACKOFF)
                logger.info(
                    f"Silence watchdog: recovery done. "
                    f"Next attempt no sooner than {_reconnect_backoff:.0f}s."
                )

            except Exception as e:
                logger.error(f"Silence watchdog error: {e}", exc_info=True)

    return watchdog


async def main():
    global health_monitor

    settings = config.settings
    setup_logging(settings.log_level)
    logger.info("=" * 60)
    logger.info("Telegram Forwarder starting...")
    logger.info("=" * 60)

    errors = config.validate()
    if errors:
        for err in errors:
            logger.error(f"Config error: {err}")
        sys.exit(1)

    await init_database(settings.db_path)

    redis_client = aioredis.from_url(
        settings.redis_url, decode_responses=True, retry_on_timeout=True,
    )

    dedup = Deduplication(redis_client)
    queue_mgr = QueueManager(redis_client, max_retries=settings.max_retries)
    album_buf = AlbumBuffer(redis_client, buffer_seconds=settings.album_buffer_seconds)
    webhook = WebhookSender(
        message_url=settings.n8n_webhook_message,
        album_url=settings.n8n_webhook_album,
        secret=settings.webhook_secret,
        config=config,
    )
    sender = TelegramBotSender(bot_token=settings.bot_token)

    app = Client(
        name="user_session",
        api_id=settings.api_id,
        api_hash=settings.api_hash,
        phone_number=settings.phone_number,
        workdir=os.getenv("HYDROGRAM_WORKDIR", os.getenv("PYROGRAM_WORKDIR", "/app/sessions")),
    )

    health_monitor = HealthMonitor(
        redis_client=redis_client,
        webhook_sender=webhook,
        config=config,
        pyrogram_app=app,
        bot_sender=sender,
        alert_bot_token=settings.alert_bot_token,
        alert_chat_id=settings.alert_chat_id,
        db_path=settings.db_path,
    )

    message_queue = asyncio.Queue(maxsize=100)
    register_listener(app, settings.source_chat_id, message_queue, redis_client=redis_client)
    stale_albums = await album_buf.flush_stale_albums()

    # ── Connect and verify ────────────────────────────────────────────
    logger.info("Connecting Hydrogram userbot...")
    await app.start()

    me = await app.get_me()
    logger.info(f"Hydrogram connected as {me.first_name} (id={me.id})")

    try:
        bot_me = await sender.get_me()
        logger.info(f"Sender bot: @{bot_me.get('username', 'unknown')}")
    except Exception as e:
        logger.error(f"Sender bot verification failed: {e}")

    relay = await resolve_relay_config(
        sender, me.id, relay_channel_id=settings.relay_channel_id,
    )

    try:
        source_chat = await app.get_chat(settings.source_chat_id)
        logger.info(f"Source channel: '{source_chat.title}'")
    except Exception as e:
        logger.error(f"Cannot access source channel {settings.source_chat_id}: {e}")

    # Force-refresh the peer access hash in the session cache.
    # After a Pyrogram→Hydrogram migration (or any long downtime), the cached
    # access hash for the source channel can be stale. Hydrogram then fails
    # silently to receive UpdateNewChannelMessage events — the MTProto connection
    # is healthy but Telegram doesn't push updates because the peer isn't
    # correctly resolved. resolve_peer() triggers a fresh GetChannels RPC call
    # and updates the local session's peer cache.
    try:
        peer = await app.resolve_peer(settings.source_chat_id)
        logger.info(
            f"Source channel peer resolved: type={type(peer).__name__} "
            f"id={getattr(peer, 'channel_id', getattr(peer, 'chat_id', '?'))}"
        )
    except Exception as e:
        logger.warning(
            f"Could not resolve source channel peer {settings.source_chat_id}: {e}. "
            "Updates may not flow if the session cache is stale."
        )

    # Ensure the userbot is a member so UpdateNewChannelMessage events are pushed.
    # This is required for public channels — Hydrogram won't receive updates
    # for channels the userbot has not joined.
    await ensure_source_channel_membership(app, settings.source_chat_id)

    relay_ok = await ensure_relay_chat(app, sender, relay)
    if not relay_ok:
        logger.warning("Media forwarding disabled until relay chat is accessible (text still works)")

    for err in await sender.verify_destinations(
        [{"chat_id": d.chat_id, "name": d.name, "enabled": d.enabled}
         for d in config.get_active_destinations()]
    ):
        logger.error(err)

    # Only critical failures block startup (dead letter queue does NOT)
    if not await wait_for_critical_checks(health_monitor):
        logger.critical("Critical startup checks failed — exiting")
        await app.stop()
        sys.exit(1)

    ctx = WorkerContext(
        sender=sender,
        webhook=webhook,
        queue_mgr=queue_mgr,
        dedup=dedup,
        album_buf=album_buf,
        pyrogram_app=app,
        relay=relay,
        config=config,
        db_path=settings.db_path,
        delay_min=settings.worker_delay_min,
        delay_max=settings.worker_delay_max,
        alert_token=settings.alert_bot_token,
        alert_chat_id=settings.alert_chat_id,
    )

    logger.info("Startup complete — launching workers")

    for album_payload in stale_albums:
        try:
            await process_payload(
                ctx.sender, ctx.queue_mgr, ctx.webhook, album_payload,
                ctx.db_path, ctx.config, dedup=ctx.dedup,
                pyrogram_app=ctx.pyrogram_app, relay=ctx.relay,
                alert_token=ctx.alert_token, alert_chat_id=ctx.alert_chat_id,
            )
        except Exception as e:
            logger.error(f"Failed to process recovered album: {e}", exc_info=True)

    tasks = []
    for i in range(settings.worker_count):
        tasks.append(asyncio.create_task(
            supervised_task(f"worker-{i+1}", make_worker_factory(i + 1, message_queue, ctx)),
            name=f"worker-{i+1}",
        ))

    tasks.append(asyncio.create_task(
        supervised_task("album-flush", make_album_flush_factory(ctx)),
        name="album-flush",
    ))
    tasks.append(asyncio.create_task(
        supervised_task("retry", make_retry_factory(ctx)),
        name="retry",
    ))
    tasks.append(asyncio.create_task(
        supervised_task("health", make_health_factory(health_monitor, settings.health_check_interval)),
        name="health",
    ))

    if settings.daily_report_enabled:
        tasks.append(asyncio.create_task(
            supervised_task(
                "daily-report",
                make_daily_report_factory(
                    health_monitor=health_monitor,
                    queue_mgr=queue_mgr,
                    redis_client=redis_client,
                    config=config,
                    db_path=settings.db_path,
                    alert_token=settings.alert_bot_token,
                    alert_chat_id=settings.alert_chat_id,
                    hour=settings.daily_report_hour,
                    tz_name=settings.daily_report_timezone,
                    run_on_start=settings.daily_report_run_on_start,
                    shutdown_event=shutdown_event,
                ),
            ),
            name="daily-report",
        ))
        logger.info(
            f"Daily report enabled: {settings.daily_report_hour:02d}:00 "
            f"{settings.daily_report_timezone}"
        )

    # ── Channel keep-alive ────────────────────────────────────────────
    # Calls get_chat() every LISTENER_KEEPALIVE_INTERVAL seconds to keep
    # the MTProto session warm. Prevents Telegram from deprioritising update
    # delivery to "inactive" sessions on large/popular channels.
    tasks.append(asyncio.create_task(
        supervised_task(
            "channel-keepalive",
            make_channel_keepalive_factory(
                app=app,
                source_chat_id=settings.source_chat_id,
                interval=settings.listener_keepalive_interval,
            ),
        ),
        name="channel-keepalive",
    ))

    # ── Silence watchdog ──────────────────────────────────────────────
    # Triggers a peer re-resolve + re-join when no message has arrived
    # for LISTENER_SILENCE_TIMEOUT seconds. Disabled when timeout is 0.
    tasks.append(asyncio.create_task(
        supervised_task(
            "silence-watchdog",
            make_silence_watchdog_factory(
                app=app,
                source_chat_id=settings.source_chat_id,
                redis_client=redis_client,
                silence_timeout=settings.listener_silence_timeout,
            ),
        ),
        name="silence-watchdog",
    ))

    logger.info(
        f"Workers running: {settings.worker_count} processors, album-flush, retry, health"
        + (", daily-report" if settings.daily_report_enabled else "")
        + f", channel-keepalive ({settings.listener_keepalive_interval}s)"
        + (f", silence-watchdog ({settings.listener_silence_timeout}s)" if settings.listener_silence_timeout > 0 else "")
    )


    def handle_shutdown(sig):
        logger.info(f"Received {sig.name} — shutting down")
        shutdown_event.set()
        for t in tasks:
            t.cancel()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_shutdown, sig)
        except NotImplementedError:
            pass

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("Shutting down...")
        await webhook.close()
        await sender.close()
        await app.stop()
        await redis_client.close()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
