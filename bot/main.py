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
import time
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
from listener import register_listener, message_worker, process_payload, retry_payload, overflow_drainer
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


def make_silence_watchdog_factory(
    app: Client,
    source_chat_id: int,
    redis_client,
    silence_timeout: int,
    health_monitor_ref,
):
    """
    PTS-audited silence watchdog (v2 — fixed).

    Key differences from v1:
      - Refreshes listener:last_pts every cycle when updates are flowing,
        so the baseline never goes stale.
      - Does NOT send Telegram alerts for organic silence (log only).
      - Checks back-off BEFORE sending the stall alert (no alert spam).
      - Adds a 30-min cooldown on stall alerts.
      - Uses get_me() as a secondary check before recycling — global PTS
        advances from ANY chat activity, so PTS delta alone is unreliable
        for single-channel stall detection.
    """
    from listener import LISTENER_LAST_RECEIVED_KEY, LISTENER_LAST_PTS_KEY
    import hydrogram.raw.functions.updates as _upd

    CHECK_INTERVAL = 300        # check every 5 minutes
    _last_recycle: float = 0.0
    _recycle_backoff: float = 300.0   # start at 5 min, doubles each time
    _MAX_BACKOFF: float = 3600.0      # cap at 1 hour
    _last_stall_alert: float = 0.0    # cooldown tracker for stall alerts
    _STALL_ALERT_COOLDOWN: float = 1800.0  # 30 min between stall alerts

    async def watchdog():
        nonlocal _last_recycle, _recycle_backoff, _last_stall_alert

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
                    # Key not set yet — fresh session, nothing to audit yet.
                    continue

                import time as _time
                last_ts = float(last_ts_raw)
                elapsed = _time.time() - last_ts
                elapsed_min = elapsed / 60

                if elapsed < silence_timeout:
                    # Push updates flowing normally — reset back-off and
                    # refresh the PTS baseline so it stays current.
                    _recycle_backoff = 300.0
                    try:
                        state = await app.invoke(_upd.GetState())
                        await redis_client.set(
                            LISTENER_LAST_PTS_KEY, str(state.pts), ex=86400
                        )
                    except Exception:
                        pass  # Non-critical; baseline just stays at last known value
                    continue

                # ── Silence threshold crossed — PTS audit ─────────────────
                logger.info(
                    f"Silence watchdog: {elapsed_min:.1f} min since last message "
                    f"(threshold={silence_timeout // 60} min) — auditing PTS..."
                )

                server_pts = None
                try:
                    state = await app.invoke(_upd.GetState())
                    server_pts = state.pts
                except Exception as pts_err:
                    logger.warning(
                        f"Silence watchdog: GetState() failed ({pts_err})"
                    )

                local_pts_raw = await redis_client.get(LISTENER_LAST_PTS_KEY)
                local_pts = int(local_pts_raw) if local_pts_raw else 0

                pts_delta = (server_pts - local_pts) if server_pts is not None else None

                # ── Determine if this is a real stall or organic silence ──
                # Global PTS advances from ANY chat the userbot is in, not
                # just the source channel.  A PTS delta alone does NOT prove
                # a stall — it only proves "something happened somewhere".
                # So we use a two-step test:
                #   1. PTS matches → definitely organic silence, skip recycle.
                #   2. PTS advanced → could be stall OR just other-chat noise.
                #      Verify the session is alive via get_me(); if that works
                #      and the client is connected, this is likely just quiet
                #      on our source channel while other chats are active.
                #      Only recycle if GetState() itself failed (dead connection).

                if server_pts is not None and (pts_delta is None or pts_delta <= 0):
                    # PTS matches — channel is genuinely quiet
                    logger.info(
                        f"Silence watchdog: PTS audit passed — "
                        f"server_pts={server_pts} == local_pts={local_pts}. "
                        "Channel is organically quiet. No recycle needed."
                    )
                    # Update the PTS baseline so next cycle has a fresh value
                    try:
                        await redis_client.set(
                            LISTENER_LAST_PTS_KEY, str(server_pts), ex=86400
                        )
                    except Exception:
                        pass
                    continue

                # PTS advanced or unavailable — check if session is alive
                session_alive = False
                if server_pts is not None:
                    try:
                        me = await app.get_me()
                        session_alive = me is not None
                    except Exception:
                        session_alive = False

                if session_alive and server_pts is not None:
                    # Session is alive, PTS advanced from other chats.
                    # Update baseline and skip recycle — source channel is
                    # just quiet while the user has activity elsewhere.
                    logger.info(
                        f"Silence watchdog: PTS delta=+{pts_delta} but session "
                        f"is alive (get_me OK). Source channel quiet, other chats "
                        f"active. Updating PTS baseline, no recycle."
                    )
                    try:
                        await redis_client.set(
                            LISTENER_LAST_PTS_KEY, str(server_pts), ex=86400
                        )
                    except Exception:
                        pass
                    continue

                # ── Confirmed stall: GetState failed OR session is dead ────
                pts_info = (
                    f"server_pts=<code>{server_pts}</code> / local_pts=<code>{local_pts}</code> "
                    f"(delta=<code>+{pts_delta}</code>)"
                    if pts_delta is not None
                    else "<i>PTS unavailable (GetState failed)</i>"
                )
                logger.warning(
                    f"Silence watchdog: CONFIRMED STALL — "
                    f"server_pts={server_pts}, local_pts={local_pts}, "
                    f"elapsed={elapsed_min:.1f} min. Recycling MTProto client..."
                )

                # Respect back-off between recycles — check BEFORE alerting
                now = _time.monotonic()
                if now - _last_recycle < _recycle_backoff:
                    remaining = _recycle_backoff - (now - _last_recycle)
                    logger.info(
                        f"Silence watchdog: back-off active — "
                        f"next recycle allowed in {remaining:.0f}s"
                    )
                    continue

                _last_recycle = now

                # Send stall alert (with 30-min cooldown)
                now_wall = _time.time()
                if now_wall - _last_stall_alert >= _STALL_ALERT_COOLDOWN:
                    _last_stall_alert = now_wall
                    try:
                        await health_monitor_ref._send_telegram_alert(
                            f"🔴 <b>ALERT: Listener Stall Confirmed</b>\n\n"
                            f"<blockquote>"
                            f"<b>Silence Duration:</b> <code>{elapsed_min:.1f} min</code>\n"
                            f"<b>PTS Audit:</b> {pts_info}\n"
                            f"<b>Session:</b> {'dead/unreachable' if not session_alive else 'alive'}\n"
                            f"<b>Action:</b> Full MTProto client recycle initiated."
                            f"</blockquote>\n"
                            f"⚙️ <i>Recycling the connection to force a full state-sync handshake.</i>\n"
                            f"🕒 <i>{_time.strftime('%Y-%m-%d %H:%M:%S UTC', _time.gmtime())}</i>"
                        )
                    except Exception:
                        pass

                # ── Perform client recycle ─────────────────────────────────
                try:
                    await app.stop()
                    logger.info("Silence watchdog: client stopped")
                    await asyncio.sleep(5)  # allow socket to fully close
                    await app.start()
                    logger.info("Silence watchdog: client restarted — session recycled")

                    # Reset liveness key so health check clock restarts from now
                    try:
                        await redis_client.set(
                            LISTENER_LAST_RECEIVED_KEY,
                            str(_time.time()),
                            ex=86400,
                        )
                    except Exception as lv_err:
                        logger.warning(f"Silence watchdog: liveness key reset failed: {lv_err}")

                    # Snapshot fresh PTS after reconnect
                    try:
                        state_after = await app.invoke(_upd.GetState())
                        await redis_client.set(
                            LISTENER_LAST_PTS_KEY,
                            str(state_after.pts),
                            ex=86400,
                        )
                        logger.info(
                            f"Silence watchdog: post-recycle PTS snapshot: {state_after.pts}"
                        )
                    except Exception as pts_snap_err:
                        logger.warning(
                            f"Silence watchdog: post-recycle PTS snapshot failed: {pts_snap_err}"
                        )

                    # Refresh access-hash cache after reconnect
                    try:
                        await app.resolve_peer(source_chat_id)
                        logger.info("Silence watchdog: peer re-resolved after recycle")
                    except Exception as rp_err:
                        logger.warning(f"Silence watchdog: resolve_peer failed: {rp_err}")

                    try:
                        await health_monitor_ref._send_telegram_alert(
                            f"✅ <b>Listener Recycle Successful</b>\n\n"
                            f"<blockquote>"
                            f"<b>Stall Duration:</b> <code>{elapsed_min:.1f} min</code>\n"
                            f"<b>Status:</b> MTProto session recycled. Awaiting fresh updates."
                            f"</blockquote>\n"
                            f"🕒 <i>{_time.strftime('%Y-%m-%d %H:%M:%S UTC', _time.gmtime())}</i>"
                        )
                    except Exception:
                        pass

                except Exception as recycle_err:
                    logger.error(
                        f"Silence watchdog: client recycle failed: {recycle_err}",
                        exc_info=True,
                    )
                    try:
                        next_retry_min = _recycle_backoff / 60
                        await health_monitor_ref._send_telegram_alert(
                            f"🔴 <b>ALERT: Listener Recycle Failed</b>\n\n"
                            f"<blockquote>"
                            f"<b>Error:</b> <code>{recycle_err.__class__.__name__}</code>\n"
                            f"<b>Details:</b> {recycle_err}\n"
                            f"<b>Next Retry:</b> no sooner than <code>{next_retry_min:.0f} min</code>"
                            f"</blockquote>\n"
                            f"⚠️ <i>Manual restart may be required if this persists.</i>\n"
                            f"🕒 <i>{_time.strftime('%Y-%m-%d %H:%M:%S UTC', _time.gmtime())}</i>"
                        )
                    except Exception:
                        pass

                _recycle_backoff = min(_recycle_backoff * 2, _MAX_BACKOFF)
                logger.info(
                    f"Silence watchdog: next recycle no sooner than {_recycle_backoff:.0f}s"
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

    message_queue = asyncio.Queue(maxsize=settings.queue_max_size)
    logger.info(f"Message queue initialised (maxsize={settings.queue_max_size})")

    # Record startup time BEFORE connecting so the listener can drop
    # any backlog updates Telegram pushes upon reconnect.
    bot_start_time = int(time.time())

    # ── Connect and verify ────────────────────────────────────────────
    logger.info("Connecting Hydrogram userbot...")
    await app.start()

    me = await app.get_me()
    logger.info(f"Hydrogram connected as {me.first_name} (id={me.id})")
    logger.info(
        f"Backlog filter active — messages older than t={bot_start_time} "
        "will be silently dropped (no relay, no DB write, no flood risk)"
    )

    # Snapshot initial PTS so the silence watchdog has a valid baseline
    # from the very first cycle (otherwise local_pts=0 → always looks like a stall).
    try:
        import hydrogram.raw.functions.updates as _upd
        from listener import LISTENER_LAST_PTS_KEY
        _init_state = await app.invoke(_upd.GetState())
        await redis_client.set(LISTENER_LAST_PTS_KEY, str(_init_state.pts), ex=86400)
        logger.info(f"Initial PTS snapshot: {_init_state.pts}")
    except Exception as pts_init_err:
        logger.warning(f"Initial PTS snapshot failed: {pts_init_err}")

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

    register_listener(
        app,
        settings.source_chat_id,
        message_queue,
        redis_client=redis_client,
        bot_start_time=bot_start_time,
    )
    stale_albums = await album_buf.flush_stale_albums()

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

    # Overflow drainer: refills the asyncio.Queue from Redis when the queue
    # was full and messages had to be spilled. Runs independently of workers.
    async def _overflow_drainer_factory():
        await overflow_drainer(message_queue, redis_client, shutdown_event)

    tasks.append(asyncio.create_task(
        supervised_task("overflow-drainer", _overflow_drainer_factory),
        name="overflow-drainer",
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



    # ── Silence watchdog (PTS-audited) ─────────────────────────────────
    # Every 5 min, checks if updates are flowing by reading the liveness key.
    # If silence > LISTENER_SILENCE_TIMEOUT (default 900s / 15 min), calls
    # updates.GetState() to compare server PTS vs local snapshot:
    #   - PTS delta > 0 → confirmed stall → full client recycle + alert
    #   - PTS delta = 0 → organic quiet  → informational alert, no recycle
    tasks.append(asyncio.create_task(
        supervised_task(
            "silence-watchdog",
            make_silence_watchdog_factory(
                app=app,
                source_chat_id=settings.source_chat_id,
                redis_client=redis_client,
                silence_timeout=settings.listener_silence_timeout,
                health_monitor_ref=health_monitor,
            ),
        ),
        name="silence-watchdog",
    ))

    logger.info(
        f"Workers running: {settings.worker_count} processors, album-flush, retry, health, overflow-drainer"
        + (", daily-report" if settings.daily_report_enabled else "")
        + (f", silence-watchdog (PTS-audited, {settings.listener_silence_timeout}s threshold)" if settings.listener_silence_timeout > 0 else "")
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
