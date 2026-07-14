"""
Main entry point for the Telegram Forwarder bot.

Responsibilities:
  1. Startup self-test — block until all dependencies are ready
  2. Task supervisor — wrap all async tasks with auto-restart + FloodWait handling
  3. Reconnection loop — recover from Pyrogram disconnects

Launched tasks (all wrapped by supervised_task):
  - message_worker × N   — pull from asyncio.Queue, process messages
  - album_flush_worker    — poll expired album buffers every 500ms
  - retry_worker          — re-attempt failed webhooks every 60s
  - health_checker        — health checks + heartbeat + alerts every 5min
"""

import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

import aiosqlite
import redis.asyncio as aioredis
from pyrogram import Client
from pyrogram.errors import (
    FloodWait,
    AuthKeyUnregistered,
    SessionRevoked,
    UserDeactivated,
)

from config import config
from logging_config import setup_logging
from listener import register_listener, message_worker
from album_buffer import AlbumBuffer
from queue_manager import QueueManager
from deduplication import Deduplication
from webhook import WebhookSender
from health import HealthMonitor

logger = logging.getLogger(__name__)

# ── Globals ───────────────────────────────────────────────────────────
shutdown_event = asyncio.Event()


# ── Task Supervisor ───────────────────────────────────────────────────

async def supervised_task(name: str, coro_factory, restart_delay: float = 5.0):
    """
    Run a coroutine forever. On crash: log, alert, wait, restart.

    FloodWait is caught here — one place handles it for ALL tasks.
    Exponential backoff on repeated crashes, capped at 5 minutes.
    """
    current_delay = restart_delay

    while not shutdown_event.is_set():
        try:
            logger.info(f"Task '{name}' starting")
            current_delay = restart_delay  # Reset on successful start
            await coro_factory()
        except FloodWait as e:
            wait = e.value + 1
            logger.warning(f"Task '{name}' hit FloodWait({e.value}s), sleeping {wait}s")
            await asyncio.sleep(wait)
        except asyncio.CancelledError:
            logger.info(f"Task '{name}' cancelled — shutting down")
            return
        except (AuthKeyUnregistered, SessionRevoked, UserDeactivated) as e:
            # Fatal auth errors — can't auto-recover, alert and stop
            logger.critical(f"Task '{name}' hit fatal auth error: {e}")
            try:
                await health_monitor._send_telegram_alert(
                    f"🔴 <b>FATAL: Telegram Forwarder</b>\n\n"
                    f"<b>Task:</b> {name}\n"
                    f"<b>Error:</b> {e}\n\n"
                    f"<b>Action needed:</b> Re-authenticate Pyrogram session"
                )
            except Exception:
                pass
            # Keep retrying with long delay — maybe user will fix session
            await asyncio.sleep(300)
        except Exception as e:
            logger.error(f"Task '{name}' crashed: {e}", exc_info=True)
            try:
                await health_monitor._send_telegram_alert(
                    f"🟠 <b>Task Crashed: {name}</b>\n\n"
                    f"<b>Error:</b> {e}\n"
                    f"Auto-restarting in {current_delay:.0f}s"
                )
            except Exception:
                pass
            await asyncio.sleep(current_delay)
            current_delay = min(current_delay * 2, 300)  # Exponential backoff, cap 5min


# ── SQLite Initialization ─────────────────────────────────────────────

async def init_database(db_path: str):
    """Apply schema.sql on first run."""
    schema_path = Path("/app/db/schema.sql")

    if not schema_path.exists():
        logger.warning(f"schema.sql not found at {schema_path} — database may not have tables")
        return

    db_dir = Path(db_path).parent
    db_dir.mkdir(parents=True, exist_ok=True)

    async with aiosqlite.connect(db_path) as db:
        schema_sql = schema_path.read_text()
        await db.executescript(schema_sql)
        await db.commit()
        logger.info(f"Database initialized at {db_path}")


# ── Worker Factories ──────────────────────────────────────────────────
# These return coroutine factories for supervised_task

def make_worker_factory(worker_id, queue, dedup, album_buf, queue_mgr, webhook, settings, app, db_path):
    """Create a message worker coroutine factory."""
    async def worker():
        await message_worker(
            worker_id=worker_id,
            queue=queue,
            dedup=dedup,
            album_buffer=album_buf,
            queue_manager=queue_mgr,
            webhook_sender=webhook,
            app=app,
            db_path=db_path,
            delay_min=settings.worker_delay_min,
            delay_max=settings.worker_delay_max,
        )
    return worker


def make_album_flush_factory(album_buf, queue_mgr, webhook, app, db_path):
    """Create album flush worker coroutine factory."""
    async def album_flush():
        while not shutdown_event.is_set():
            try:
                completed = await album_buf.check_and_flush_expired()
                for album_payload in completed:
                    await queue_mgr.enqueue(album_payload)
                    processed_payload = await webhook.send(album_payload, endpoint="album")
                    if processed_payload:
                        from listener import forward_message_pipeline
                        forward_success = await forward_message_pipeline(app, album_payload, processed_payload, db_path)
                        if forward_success:
                            await queue_mgr.remove_from_queue(album_payload)
                        else:
                            await queue_mgr.enqueue_failed(album_payload, "Telegram forwarding failed")
                    else:
                        await queue_mgr.enqueue_failed(
                            album_payload, "Webhook delivery failed (album flush)"
                        )
            except Exception as e:
                logger.error(f"Album flush error: {e}", exc_info=True)

            await asyncio.sleep(0.5)  # Poll every 500ms
    return album_flush


def make_retry_factory(queue_mgr, webhook, app, db_path):
    """Create retry worker coroutine factory."""
    async def retry():
        while not shutdown_event.is_set():
            try:
                payload = await queue_mgr.dequeue_failed()
                if payload:
                    # Determine endpoint
                    endpoint = "album" if payload.get("type") == "album" else "message"
                    processed_payload = await webhook.send(payload, endpoint=endpoint)
                    if processed_payload:
                        from listener import forward_message_pipeline
                        forward_success = await forward_message_pipeline(app, payload, processed_payload, db_path)
                        if forward_success:
                            await queue_mgr.remove_from_queue(payload)
                            logger.info(
                                f"Retry succeeded for message "
                                f"{payload.get('message_id', payload.get('media_group_id'))}"
                            )
                        else:
                            await queue_mgr.enqueue_failed(payload, "Retry telegram forwarding failed")
                    else:
                        await queue_mgr.enqueue_failed(payload, "Retry webhook failed")
                else:
                    # No failed messages — sleep longer
                    await asyncio.sleep(60)
                    continue
            except Exception as e:
                logger.error(f"Retry worker error: {e}", exc_info=True)

            await asyncio.sleep(2)  # Small delay between retries
    return retry


def make_health_factory(monitor, interval):
    """Create health checker coroutine factory."""
    async def health():
        while not shutdown_event.is_set():
            try:
                await monitor.run_cycle()
            except Exception as e:
                logger.error(f"Health check error: {e}", exc_info=True)
            await asyncio.sleep(interval)
    return health


# ── Main ──────────────────────────────────────────────────────────────

# Global health monitor reference (used by supervised_task for alerts)
health_monitor: HealthMonitor = None


async def main():
    global health_monitor

    settings = config.settings

    # Initialize logging
    setup_logging(settings.log_level)
    logger.info("=" * 60)
    logger.info("Telegram Forwarder starting...")
    logger.info("=" * 60)

    # Validate configuration
    errors = config.validate()
    if errors:
        for err in errors:
            logger.error(f"Config error: {err}")
        logger.critical("Configuration validation failed — exiting")
        sys.exit(1)

    # Initialize SQLite
    await init_database(settings.db_path)

    # Initialize Redis
    redis_client = aioredis.from_url(
        settings.redis_url,
        decode_responses=True,
        retry_on_timeout=True,
    )

    # Initialize components
    dedup = Deduplication(redis_client)
    queue_mgr = QueueManager(redis_client, max_retries=settings.max_retries)
    album_buf = AlbumBuffer(redis_client, buffer_seconds=settings.album_buffer_seconds)
    webhook = WebhookSender(
        message_url=settings.n8n_webhook_message,
        album_url=settings.n8n_webhook_album,
        secret=settings.webhook_secret,
        config=config,
    )

    # Initialize Pyrogram client
    _workdir = os.getenv("PYROGRAM_WORKDIR", "/app/sessions")

    app = Client(
        name="user_session",
        api_id=settings.api_id,
        api_hash=settings.api_hash,
        phone_number=settings.phone_number,
        workdir=_workdir,
    )

    # Initialize health monitor
    health_monitor = HealthMonitor(
        redis_client=redis_client,
        webhook_sender=webhook,
        config=config,
        pyrogram_app=app,
        alert_bot_token=settings.alert_bot_token,
        alert_chat_id=settings.alert_chat_id,
        db_path=settings.db_path,
    )

    # Message queue (backpressure buffer)
    message_queue = asyncio.Queue(maxsize=100)

    # Register the on_message handler
    register_listener(app, settings.source_chat_id, message_queue)

    # Flush any stale albums from previous session
    await album_buf.flush_stale_albums()

    # ── Startup self-test ─────────────────────────────────────────
    logger.info("Running startup self-test...")

    # Start Pyrogram first (needed for get_me check)
    await app.start()
    logger.info("Pyrogram client started")

    # Verify source channel access
    try:
        source_chat = await app.get_chat(settings.source_chat_id)
        logger.info(f"✓ Source channel access verified: '{source_chat.title}' (type: {source_chat.type})")
    except Exception as e:
        logger.error(
            f"✗ CRITICAL: Cannot access source channel {settings.source_chat_id}. "
            f"Please verify that the user account is joined to this channel. Error: {e}"
        )

    # Now run self-test (retries every 30s until all pass)
    retry_count = 0
    while True:
        all_passed = await health_monitor.startup_self_test()
        if all_passed:
            break
        retry_count += 1
        if retry_count >= 20:  # ~10 minutes of retries
            logger.critical("Startup self-test failed after 20 attempts — exiting")
            await app.stop()
            sys.exit(1)
        logger.warning(f"Self-test failed, retrying in 30s (attempt {retry_count}/20)...")
        await asyncio.sleep(30)

    logger.info("Startup complete — launching workers")

    # ── Launch supervised tasks ───────────────────────────────────
    tasks = []

    # Message workers (N workers, default 2)
    for i in range(settings.worker_count):
        factory = make_worker_factory(
            i + 1, message_queue, dedup, album_buf, queue_mgr, webhook, settings, app, settings.db_path
        )
        tasks.append(asyncio.create_task(
            supervised_task(f"worker-{i+1}", factory),
            name=f"worker-{i+1}",
        ))

    # Album flush worker
    tasks.append(asyncio.create_task(
        supervised_task("album-flush", make_album_flush_factory(album_buf, queue_mgr, webhook, app, settings.db_path)),
        name="album-flush",
    ))

    # Retry worker
    tasks.append(asyncio.create_task(
        supervised_task("retry", make_retry_factory(queue_mgr, webhook, app, settings.db_path)),
        name="retry",
    ))

    # Health checker
    tasks.append(asyncio.create_task(
        supervised_task(
            "health",
            make_health_factory(health_monitor, settings.health_check_interval),
        ),
        name="health",
    ))

    logger.info(
        f"All tasks launched: {settings.worker_count} workers, "
        f"album-flush, retry, health"
    )

    # ── Graceful shutdown ─────────────────────────────────────────
    def handle_shutdown(sig):
        logger.info(f"Received {sig.name} — shutting down gracefully")
        shutdown_event.set()
        for t in tasks:
            t.cancel()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_shutdown, sig)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

    # Wait for all tasks (they run forever until shutdown)
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("Shutting down...")
        await webhook.close()
        await app.stop()
        await redis_client.close()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
