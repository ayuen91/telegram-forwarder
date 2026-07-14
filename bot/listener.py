"""
Message listener with backpressure control.

Two-stage design:
  Stage 1 — on_message handler: validates, normalizes, pushes to asyncio.Queue (fast)
  Stage 2 — Worker tasks: pull from queue, dedup, route to album buffer or webhook (controlled)

The asyncio.Queue (maxsize=100) acts as backpressure — if a channel dumps
50 messages at once, the queue absorbs the burst. Workers (2 by default)
process sequentially with randomized delays (0.5–1.5s) to look natural
to Telegram and prevent FloodWait errors.
"""

import asyncio
import json
import logging
import random
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import MessageMediaType

logger = logging.getLogger(__name__)


def normalize_message(message: Message) -> Optional[Dict[str, Any]]:
    """
    Extract a consistent payload from any Pyrogram message type.

    Returns None for unsupported message types (service messages, etc.)
    """
    # Determine message type and media info
    msg_type = _get_message_type(message)
    if msg_type is None:
        return None

    # Extract entities for formatting preservation
    entities = _serialize_entities(message.entities) if message.entities else []
    caption_entities = (
        _serialize_entities(message.caption_entities)
        if message.caption_entities
        else []
    )

    payload = {
        "message_id": message.id,
        "chat_id": message.chat.id,
        "type": msg_type,
        "text": message.text,
        "caption": message.caption,
        "entities": entities,
        "caption_entities": caption_entities,
        "media_group_id": message.media_group_id,
        "has_media": msg_type != "text",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    return payload


def _get_message_type(message: Message) -> Optional[str]:
    """Map Pyrogram message to our type string."""
    if message.text:
        return "text"
    elif message.photo:
        return "photo"
    elif message.video:
        return "video"
    elif message.document:
        return "document"
    elif message.sticker:
        return "sticker"
    elif message.voice:
        return "voice"
    elif message.video_note:
        return "video_note"
    elif message.animation:
        return "animation"
    elif message.audio:
        return "audio"
    elif message.poll:
        return "poll"
    elif message.contact:
        return "contact"
    elif message.location:
        return "location"
    else:
        # Service messages, empty messages, etc.
        logger.debug(f"Unsupported message type for message {message.id}, skipping")
        return None


def _serialize_entities(entities) -> list:
    """
    Serialize Pyrogram MessageEntity objects to dicts for JSON transport.
    These are needed by n8n to preserve formatting (bold, italic, links, etc.)
    when using sendMessage or overriding captions.
    """
    if not entities:
        return []

    result = []
    for entity in entities:
        entry = {
            "type": entity.type.value if hasattr(entity.type, "value") else str(entity.type),
            "offset": entity.offset,
            "length": entity.length,
        }
        # Optional fields
        if entity.url:
            entry["url"] = entity.url
        if entity.user:
            entry["user_id"] = entity.user.id
        if entity.language:
            entry["language"] = entity.language

        result.append(entry)

    return result


async def message_worker(
    worker_id: int,
    queue: asyncio.Queue,
    dedup,
    album_buffer,
    queue_manager,
    webhook_sender,
    delay_min: float = 0.5,
    delay_max: float = 1.5,
):
    """
    Worker task: pulls messages from asyncio.Queue, processes sequentially.

    Each worker adds a randomized delay between operations to:
    1. Look natural to Telegram (not machine-like)
    2. Prevent FloodWait errors
    3. Naturally limit throughput without a separate rate limiter

    With 2 workers and 0.5–1.5s delay, effective throughput is ~1–3 msg/sec.
    """
    logger.info(f"Worker-{worker_id} started")

    while True:
        payload = await queue.get()
        message_id = payload.get("message_id", "unknown")

        try:
            # Randomized delay — prevents burst processing
            delay = random.uniform(delay_min, delay_max)
            await asyncio.sleep(delay)

            # Deduplication check
            chat_id = payload.get("chat_id", 0)
            is_new = await dedup.is_new(chat_id, message_id)
            if not is_new:
                logger.debug(f"Worker-{worker_id}: duplicate {message_id}, skipping")
                continue

            # Route: album buffer or direct queue + webhook
            if payload.get("media_group_id"):
                # Album item — buffer in Redis, flush worker handles the rest
                flush_result = await album_buffer.add(payload)
                if flush_result:
                    # Album was full (10 items), immediately enqueue + send webhook
                    await queue_manager.enqueue(flush_result)
                    success = await webhook_sender.send(flush_result, endpoint="album")
                    if not success:
                        await queue_manager.enqueue_failed(
                            flush_result, "Webhook delivery failed (album, immediate flush)"
                        )
                logger.debug(
                    f"Worker-{worker_id}: buffered album item {message_id} "
                    f"(group={payload['media_group_id']})"
                )
            else:
                # Single message — enqueue in Redis and fire webhook
                await queue_manager.enqueue(payload)
                success = await webhook_sender.send(payload, endpoint="message")
                if not success:
                    await queue_manager.enqueue_failed(
                        payload, "Webhook delivery failed"
                    )
                else:
                    logger.info(
                        f"Worker-{worker_id}: processed message {message_id} "
                        f"type={payload.get('type')}"
                    )

        except Exception as e:
            logger.error(
                f"Worker-{worker_id}: error processing message {message_id}: {e}",
                exc_info=True,
            )
            # Push to failed queue for retry
            try:
                await queue_manager.enqueue_failed(payload, str(e))
            except Exception as qe:
                logger.error(f"Worker-{worker_id}: failed to enqueue error: {qe}")

        finally:
            queue.task_done()


def register_listener(
    app: Client,
    source_chat_id: int,
    message_queue: asyncio.Queue,
):
    """
    Register the message and channel post handlers on the Pyrogram client.

    The handler is intentionally thin — validate, normalize, enqueue.
    No Redis or webhook calls happen here.
    """

    @app.on_message(filters.chat(source_chat_id))
    async def on_message(client: Client, message: Message):
        """Validate, normalize, enqueue. No heavy work here."""
        payload = normalize_message(message)
        if payload is None:
            return  # Unsupported message type

        try:
            # Put to queue — blocks if queue is full (backpressure)
            await message_queue.put(payload)
            logger.debug(
                f"Enqueued message {message.id} type={payload['type']} "
                f"media_group={payload.get('media_group_id')}"
            )
        except asyncio.QueueFull:
            # Shouldn't happen with put() (it waits), but safety net
            logger.error(f"Message queue full, dropping message {message.id}")
