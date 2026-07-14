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

import aiosqlite
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
        "reply_to_message_id": message.reply_to_message_id if message.reply_to_message_id else None,
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


async def forward_message_pipeline(client, payload, processed_payload, db_path):
    """
    Copy or send the processed message/album to all destinations.
    Keeps track of message IDs in SQLite to map reply threading correctly.
    """
    msg_type = payload.get("type")
    destinations = processed_payload.get("destinations", [])
    if not destinations:
        logger.warning("No destinations specified in processed payload")
        return True

    # Safely convert incoming IDs to integers if they are numerical
    def to_int_or_str(val):
        if val is None:
            return None
        try:
            return int(val)
        except (ValueError, TypeError):
            return val

    source_chat_id = to_int_or_str(payload.get("chat_id"))
    telegram_id = to_int_or_str(payload.get("message_id") or payload.get("media_group_id"))
    source_reply_id = to_int_or_str(payload.get("reply_to_message_id"))

    # 1. Insert/Update the source message in SQLite
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        
        # Insert parent message tracker
        await db.execute(
            """
            INSERT INTO messages (
                telegram_message_id, source_chat_id, message_type, 
                original_text, original_caption, 
                processed_text, processed_caption, 
                has_media, media_group_id, status, processed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'processing', datetime('now'))
            ON CONFLICT(telegram_message_id, source_chat_id) DO UPDATE SET
                processed_text = excluded.processed_text,
                processed_caption = excluded.processed_caption,
                status = 'processing',
                processed_at = datetime('now')
            """,
            (
                telegram_id,
                source_chat_id,
                msg_type,
                payload.get("text"),
                payload.get("caption"),
                processed_payload.get("processed_text"),
                processed_payload.get("processed_caption"),
                1 if payload.get("has_media") else 0,
                payload.get("media_group_id"),
            )
        )
        
        # Get the ID of the inserted/updated row
        cursor = await db.execute(
            "SELECT id FROM messages WHERE telegram_message_id = ? AND source_chat_id = ?",
            (telegram_id, source_chat_id)
        )
        row = await cursor.fetchone()
        message_db_id = row["id"] if row else None
        
        if not message_db_id:
            logger.error("Failed to track message in database")
            return False

        # 2. Forward to all destinations
        for dest in destinations:
            dest_chat_id = to_int_or_str(dest["chat_id"])
            sent_msg_id = None
            error_msg = None
            
            try:
                # Force Pyrogram to resolve and cache the destination chat ID
                try:
                    await client.get_chat(dest_chat_id)
                except Exception as cache_ex:
                    logger.warning(f"Could not pre-resolve destination chat {dest_chat_id}: {cache_ex}")

                # Check reply mapping
                reply_to_id = None
                if source_reply_id:
                    cursor = await db.execute(
                        """
                        SELECT md.sent_message_id 
                        FROM message_destinations md
                        JOIN messages m ON md.message_id = m.id
                        WHERE m.telegram_message_id = ? AND m.source_chat_id = ? AND md.destination_chat_id = ?
                        LIMIT 1
                        """,
                        (source_reply_id, source_chat_id, dest_chat_id)
                    )
                    reply_row = await cursor.fetchone()
                    if reply_row:
                        reply_to_id = to_int_or_str(reply_row["sent_message_id"])
                        logger.info(f"Mapping reply: source_reply_id={source_reply_id} -> destination_reply_id={reply_to_id}")

                # Perform the forward using Pyrogram Client
                if msg_type == "album":
                    items = processed_payload.get("items", [])
                    sorted_items = sorted(items, key=lambda x: to_int_or_str(x.get("message_id", 0)))
                    
                    # Extract captions in the correct order
                    captions = []
                    for item in sorted_items:
                        caption = item.get("processed_caption") or item.get("caption") or ""
                        captions.append(caption)
                        
                    anchor_msg_id = to_int_or_str(sorted_items[0]["message_id"])
                    
                    sent_messages = await client.copy_media_group(
                        chat_id=dest_chat_id,
                        from_chat_id=source_chat_id,
                        message_id=anchor_msg_id,
                        captions=captions,
                        reply_to_message_id=reply_to_id
                    )
                    if sent_messages:
                        sent_msg_id = to_int_or_str(sent_messages[0].id)
                else:
                    # Single message
                    msg_id = to_int_or_str(payload.get("message_id"))
                    if msg_type == "text":
                        text_changed = processed_payload.get("text_changed", False)
                        text_to_send = processed_payload.get("processed_text") or payload.get("text")
                        
                        if text_changed:
                            sent_msg = await client.send_message(
                                chat_id=dest_chat_id,
                                text=text_to_send,
                                reply_to_message_id=reply_to_id
                            )
                        else:
                            sent_msg = await client.copy_message(
                                chat_id=dest_chat_id,
                                from_chat_id=source_chat_id,
                                message_id=msg_id,
                                reply_to_message_id=reply_to_id
                            )
                    else:
                        # Media message
                        caption_changed = processed_payload.get("caption_changed", False)
                        caption_to_send = processed_payload.get("processed_caption") or payload.get("caption")
                        
                        if caption_changed:
                            sent_msg = await client.copy_message(
                                chat_id=dest_chat_id,
                                from_chat_id=source_chat_id,
                                message_id=msg_id,
                                caption=caption_to_send,
                                reply_to_message_id=reply_to_id
                            )
                        else:
                            sent_msg = await client.copy_message(
                                chat_id=dest_chat_id,
                                from_chat_id=source_chat_id,
                                message_id=msg_id,
                                reply_to_message_id=reply_to_id
                            )
                    sent_msg_id = to_int_or_str(sent_msg.id)
                
                # Log success to message_destinations
                await db.execute(
                    """
                    INSERT INTO message_destinations (message_id, destination_chat_id, status, sent_message_id, sent_at)
                    VALUES (?, ?, 'sent', ?, datetime('now'))
                    """,
                    (message_db_id, dest_chat_id, sent_msg_id)
                )
                logger.info(f"Forwarded successfully to {dest['name']} (chat_id={dest_chat_id}, sent_message_id={sent_msg_id})")

            except Exception as ex:
                error_msg = str(ex)
                logger.error(f"Failed to forward message to {dest['name']} (chat_id={dest_chat_id}): {ex}", exc_info=True)
                await db.execute(
                    """
                    INSERT INTO message_destinations (message_id, destination_chat_id, status, error_message, sent_at)
                    VALUES (?, ?, 'failed', ?, datetime('now'))
                    """,
                    (message_db_id, dest_chat_id, error_msg)
                )
        
        # 3. Update the overall status to sent
        await db.execute(
            "UPDATE messages SET status = 'sent', sent_at = datetime('now') WHERE id = ?",
            (message_db_id,)
        )
        await db.commit()
    return True


async def message_worker(
    worker_id: int,
    queue: asyncio.Queue,
    dedup,
    album_buffer,
    queue_manager,
    webhook_sender,
    app,
    db_path: str,
    delay_min: float = 0.5,
    delay_max: float = 1.5,
):
    """
    Worker task: pulls messages from asyncio.Queue, processes sequentially.
    Uses n8n webhook for processing text, then forwards directly using Pyrogram.
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
                    processed_payload = await webhook_sender.send(flush_result, endpoint="album")
                    if processed_payload:
                        forward_success = await forward_message_pipeline(app, flush_result, processed_payload, db_path)
                        if forward_success:
                            await queue_manager.remove_from_queue(flush_result)
                        else:
                            await queue_manager.enqueue_failed(flush_result, "Telegram forwarding failed")
                    else:
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
                processed_payload = await webhook_sender.send(payload, endpoint="message")
                if not processed_payload:
                    await queue_manager.enqueue_failed(
                        payload, "Webhook delivery failed"
                    )
                else:
                    forward_success = await forward_message_pipeline(app, payload, processed_payload, db_path)
                    if forward_success:
                        await queue_manager.remove_from_queue(payload)
                        logger.info(
                            f"Worker-{worker_id}: processed message {message_id} "
                            f"type={payload.get('type')}"
                        )
                    else:
                        await queue_manager.enqueue_failed(
                            payload, "Telegram forwarding failed"
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
