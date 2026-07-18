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
from typing import Dict, Any, Optional, Literal

import aiosqlite
from pyrogram import Client, filters
from pyrogram.types import Message

from media_relay import RelayConfig, relay_to_bot, cleanup_relay
from replacements import build_processed_payload, needs_n8n
from alerts import send_alert
from telegram_sender import TelegramBotSender, TelegramFloodWait
from forward_attribution import (
    serialize_forward_origin,
    ForwardAttributionChecker,
    attach_native_forward_flag,
)

logger = logging.getLogger(__name__)

ForwardStatus = Literal["success", "failed", "defer"]

# Message types delivered via Bot API send* methods (no userbot relay).
_BOT_API_DIRECT_TYPES = frozenset({"text", "poll", "contact", "location", "venue"})

# File media types that require userbot relay before copyMessage delivery.
_RELAY_MEDIA_TYPES = frozenset({
    "photo", "video", "document", "sticker", "voice",
    "video_note", "animation", "audio",
})


def normalize_message(message: Message) -> Optional[Dict[str, Any]]:
    """
    Extract a consistent payload from any Pyrogram message type.

    Returns None for unsupported message types (service messages, etc.)

    Formatting (bold, italic, links, block-quotes, etc.) is preserved by
    storing Pyrogram's pre-rendered .html string for text and caption.
    This is always a plain str and is always JSON-safe — no manual entity
    serialization is required.
    """
    # Determine message type and media info
    msg_type = _get_message_type(message)
    if msg_type is None:
        return None

    # Use Pyrogram's .html property to capture all formatting entities as
    # a plain HTML string.  Falls back to plain text when there are no
    # entities so the value is always a str or None.
    text_html: Optional[str] = None
    if message.text:
        text_html = str(message.text.html) if message.text.entities else str(message.text)

    caption_html: Optional[str] = None
    if message.caption:
        caption_html = str(message.caption.html) if message.caption.entities else str(message.caption)

    payload = {
        "message_id": message.id,
        "chat_id": message.chat.id,
        "type": msg_type,
        # plain text kept for DB storage / replacement matching
        "text": str(message.text) if message.text else None,
        "caption": str(message.caption) if message.caption else None,
        # HTML-encoded text that carries all Telegram formatting entities
        "text_html": text_html,
        "caption_html": caption_html,
        "media_group_id": message.media_group_id,
        "has_media": msg_type in _RELAY_MEDIA_TYPES or msg_type == "album",
        "reply_to_message_id": message.reply_to_message_id if message.reply_to_message_id else None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if msg_type == "poll" and message.poll:
        payload["poll"] = _serialize_poll(message.poll)
    elif msg_type == "contact" and message.contact:
        payload["contact"] = _serialize_contact(message.contact)
    elif msg_type == "location" and message.location:
        payload["location"] = _serialize_location(message.location)
    elif msg_type == "venue" and message.venue:
        payload["venue"] = _serialize_venue(message.venue)

    forward_origin = serialize_forward_origin(message)
    if forward_origin:
        payload["forward_origin"] = forward_origin

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
    elif message.venue:
        return "venue"
    elif message.location:
        return "location"
    else:
        # Service messages, empty messages, etc.
        logger.debug(f"Unsupported message type for message {message.id}, skipping")
        return None


def _serialize_poll(poll) -> Dict[str, Any]:
    poll_type = poll.type.value if hasattr(poll.type, "value") else str(poll.type or "regular")
    data: Dict[str, Any] = {
        "question": poll.question,
        "options": [opt.text for opt in poll.options],
        "is_anonymous": poll.is_anonymous if poll.is_anonymous is not None else True,
        "type": poll_type,
    }
    if poll.allows_multiple_answers:
        data["allows_multiple_answers"] = True
    if poll.correct_option_id is not None:
        data["correct_option_id"] = poll.correct_option_id
    if poll.explanation:
        data["explanation"] = poll.explanation
    if poll.open_period:
        data["open_period"] = poll.open_period
    if poll.close_date:
        data["close_date"] = poll.close_date.isoformat()
    return data


def _serialize_location(location) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "latitude": location.latitude,
        "longitude": location.longitude,
    }
    accuracy = getattr(location, "horizontal_accuracy", None)
    if accuracy is not None:
        data["horizontal_accuracy"] = accuracy
    return data


def _serialize_venue(venue) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "latitude": venue.location.latitude,
        "longitude": venue.location.longitude,
        "title": venue.title,
        "address": venue.address,
    }
    if venue.foursquare_id:
        data["foursquare_id"] = venue.foursquare_id
    if venue.foursquare_type:
        data["foursquare_type"] = venue.foursquare_type
    google_place_id = getattr(venue, "google_place_id", None)
    if google_place_id:
        data["google_place_id"] = google_place_id
    return data


def _serialize_contact(contact) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "phone_number": contact.phone_number,
        "first_name": contact.first_name,
    }
    if contact.last_name:
        data["last_name"] = contact.last_name
    if contact.vcard:
        data["vcard"] = contact.vcard
    return data


# _serialize_entities removed: formatting is now preserved via Pyrogram's
# .html property (stored as text_html / caption_html in the payload).
# This eliminates the "Object of type type is not JSON serializable" crash
# that occurred when MessageEntityType enum values were placed into the
# payload dict and then passed to json.dumps / aiohttp's JSON serializer.


def _to_int(val):
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _payload_inflight_ids(payload: Dict[str, Any]) -> list:
    """Message ids to track while a payload is being forwarded."""
    if payload.get("type") == "album":
        return [int(i["message_id"]) for i in payload.get("items", [])]
    mid = payload.get("message_id")
    return [int(mid)] if mid is not None else []


def _telegram_row_id(payload: Dict[str, Any]) -> Optional[int]:
    """Stable INTEGER key for the messages table (albums use first item id)."""
    if payload.get("type") == "album":
        items = payload.get("items") or []
        if items:
            sorted_items = sorted(items, key=lambda x: _to_int(x.get("message_id")) or 0)
            return _to_int(sorted_items[0].get("message_id"))
        return None
    return _to_int(payload.get("message_id"))


async def _lookup_reply_target(
    db, source_chat_id: int, source_reply_id: int, dest_chat_id: int
) -> Optional[int]:
    """Map a source reply target to the sent message id in the destination chat."""
    cursor = await db.execute(
        """
        SELECT sent_message_id FROM message_reply_map
        WHERE source_chat_id = ? AND source_message_id = ? AND destination_chat_id = ?
        """,
        (source_chat_id, source_reply_id, dest_chat_id),
    )
    row = await cursor.fetchone()
    if row:
        return int(row["sent_message_id"])
    return None


async def _is_reply_parent_pending(
    db, dedup, source_chat_id: int, source_reply_id: int
) -> bool:
    """True if the parent message is still being forwarded (defer reply threading)."""
    if dedup and await dedup.is_inflight(source_chat_id, source_reply_id):
        return True

    cursor = await db.execute(
        """
        SELECT status FROM messages
        WHERE source_chat_id = ? AND telegram_message_id = ?
        """,
        (source_chat_id, source_reply_id),
    )
    row = await cursor.fetchone()
    if row:
        return row["status"] in ("received", "queued", "processing")
    return False


async def _store_reply_mappings(
    db,
    source_chat_id: int,
    dest_chat_id: int,
    reply_mappings: list,
):
    for source_msg_id, sent_msg_id in reply_mappings:
        await db.execute(
            """
            INSERT INTO message_reply_map (
                source_chat_id, source_message_id, destination_chat_id, sent_message_id
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(source_chat_id, source_message_id, destination_chat_id) DO UPDATE SET
                sent_message_id = excluded.sent_message_id,
                created_at = datetime('now')
            """,
            (source_chat_id, int(source_msg_id), dest_chat_id, int(sent_msg_id)),
        )


def _source_message_ids(payload: Dict[str, Any]) -> list:
    """Source message ids that need userbot relay for media or rich-entity delivery."""
    msg_type = payload.get("type")
    if msg_type == "album":
        items = payload.get("items") or []
        return [int(i["message_id"]) for i in sorted(items, key=lambda x: int(x.get("message_id", 0)))]
    if msg_type in _RELAY_MEDIA_TYPES:
        return [int(payload["message_id"])]
    # Always relay text messages through Pyrogram's MTProto copy_message.
    # Original Pyrogram does not parse newer entity types (blockquote,
    # timestamp/date) so we cannot detect them from message.text.entities.
    # Relaying every text message is the only reliable way to ensure ALL
    # formatting — including blockquotes, dates, spoilers, custom emoji —
    # survives intact.  If a word-replacement fires, the relay result is
    # discarded (sendMessage is used) and cleaned up after delivery.
    if msg_type == "text":
        return [int(payload["message_id"])]
    return []


async def forward_message_pipeline(
    sender: TelegramBotSender,
    payload: Dict[str, Any],
    processed_payload: Dict[str, Any],
    db_path: str,
    dedup=None,
    pyrogram_app: Optional[Client] = None,
    relay: Optional[RelayConfig] = None,
) -> ForwardStatus:
    """
    Send the processed message/album to all destinations via Bot API.
    Media is relayed through the userbot first; text is sent directly.
    Native-forward payloads skip relay and use forwardMessage from the origin.
    Tracks per-item reply mappings so channel reply threads work correctly.
    """
    msg_type = payload.get("type")
    destinations = processed_payload.get("destinations") or []
    if not destinations:
        logger.warning("No destinations specified in processed payload")
        return "success"

    source_chat_id = _to_int(payload.get("chat_id"))
    telegram_id = _telegram_row_id(payload)
    source_reply_id = _to_int(payload.get("reply_to_message_id"))

    if source_chat_id is None or telegram_id is None:
        logger.error("Invalid payload: missing chat_id or message identifier")
        return "failed"

    any_failed = False
    any_deferred = False
    relay_message_ids: Optional[list] = None
    use_native = bool(payload.get("use_native_forward"))

    # Media relay only when not using native forward from origin channel
    source_ids_for_relay = [] if use_native else _source_message_ids(payload)
    if source_ids_for_relay:
        if not pyrogram_app or relay is None:
            logger.error("Media forwarding requires Pyrogram client and relay config")
            return "failed"
        try:
            relay_message_ids = await relay_to_bot(
                pyrogram_app,
                sender,
                source_chat_id,
                source_ids_for_relay,
                relay,
                is_album=(msg_type == "album"),
            )
        except Exception as ex:
            logger.error(f"Userbot media relay failed: {ex}", exc_info=True)
            return "failed"

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row

        await db.execute(
            """
            INSERT INTO messages (
                telegram_message_id, source_chat_id, message_type,
                original_text, original_caption,
                processed_text, processed_caption,
                has_media, media_group_id, album_item_count, status, processed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'processing', datetime('now'))
            ON CONFLICT(telegram_message_id, source_chat_id) DO UPDATE SET
                processed_text = excluded.processed_text,
                processed_caption = excluded.processed_caption,
                message_type = excluded.message_type,
                media_group_id = excluded.media_group_id,
                album_item_count = excluded.album_item_count,
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
                1 if payload.get("has_media") or msg_type == "album" else 0,
                str(payload.get("media_group_id")) if payload.get("media_group_id") else None,
                payload.get("item_count") if msg_type == "album" else None,
            ),
        )

        cursor = await db.execute(
            "SELECT id FROM messages WHERE telegram_message_id = ? AND source_chat_id = ?",
            (telegram_id, source_chat_id),
        )
        row = await cursor.fetchone()
        message_db_id = row["id"] if row else None

        if not message_db_id:
            logger.error("Failed to track message in database")
            return "failed"

        for dest in destinations:
            if not dest.get("enabled", True):
                continue

            dest_chat_id = _to_int(dest["chat_id"])
            dest_name = dest.get("name", str(dest_chat_id))

            # Skip destinations already delivered (e.g. retry after defer)
            cursor = await db.execute(
                """
                SELECT status FROM message_destinations
                WHERE message_id = ? AND destination_chat_id = ? AND status = 'sent'
                """,
                (message_db_id, dest_chat_id),
            )
            if await cursor.fetchone():
                logger.debug(f"Skipping {dest_name} — already sent")
                continue

            try:
                reply_to_id = None
                if source_reply_id:
                    reply_to_id = await _lookup_reply_target(
                        db, source_chat_id, source_reply_id, dest_chat_id
                    )
                    if reply_to_id is None:
                        if await _is_reply_parent_pending(
                            db, dedup, source_chat_id, source_reply_id
                        ):
                            logger.info(
                                f"Reply parent {source_reply_id} not ready yet for "
                                f"{dest_name} — will retry"
                            )
                            any_deferred = True
                            continue
                        logger.warning(
                            f"No reply mapping for source message {source_reply_id} "
                            f"in {dest_name}; sending without reply thread"
                        )
                    else:
                        logger.info(
                            f"Reply map: {source_reply_id} -> {reply_to_id} in {dest_name}"
                        )

                async def _send():
                    return await sender.forward_to_destination(
                        dest_chat_id=dest_chat_id,
                        msg_type=msg_type,
                        payload=payload,
                        processed_payload=processed_payload,
                        relay_chat_id=relay.bot_from_chat if relay else None,
                        relay_message_ids=relay_message_ids,
                        reply_to_message_id=reply_to_id,
                    )

                try:
                    result = await sender.call_with_flood_wait(_send)
                except TelegramFloodWait:
                    raise
                except Exception as native_err:
                    # Fall back to copy/send if native forward fails (protected
                    # content, deleted origin message, bot lacking access, etc.)
                    if use_native and _is_native_forward_fallback_error(native_err):
                        logger.warning(
                            f"Native forward failed for {dest_name} "
                            f"({native_err}); falling back to copy/send"
                        )
                        relay_holder = [relay_message_ids]
                        result = await _deliver_with_native_fallback(
                            sender=sender,
                            payload=payload,
                            processed_payload=processed_payload,
                            msg_type=msg_type,
                            dest_chat_id=dest_chat_id,
                            reply_to_id=reply_to_id,
                            pyrogram_app=pyrogram_app,
                            relay=relay,
                            source_chat_id=source_chat_id,
                            relay_message_ids_holder=relay_holder,
                        )
                        relay_message_ids = relay_holder[0]
                    else:
                        raise

                sent_msg_id = result["sent_message_id"]

                await _store_reply_mappings(
                    db, source_chat_id, dest_chat_id, result["reply_mappings"]
                )

                await db.execute(
                    """
                    INSERT INTO message_destinations (
                        message_id, destination_chat_id, status, sent_message_id, sent_at
                    ) VALUES (?, ?, 'sent', ?, datetime('now'))
                    ON CONFLICT(message_id, destination_chat_id) DO UPDATE SET
                        status = 'sent',
                        sent_message_id = excluded.sent_message_id,
                        sent_at = datetime('now'),
                        error_message = NULL
                    """,
                    (message_db_id, dest_chat_id, sent_msg_id),
                )
                logger.info(
                    f"Forwarded to {dest_name} (chat_id={dest_chat_id}, "
                    f"sent_message_id={sent_msg_id})"
                )

            except TelegramFloodWait:
                raise
            except Exception as ex:
                any_failed = True
                logger.error(
                    f"Failed to forward to {dest_name} (chat_id={dest_chat_id}): {ex}",
                    exc_info=True,
                )
                await db.execute(
                    """
                    INSERT INTO message_destinations (
                        message_id, destination_chat_id, status, error_message, sent_at
                    ) VALUES (?, ?, 'failed', ?, datetime('now'))
                    ON CONFLICT(message_id, destination_chat_id) DO UPDATE SET
                        status = 'failed',
                        error_message = excluded.error_message,
                        sent_at = datetime('now')
                    """,
                    (message_db_id, dest_chat_id, str(ex)),
                )

        if any_deferred:
            await db.execute(
                "UPDATE messages SET status = 'processing' WHERE id = ?",
                (message_db_id,),
            )
        elif any_failed:
            await db.execute(
                "UPDATE messages SET status = 'failed' WHERE id = ?",
                (message_db_id,),
            )
        else:
            await db.execute(
                """
                UPDATE messages SET status = 'sent', sent_at = datetime('now')
                WHERE id = ?
                """,
                (message_db_id,),
            )

        await db.commit()

    if relay_message_ids and not any_failed and not any_deferred:
        await cleanup_relay(pyrogram_app, relay, relay_message_ids)

    if any_deferred:
        return "defer"
    if any_failed:
        return "failed"
    return "success"


def _is_native_forward_fallback_error(exc: Exception) -> bool:
    """True if the error suggests falling back to copy/send is worthwhile."""
    msg = str(exc).lower()
    keywords = (
        "protected",
        "can't be forwarded",
        "cannot be forwarded",
        "message to forward not found",
        "message not found",
        "chat not found",
        "not enough rights",
        "have no rights",
        "bot is not a member",
        "forbidden",
        "forwardmessage failed",
        "forwardmessages failed",
    )
    return any(k in msg for k in keywords)


async def _deliver_with_native_fallback(
    sender: TelegramBotSender,
    payload: Dict[str, Any],
    processed_payload: Dict[str, Any],
    msg_type: str,
    dest_chat_id: int,
    reply_to_id,
    pyrogram_app: Optional[Client],
    relay: Optional[RelayConfig],
    source_chat_id: int,
    relay_message_ids_holder: list,
) -> Dict[str, Any]:
    """
    Retry delivery without native forward (copy/send path).
    Ensures media is relayed if needed. Mutates payload flags off.
    """
    fallback_payload = dict(payload)
    fallback_payload["use_native_forward"] = False
    fallback_payload.pop("native_forward_origin", None)

    relay_ids = relay_message_ids_holder[0]
    source_ids = _source_message_ids(fallback_payload)
    if source_ids and not relay_ids:
        if not pyrogram_app or relay is None:
            raise RuntimeError(
                "Native forward fallback needs Pyrogram/relay for media"
            )
        relay_ids = await relay_to_bot(
            pyrogram_app,
            sender,
            source_chat_id,
            source_ids,
            relay,
            is_album=(msg_type == "album"),
        )
        relay_message_ids_holder[0] = relay_ids

    async def _send():
        return await sender.forward_to_destination(
            dest_chat_id=dest_chat_id,
            msg_type=msg_type,
            payload=fallback_payload,
            processed_payload=processed_payload,
            relay_chat_id=relay.bot_from_chat if relay else None,
            relay_message_ids=relay_ids,
            reply_to_message_id=reply_to_id,
        )

    result = await sender.call_with_flood_wait(_send)
    return result


async def _resolve_processed_payload(
    webhook_sender,
    config,
    payload: Dict[str, Any],
    endpoint: str,
) -> Optional[Dict[str, Any]]:
    """Get processed payload via n8n (if text to edit) with local fallback."""
    if needs_n8n(payload):
        processed = await webhook_sender.send(payload, endpoint=endpoint)
        if processed and processed.get("destinations"):
            return processed
        logger.info(
            f"n8n unavailable or rejected msg {payload.get('message_id')} — using local processing"
        )
    return build_processed_payload(payload, config)


async def _prepare_payload_for_delivery(
    sender: TelegramBotSender,
    payload: Dict[str, Any],
    config,
) -> Dict[str, Any]:
    """Evaluate forward-attribution eligibility and flag the payload."""
    checker = ForwardAttributionChecker(sender)
    use_native = await checker.should_use_native_forward(payload, config)
    return attach_native_forward_flag(payload, use_native)


async def process_payload(
    sender: TelegramBotSender,
    queue_manager,
    webhook_sender,
    payload: Dict[str, Any],
    db_path: str,
    config,
    dedup=None,
    pyrogram_app: Optional[Client] = None,
    relay: Optional[RelayConfig] = None,
    alert_token: str = "",
    alert_chat_id: int = 0,
) -> ForwardStatus:
    """Run word replacement + Bot API forward for one payload."""
    endpoint = "album" if payload.get("type") == "album" else "message"
    chat_id = payload.get("chat_id", 0)
    inflight_ids = _payload_inflight_ids(payload)
    status: ForwardStatus = "failed"
    message_id = payload.get("message_id", payload.get("media_group_id", "?"))

    await queue_manager.enqueue(payload)
    if dedup and inflight_ids:
        await dedup.mark_inflight(chat_id, inflight_ids)

    try:
        payload = await _prepare_payload_for_delivery(sender, payload, config)
        processed_payload = await _resolve_processed_payload(
            webhook_sender, config, payload, endpoint
        )

        if not processed_payload or not processed_payload.get("destinations"):
            reason = "Processing failed — no destinations"
            await queue_manager.enqueue_failed(payload, reason)
            await send_alert(
                alert_token, alert_chat_id,
                f"🔴 <b>Forward failed</b>\nMessage: {message_id}\nReason: {reason}",
                alert_key=f"proc-fail-{message_id}",
            )
            return "failed"

        status = await forward_message_pipeline(
            sender, payload, processed_payload, db_path,
            dedup=dedup, pyrogram_app=pyrogram_app, relay=relay,
        )

        if status == "success":
            await queue_manager.remove_from_queue(payload)
        elif status == "defer":
            await queue_manager.enqueue_deferred(payload, "Reply parent not ready yet")
        else:
            reason = "Telegram forwarding failed"
            result = await queue_manager.enqueue_failed(payload, reason)
            await send_alert(
                alert_token, alert_chat_id,
                f"🔴 <b>Forward failed</b>\nMessage: {message_id}\nType: {payload.get('type')}\n"
                f"Reason: {reason}"
                + ("\n⚠️ Moved to dead letter queue" if result == "dead_letter" else ""),
                alert_key=f"fwd-fail-{message_id}",
            )

        return status
    finally:
        if dedup and inflight_ids and status != "defer":
            await dedup.clear_inflight(chat_id, inflight_ids)


async def retry_payload(
    sender: TelegramBotSender,
    queue_manager,
    webhook_sender,
    payload: Dict[str, Any],
    db_path: str,
    config,
    dedup=None,
    pyrogram_app: Optional[Client] = None,
    relay: Optional[RelayConfig] = None,
    alert_token: str = "",
    alert_chat_id: int = 0,
) -> ForwardStatus:
    """Retry processing + forward without re-enqueueing to the pending queue."""
    endpoint = "album" if payload.get("type") == "album" else "message"
    chat_id = payload.get("chat_id", 0)
    inflight_ids = _payload_inflight_ids(payload)
    status: ForwardStatus = "failed"
    message_id = payload.get("message_id", payload.get("media_group_id", "?"))

    if dedup and inflight_ids:
        await dedup.mark_inflight(chat_id, inflight_ids)

    try:
        payload = await _prepare_payload_for_delivery(sender, payload, config)
        processed_payload = await _resolve_processed_payload(
            webhook_sender, config, payload, endpoint
        )

        if not processed_payload or not processed_payload.get("destinations"):
            await queue_manager.enqueue_failed(payload, "Retry processing failed")
            return "failed"

        status = await forward_message_pipeline(
            sender, payload, processed_payload, db_path,
            dedup=dedup, pyrogram_app=pyrogram_app, relay=relay,
        )

        if status == "success":
            await queue_manager.remove_from_queue(payload)
            logger.info(f"Retry succeeded for {message_id}")
        elif status == "defer":
            await queue_manager.enqueue_deferred(payload, "Reply parent not ready yet (retry)")
        else:
            result = await queue_manager.enqueue_failed(payload, "Retry telegram forwarding failed")
            await send_alert(
                alert_token, alert_chat_id,
                f"🔴 <b>Retry failed</b>\nMessage: {message_id}"
                + ("\n⚠️ Moved to dead letter queue" if result == "dead_letter" else ""),
                alert_key=f"retry-fail-{message_id}",
            )

        return status
    finally:
        if dedup and inflight_ids and status != "defer":
            await dedup.clear_inflight(chat_id, inflight_ids)


async def message_worker(
    worker_id: int,
    queue: asyncio.Queue,
    dedup,
    album_buffer,
    queue_manager,
    webhook_sender,
    sender: TelegramBotSender,
    db_path: str,
    config,
    pyrogram_app: Optional[Client] = None,
    relay: Optional[RelayConfig] = None,
    alert_token: str = "",
    alert_chat_id: int = 0,
    delay_min: float = 0.5,
    delay_max: float = 1.5,
):
    """
    Worker task: pulls messages from asyncio.Queue, processes sequentially.
    Uses n8n webhook for word replacement, then delivers via Bot API.
    """
    logger.info(f"Worker-{worker_id} started")

    while True:
        payload = await queue.get()
        message_id = payload.get("message_id", "unknown")

        try:
            delay = random.uniform(delay_min, delay_max)
            await asyncio.sleep(delay)

            chat_id = payload.get("chat_id", 0)
            is_new = await dedup.is_new(chat_id, message_id)
            if not is_new:
                logger.debug(f"Worker-{worker_id}: duplicate {message_id}, skipping")
                continue

            if payload.get("media_group_id"):
                flush_result = await album_buffer.add(payload)
                if flush_result:
                    await process_payload(
                        sender, queue_manager, webhook_sender, flush_result,
                        db_path, config, dedup=dedup,
                        pyrogram_app=pyrogram_app, relay=relay,
                        alert_token=alert_token, alert_chat_id=alert_chat_id,
                    )
                logger.debug(
                    f"Worker-{worker_id}: buffered album item {message_id} "
                    f"(group={payload['media_group_id']})"
                )
            else:
                status = await process_payload(
                    sender, queue_manager, webhook_sender, payload,
                    db_path, config, dedup=dedup,
                    pyrogram_app=pyrogram_app, relay=relay,
                    alert_token=alert_token, alert_chat_id=alert_chat_id,
                )
                if status == "success":
                    logger.info(
                        f"Worker-{worker_id}: processed message {message_id} "
                        f"type={payload.get('type')}"
                    )

        except TelegramFloodWait as e:
            wait = e.retry_after + 1
            logger.warning(f"Worker-{worker_id}: FloodWait {wait}s")
            await asyncio.sleep(wait)
            await queue.put(payload)
        except Exception as e:
            logger.error(
                f"Worker-{worker_id}: error processing message {message_id}: {e}",
                exc_info=True,
            )
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
    redis_client=None,
):
    """
    Register the message handler on the Pyrogram (MTProto) client.

    In Pyrogram, on_message receives ALL incoming message updates including
    channel posts (UpdateNewChannelMessage) — there is no separate
    on_channel_post decorator (that is a Bot API / python-telegram-bot concept).

    For both public and private channels, the userbot MUST be a joined member
    to receive push updates. ensure_source_channel_membership() in main.py
    handles joining at startup when the userbot is not already a member.

    The handler is intentionally thin — validate, normalize, enqueue.
    Optionally increments a Redis daily received counter for metrics.
    """

    async def _handle(client: Client, message: Message):
        """Shared handler: validate, normalize, enqueue. No heavy work here."""
        payload = normalize_message(message)
        if payload is None:
            return  # Unsupported message type

        if redis_client is not None:
            try:
                from metrics import incr_received

                await incr_received(redis_client)
            except Exception as e:
                logger.debug(f"Received counter skipped: {e}")

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

    # Pyrogram routes channel posts (UpdateNewChannelMessage) and regular
    # messages (UpdateNewMessage) through the same on_message handler.
    # filters.chat() matches by numeric ID for both public and private channels.
    @app.on_message(filters.chat(source_chat_id))
    async def on_message(client: Client, message: Message):
        await _handle(client, message)

