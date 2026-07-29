"""
Message listener with backpressure control.

Two-stage design:
  Stage 1 — on_message handler: validates, normalizes, pushes to asyncio.Queue (fast)
  Stage 2 — Worker tasks: pull from queue, apply local replacements, deliver via Bot API

The asyncio.Queue (maxsize=100) acts as backpressure — if a channel dumps
50 messages at once, the queue absorbs the burst. Workers (2 by default)
process sequentially with randomized delays (0.5–1.5s) to look natural
to Telegram and prevent FloodWait errors.
"""

import asyncio
import json
import logging
import random
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Literal

import aiosqlite
from hydrogram import Client, filters
from hydrogram.errors import FloodWait
try:
    from hydrogram.errors import MessageIdsEmpty
except ImportError:
    try:
        from hydrogram.errors.exceptions.bad_request_400 import MessageIdsEmpty
    except ImportError:
        class MessageIdsEmpty(Exception):
            pass
try:
    from hydrogram.raw.types import UpdatesTooLong
except (ImportError, ModuleNotFoundError, RuntimeError):
    UpdatesTooLong = None

try:
    from hydrogram.raw.types import UpdateChannelTooLong
except (ImportError, ModuleNotFoundError, RuntimeError):
    UpdateChannelTooLong = None  # Graceful fallback
from hydrogram.types import Message

from media_relay import RelayConfig, relay_to_bot, cleanup_relay
from replacements import build_processed_payload
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


def _serialize_reply_markup(reply_markup) -> Optional[Dict[str, Any]]:
    if not reply_markup or not getattr(reply_markup, "inline_keyboard", None):
        return None
    rows = []
    for row in reply_markup.inline_keyboard:
        serialized_row = []
        for btn in row:
            btn_dict: Dict[str, Any] = {"text": str(getattr(btn, "text", ""))}
            if getattr(btn, "url", None):
                btn_dict["url"] = str(btn.url)
            if getattr(btn, "callback_data", None):
                cb = btn.callback_data
                if isinstance(cb, bytes):
                    try:
                        cb = cb.decode("utf-8")
                    except Exception:
                        cb = cb.hex()
                btn_dict["callback_data"] = str(cb)
            if getattr(btn, "switch_inline_query", None) is not None:
                btn_dict["switch_inline_query"] = str(btn.switch_inline_query)
            if getattr(btn, "switch_inline_query_current_chat", None) is not None:
                btn_dict["switch_inline_query_current_chat"] = str(btn.switch_inline_query_current_chat)
            if getattr(btn, "web_app", None) and getattr(btn.web_app, "url", None):
                btn_dict["web_app"] = {"url": str(btn.web_app.url)}
            if getattr(btn, "login_url", None) and getattr(btn.login_url, "url", None):
                login_dict: Dict[str, Any] = {"url": str(btn.login_url.url)}
                if getattr(btn.login_url, "forward_text", None):
                    login_dict["forward_text"] = str(btn.login_url.forward_text)
                btn_dict["login_url"] = login_dict
            serialized_row.append(btn_dict)
        if serialized_row:
            rows.append(serialized_row)
    if not rows:
        return None
    return {"inline_keyboard": rows}


def normalize_message(message: Message) -> Optional[Dict[str, Any]]:
    """
    Extract a consistent payload from any Hydrogram message type.

    Returns None for unsupported message types (service messages, etc.)

    Formatting (bold, italic, links, block-quotes, etc.) is preserved by
    storing Hydrogram's pre-rendered .html string for text and caption.
    This is always a plain str and is always JSON-safe — no manual entity
    serialization is required.
    """
    # Determine message type and media info
    msg_type = _get_message_type(message)
    if msg_type is None:
        return None

    if msg_type == "pin":
        pinned_msg = getattr(message, "pinned_message", None)
        pinned_msg_id = pinned_msg.id if pinned_msg else getattr(message, "action_message_id", None)
        if not pinned_msg_id and hasattr(message, "action"):
            action = message.action
            if hasattr(action, "message_id"):
                pinned_msg_id = action.message_id
        if not pinned_msg_id:
            logger.warning(f"Pin service message {message.id} missing target pinned_message ID")
            return None
        return {
            "message_id": message.id,
            "chat_id": message.chat.id,
            "type": "pin",
            "pinned_message_id": pinned_msg_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # Use Hydrogram's .html property to capture all formatting entities as
    # a plain HTML string.  Falls back to plain text when there are no
    # entities so the value is always a str or None.
    text_html: Optional[str] = None
    if message.text:
        text_html = str(message.text.html) if getattr(message.text, "entities", None) else str(message.text)

    caption_html: Optional[str] = None
    if getattr(message, "caption", None):
        caption_html = str(message.caption.html) if getattr(message.caption, "entities", None) else str(message.caption)

    # Detect Restrict Saving Content (protected content) flag.
    # When True, copy_message / copy_media_group will raise
    # CHAT_FORWARDS_RESTRICTED — the relay layer falls back to
    # download + reupload automatically, but capturing it here lets
    # the worker log it early and avoids a surprise exception later.
    has_protected_content: bool = bool(
        getattr(message, "has_protected_content", False)
        or getattr(message.chat, "has_protected_content", False)
    )

    reply_to_quote_text: Optional[str] = None
    reply_to_quote_html: Optional[str] = None
    reply_to_quote_position: Optional[int] = None

    quote_obj = getattr(message, "quote", None)
    if quote_obj:
        q_text = getattr(quote_obj, "text", None)
        if q_text is not None:
            reply_to_quote_text = str(q_text)
            if getattr(quote_obj, "entities", None) and hasattr(q_text, "html"):
                reply_to_quote_html = str(q_text.html)
            elif getattr(quote_obj, "html", None):
                reply_to_quote_html = str(quote_obj.html)
            else:
                reply_to_quote_html = reply_to_quote_text
        else:
            reply_to_quote_text = str(quote_obj)
            reply_to_quote_html = reply_to_quote_text
        reply_to_quote_position = (
            getattr(quote_obj, "position", None)
            if getattr(quote_obj, "position", None) is not None
            else getattr(quote_obj, "offset", None)
        )
    elif getattr(message, "quote_text", None):
        reply_to_quote_text = str(message.quote_text)
        reply_to_quote_html = str(getattr(message, "quote_html", reply_to_quote_text))
        reply_to_quote_position = (
            getattr(message, "quote_position", None)
            if getattr(message, "quote_position", None) is not None
            else getattr(message, "quote_offset", None)
        )
    elif getattr(message, "reply_to", None) and getattr(message.reply_to, "quote_text", None):
        reply_to_quote_text = str(message.reply_to.quote_text)
        reply_to_quote_html = reply_to_quote_text
        reply_to_quote_position = getattr(message.reply_to, "quote_offset", None)

    payload = {
        "message_id": message.id,
        "chat_id": message.chat.id,
        "type": msg_type,
        # plain text kept for DB storage / replacement matching
        "text": str(message.text) if getattr(message, "text", None) else None,
        "caption": str(message.caption) if getattr(message, "caption", None) else None,
        # HTML-encoded text that carries all Telegram formatting entities
        "text_html": text_html,
        "caption_html": caption_html,
        "media_group_id": str(message.media_group_id) if message.media_group_id is not None else None,
        "has_media": msg_type in _RELAY_MEDIA_TYPES or msg_type == "album",
        "has_protected_content": has_protected_content,
        "reply_to_message_id": int(message.reply_to_message_id) if message.reply_to_message_id else None,
        "reply_to_quote_text": reply_to_quote_text,
        "reply_to_quote_html": reply_to_quote_html,
        "reply_to_quote_position": reply_to_quote_position,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    reply_markup = _serialize_reply_markup(getattr(message, "reply_markup", None))
    if reply_markup:
        payload["reply_markup"] = reply_markup

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
    """Map Hydrogram message to our type string."""
    if getattr(message, "pinned_message", None) or (
        getattr(message, "service", False) and "pin" in str(getattr(message, "action", "")).lower()
    ):
        return "pin"
    elif message.text:
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

    # poll.question and each option's .text are Hydrogram TextWithEntities objects,
    # not plain strings. Calling str() or accessing .text coerces them safely.
    def _to_str(val) -> str:
        """Safely extract a plain string from a TextWithEntities or plain str."""
        if val is None:
            return ""
        # TextWithEntities exposes a .text attribute; plain strings don't.
        raw = getattr(val, "text", None)
        return str(raw) if raw is not None else str(val)

    data: Dict[str, Any] = {
        "question": _to_str(poll.question),
        "options": [_to_str(getattr(opt, "text", opt)) for opt in poll.options],
        "is_anonymous": poll.is_anonymous if poll.is_anonymous is not None else True,
        "type": poll_type,
    }
    if poll.allows_multiple_answers:
        data["allows_multiple_answers"] = True
    if poll.correct_option_id is not None:
        data["correct_option_id"] = poll.correct_option_id
    if poll.explanation:
        data["explanation"] = _to_str(poll.explanation)
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


# _serialize_entities removed: formatting is now preserved via Hydrogram's
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
    # Always relay text messages through Hydrogram's MTProto copy_message.
    # Original Hydrogram does not parse newer entity types (blockquote,
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
    hydrogram_app: Optional[Client] = None,
    relay: Optional[RelayConfig] = None,
    alert_token: str = "",
    alert_chat_id: int = 0,
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

    if msg_type == "pin":
        pinned_msg_id = payload.get("pinned_message_id")
        if not pinned_msg_id or source_chat_id is None:
            logger.error("Pin payload missing pinned_message_id or source chat_id")
            return "failed"

        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row

            if dedup and await _is_reply_parent_pending(db, dedup, source_chat_id, pinned_msg_id):
                logger.info(
                    f"Pinned target message {pinned_msg_id} in {source_chat_id} is still pending — deferring pin"
                )
                return "defer"

            any_failed = False
            for dest in destinations:
                if not dest.get("enabled", True):
                    continue
                dest_chat_id = _to_int(dest["chat_id"])
                if dest_chat_id is None:
                    continue
                dest_name = dest.get("name", str(dest_chat_id))

                sent_msg_id = await _lookup_reply_target(db, source_chat_id, pinned_msg_id, dest_chat_id)
                if sent_msg_id is None:
                    logger.warning(
                        f"Pin failed: message {pinned_msg_id} from source {source_chat_id} "
                        f"not found in destination {dest_name} ({dest_chat_id})"
                    )
                    any_failed = True
                    cur_alert_token = alert_token or processed_payload.get("alert_token", "")
                    cur_alert_chat_id = alert_chat_id or processed_payload.get("alert_chat_id", 0)
                    if cur_alert_token and cur_alert_chat_id:
                        await send_alert(
                            cur_alert_token,
                            cur_alert_chat_id,
                            f"⚠️ <b>Message Not Pinned</b>\n\n"
                            f"Message <code>{pinned_msg_id}</code> was pinned in source channel <code>{source_chat_id}</code>, "
                            f"but it does not exist in destination channel <b>{dest_name}</b> (<code>{dest_chat_id}</code>).",
                            alert_key=f"pin-missing-{source_chat_id}-{pinned_msg_id}-{dest_chat_id}",
                        )
                else:
                    try:
                        await sender.call_with_flood_wait(
                            lambda: sender.pin_chat_message(dest_chat_id, sent_msg_id)
                        )
                        logger.info(
                            f"Successfully pinned message {sent_msg_id} in destination {dest_name} "
                            f"for source pin of message {pinned_msg_id}"
                        )
                    except Exception as pin_err:
                        logger.error(
                            f"Failed to pin message {sent_msg_id} in destination {dest_name}: {pin_err}"
                        )
                        any_failed = True
                        cur_alert_token = alert_token or processed_payload.get("alert_token", "")
                        cur_alert_chat_id = alert_chat_id or processed_payload.get("alert_chat_id", 0)
                        if cur_alert_token and cur_alert_chat_id:
                            await send_alert(
                                cur_alert_token,
                                cur_alert_chat_id,
                                f"⚠️ <b>Message Pin Failed</b>\n\n"
                                f"Message <code>{pinned_msg_id}</code> was pinned in source channel <code>{source_chat_id}</code>, "
                                f"but pinning message <code>{sent_msg_id}</code> failed in destination <b>{dest_name}</b> (<code>{dest_chat_id}</code>).\n\n"
                                f"<b>Error:</b> <code>{pin_err}</code>",
                                alert_key=f"pin-error-{source_chat_id}-{pinned_msg_id}-{dest_chat_id}",
                            )

            return "failed" if any_failed else "success"

    any_failed = False
    any_deferred = False
    relay_message_ids: Optional[list] = None
    use_native = bool(payload.get("use_native_forward"))

    # Media relay only when not using native forward from origin channel
    source_ids_for_relay = [] if use_native else _source_message_ids(payload)
    if source_ids_for_relay:
        if not hydrogram_app or relay is None:
            logger.error("Media forwarding requires Hydrogram client and relay config")
            return "failed"

        # Guard against MESSAGE_IDS_EMPTY: drop zeroes / None before relay
        valid_relay_ids = [mid for mid in source_ids_for_relay if mid and int(mid) > 0]
        if not valid_relay_ids:
            logger.warning(
                f"All relay message IDs were invalid for payload "
                f"{payload.get('message_id')} — skipping relay (message may be deleted)"
            )
            return "failed"

        if payload.get("has_protected_content"):
            logger.info(
                f"Source channel has protected content — relay will use "
                f"download+reupload fallback for message {payload.get('message_id')}"
            )

        try:
            relay_message_ids = await relay_to_bot(
                hydrogram_app,
                sender,
                source_chat_id,
                valid_relay_ids,
                relay,
                is_album=(msg_type == "album"),
            )
        except MessageIdsEmpty:
            logger.warning(
                f"MESSAGE_IDS_EMPTY during relay for message "
                f"{payload.get('message_id')} — message was likely deleted"
            )
            return "failed"
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
                            hydrogram_app=hydrogram_app,
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

    if relay_message_ids and not any_deferred:
        await cleanup_relay(hydrogram_app, relay, relay_message_ids)

    if any_deferred:
        return "defer"
    if any_failed:
        return "failed"
    return "success"


def _is_native_forward_fallback_error(exc: Exception) -> bool:
    """True if the error suggests falling back to copy/send is worthwhile.

    Covers all known Telegram error responses that indicate the sender bot
    cannot forward from the origin channel (not a member, not admin, private
    channel, kicked, etc.).  Any error in this list triggers the safe
    copy/send fallback instead of bubbling up to the failed-queue handler.
    """
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
        # Bot is not a member / participant of the origin channel
        "channel_private",
        "user_not_participant",
        "not a member",
        "bot was kicked",
        "kicked from",
        # Origin peer is inaccessible or the ID is stale/invalid
        "peer_id_invalid",
        # Generic admin-rights errors from some Telegram servers
        "need administrator rights",
        "administrator rights",
    )
    return any(k in msg for k in keywords)


async def _deliver_with_native_fallback(
    sender: TelegramBotSender,
    payload: Dict[str, Any],
    processed_payload: Dict[str, Any],
    msg_type: str,
    dest_chat_id: int,
    reply_to_id,
    hydrogram_app: Optional[Client],
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
        if not hydrogram_app or relay is None:
            raise RuntimeError(
                "Native forward fallback needs Hydrogram/relay for media"
            )
        relay_ids = await relay_to_bot(
            hydrogram_app,
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


def _resolve_processed_payload(
    config,
    payload: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Apply word replacements locally and attach destinations."""
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
    payload: Dict[str, Any],
    db_path: str,
    config,
    dedup=None,
    hydrogram_app: Optional[Client] = None,
    relay: Optional[RelayConfig] = None,
    alert_token: str = "",
    alert_chat_id: int = 0,
) -> ForwardStatus:
    """Apply local word replacements + Bot API forward for one payload."""
    chat_id = payload.get("chat_id", 0)
    inflight_ids = _payload_inflight_ids(payload)
    status: ForwardStatus = "failed"
    message_id = payload.get("message_id", payload.get("media_group_id", "?"))

    await queue_manager.enqueue(payload)
    if dedup and inflight_ids:
        await dedup.mark_inflight(chat_id, inflight_ids)

    try:
        payload = await _prepare_payload_for_delivery(sender, payload, config)
        processed_payload = _resolve_processed_payload(config, payload)

        if not processed_payload or not processed_payload.get("destinations"):
            reason = "Processing failed — no destinations"
            # Always remove from the pending queue before moving to failed,
            # so the item is never left as a ghost entry.
            await queue_manager.remove_from_queue(payload)
            await queue_manager.enqueue_failed(payload, reason)
            await send_alert(
                alert_token, alert_chat_id,
                f"🔴 <b>Forward Failed</b>\n\n"
                f"<blockquote><b>Message ID:</b> <code>{message_id}</code>\n"
                f"<b>Reason:</b> {reason}</blockquote>",
                alert_key=f"proc-fail-{message_id}",
            )
            return "failed"

        status = await forward_message_pipeline(
            sender, payload, processed_payload, db_path,
            dedup=dedup, hydrogram_app=hydrogram_app, relay=relay,
            alert_token=alert_token, alert_chat_id=alert_chat_id,
        )

        # Always remove from the primary pending queue first, then move to the
        # appropriate secondary queue (deferred / failed) if needed.  This
        # ensures the item never stays as a ghost in queue:messages.
        await queue_manager.remove_from_queue(payload)

        if status == "defer":
            await queue_manager.enqueue_deferred(payload, "Reply parent not ready yet")
        elif status != "success":
            reason = "Telegram forwarding failed"
            result = await queue_manager.enqueue_failed(payload, reason)
            await send_alert(
                alert_token, alert_chat_id,
                f"🔴 <b>Forward Failed</b>\n\n"
                f"<blockquote><b>Message ID:</b> <code>{message_id}</code>\n"
                f"<b>Type:</b> <code>{payload.get('type')}</code>\n"
                f"<b>Reason:</b> {reason}</blockquote>"
                + ("\n⚠️ <b>Status:</b> Moved to dead letter queue" if result == "dead_letter" else ""),
                alert_key=f"fwd-fail-{message_id}",
            )

        return status
    finally:
        if dedup and inflight_ids and status != "defer":
            await dedup.clear_inflight(chat_id, inflight_ids)


async def retry_payload(
    sender: TelegramBotSender,
    queue_manager,
    payload: Dict[str, Any],
    db_path: str,
    config,
    dedup=None,
    hydrogram_app: Optional[Client] = None,
    relay: Optional[RelayConfig] = None,
    alert_token: str = "",
    alert_chat_id: int = 0,
) -> ForwardStatus:
    """Retry processing + forward without re-enqueueing to the pending queue."""
    chat_id = payload.get("chat_id", 0)
    inflight_ids = _payload_inflight_ids(payload)
    status: ForwardStatus = "failed"
    message_id = payload.get("message_id", payload.get("media_group_id", "?"))

    if dedup and inflight_ids:
        await dedup.mark_inflight(chat_id, inflight_ids)

    try:
        payload = await _prepare_payload_for_delivery(sender, payload, config)
        processed_payload = _resolve_processed_payload(config, payload)

        if not processed_payload or not processed_payload.get("destinations"):
            # Remove from the pending queue before escalating to failed.
            await queue_manager.remove_from_queue(payload)
            await queue_manager.enqueue_failed(payload, "Retry processing failed")
            return "failed"

        status = await forward_message_pipeline(
            sender, payload, processed_payload, db_path,
            dedup=dedup, hydrogram_app=hydrogram_app, relay=relay,
            alert_token=alert_token, alert_chat_id=alert_chat_id,
        )

        # Always remove from the primary pending queue first so the item is
        # never left as a ghost entry regardless of outcome.
        await queue_manager.remove_from_queue(payload)

        if status == "success":
            logger.info(f"Retry succeeded for {message_id}")
        elif status == "defer":
            await queue_manager.enqueue_deferred(payload, "Reply parent not ready yet (retry)")
        else:
            result = await queue_manager.enqueue_failed(payload, "Retry telegram forwarding failed")
            await send_alert(
                alert_token, alert_chat_id,
                f"🔴 <b>Retry Failed</b>\n\n"
                f"<blockquote><b>Message ID:</b> <code>{message_id}</code>\n"
                f"<b>Reason:</b> Retry telegram forwarding failed</blockquote>"
                + ("\n⚠️ <b>Status:</b> Moved to dead letter queue" if result == "dead_letter" else ""),
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
    sender: TelegramBotSender,
    db_path: str,
    config,
    hydrogram_app: Optional[Client] = None,
    relay: Optional[RelayConfig] = None,
    alert_token: str = "",
    alert_chat_id: int = 0,
    delay_min: float = 0.5,
    delay_max: float = 1.5,
):
    """
    Worker task: pulls messages from asyncio.Queue, applies local replacements,
    and delivers via Bot API.
    """
    logger.info(f"Worker-{worker_id} started")

    while True:
        payload = await queue.get()
        message_id = payload.get("message_id", "unknown")

        try:
            chat_id = payload.get("chat_id", 0)
            is_new = await dedup.is_new(chat_id, message_id)
            if not is_new:
                logger.debug(f"Worker-{worker_id}: duplicate {message_id}, skipping")
                continue

            delay = random.uniform(delay_min, delay_max)
            await asyncio.sleep(delay)

            if payload.get("media_group_id"):
                flush_result = await album_buffer.add(payload)
                if flush_result:
                    await process_payload(
                        sender, queue_manager, flush_result,
                        db_path, config, dedup=dedup,
                        hydrogram_app=hydrogram_app, relay=relay,
                        alert_token=alert_token, alert_chat_id=alert_chat_id,
                    )
                logger.debug(
                    f"Worker-{worker_id}: buffered album item {message_id} "
                    f"(group={payload['media_group_id']})"
                )
            else:
                status = await process_payload(
                    sender, queue_manager, payload,
                    db_path, config, dedup=dedup,
                    hydrogram_app=hydrogram_app, relay=relay,
                    alert_token=alert_token, alert_chat_id=alert_chat_id,
                )
                if status == "success":
                    logger.info(
                        f"Worker-{worker_id}: processed message {message_id} "
                        f"type={payload.get('type')}"
                    )

        except TelegramFloodWait as e:
            # Bot API 429 — sleep the requested duration then re-queue
            wait = e.retry_after + 1
            logger.warning(
                f"Worker-{worker_id}: Bot API FloodWait {wait}s — "
                f"re-queuing message {message_id}"
            )
            await asyncio.sleep(wait)
            await queue.put(payload)
        except FloodWait as e:
            # Hydrogram (MTProto) flood wait — also sleep + re-queue
            # Previously this fell through to the generic Exception handler
            # which moved the message to the failed queue instead of retrying.
            wait = e.value + 1
            logger.warning(
                f"Worker-{worker_id}: MTProto FloodWait {wait}s — "
                f"re-queuing message {message_id}"
            )
            await asyncio.sleep(wait)
            await queue.put(payload)
        except Exception as e:
            logger.error(
                f"Worker-{worker_id}: error processing message {message_id}: {e}",
                exc_info=True,
            )
            try:
                # Remove from the primary queue first so the item does not linger
                # as a ghost "pending" entry before being moved to the failed queue.
                await queue_manager.remove_from_queue(payload)
                await queue_manager.enqueue_failed(payload, str(e))
            except Exception as qe:
                logger.error(f"Worker-{worker_id}: failed to enqueue error: {qe}")

        finally:
            queue.task_done()


# Redis key that tracks when the listener last received any message.
# Used by the liveness health check to detect silent update failures.
LISTENER_LAST_RECEIVED_KEY = "listener:last_received_at"

# Redis key prefix for the last known message ID per channel.
# Written by _handle() on every push-delivered message and by the
# UpdateChannelTooLong catch-up handler after a successful sync.
# Format: listener:last_msg_id:{chat_id}
LISTENER_LAST_MSG_ID_PREFIX = "listener:last_msg_id"

# Redis key that stores the last PTS value seen from updates.GetState().
# Written by _do_channel_catchup() after every successful sync so the
# silence watchdog can compare against the server-side PTS to distinguish
# a genuine stall from an organically quiet channel.
LISTENER_LAST_PTS_KEY = "listener:last_pts"

# Redis list used as an overflow buffer when the asyncio.Queue is full.
# _handle() pushes here instead of blocking; overflow_drainer() refills
# the queue as workers free up slots. Messages are never lost.
LISTENER_OVERFLOW_KEY = "listener:overflow"


def register_listener(
    app: Client,
    source_chat_id: int,
    message_queue: asyncio.Queue,
    redis_client=None,
    bot_start_time: Optional[int] = None,
):
    """
    bot_start_time — Unix timestamp (seconds) recorded when the bot started.
    Any message whose .date is strictly before this value is considered
    backlog from a previous session and is silently dropped.  Pass
    int(time.time()) from main.py just before app.start() to activate.
    Defaults to None (disabled) so existing call sites are unaffected.
    """
    """
    Register the message handler on the Hydrogram (MTProto) client.

    In Hydrogram, on_message receives ALL incoming message updates including
    channel posts (UpdateNewChannelMessage) — there is no separate
    on_channel_post decorator (that is a Bot API / python-telegram-bot concept).

    For both public and private channels, the userbot MUST be a joined member
    to receive push updates. ensure_source_channel_membership() in main.py
    handles joining at startup when the userbot is not already a member.

    The handler is intentionally thin — validate, normalize, enqueue.
    Optionally increments a Redis daily received counter for metrics.

    Diagnostic aids (added to detect silent update failures):
      - catch-all on_raw_update logs every MTProto update type at DEBUG level
      - LISTENER_LAST_RECEIVED_KEY is written to Redis on every accepted message
        so the liveness health check can detect when updates silently stop
    """
    logger.info(
        f"Registering listener on source_chat_id={source_chat_id} "
        f"(handler=on_message, filter=filters.chat)"
    )

    async def _handle(client: Client, message: Message):
        """Shared handler: validate, normalize, enqueue. No heavy work here."""
        # ── Startup backlog filter ────────────────────────────────────────
        # Telegram pushes missed updates on reconnect.  For large channels
        # this backlog can be huge and trigger flood-waits before we even
        # start processing real-time messages.  Drop anything older than
        # the moment the bot started.
        if bot_start_time is not None:
            msg_ts = int(message.date.timestamp()) if hasattr(message.date, "timestamp") else int(message.date)
            # Allow a 300-second (5-minute) grace window for machine clock skew
            # or startup latency so legitimate real-time messages are not discarded.
            if msg_ts < (bot_start_time - 300):
                logger.info(
                    f"Dropping backlog message {message.id} "
                    f"(date={msg_ts} < cutoff={bot_start_time - 300}) — pre-startup backlog, skipped"
                )
                return

        payload = normalize_message(message)
        if payload is None:
            return  # Unsupported message type

        # Track liveness and sequence — both written on every push-delivered
        # message so the health check and catch-up handler have accurate state.
        if redis_client is not None:
            try:
                import time
                await redis_client.set(
                    LISTENER_LAST_RECEIVED_KEY,
                    str(time.time()),
                    ex=86400,  # auto-expire after 24 h so new sessions start clean
                )
            except Exception as e:
                logger.debug(f"Liveness timestamp write skipped: {e}")

            try:
                await redis_client.set(
                    f"{LISTENER_LAST_MSG_ID_PREFIX}:{message.chat.id}",
                    str(message.id),
                    ex=86400,
                )
            except Exception as e:
                logger.debug(f"Last msg ID write skipped: {e}")

        if redis_client is not None:
            try:
                from metrics import incr_received

                await incr_received(redis_client)
            except Exception as e:
                logger.debug(f"Received counter skipped: {e}")

        try:
            # Non-blocking put — never suspends the dispatcher.
            # If the queue is full, spill to Redis so no messages are lost
            # and Hydrogram can immediately deliver the next update.
            message_queue.put_nowait(payload)
            logger.info(
                f"Enqueued message {message.id} type={payload['type']} "
                f"(queue={message_queue.qsize()}/{message_queue.maxsize}) "
                f"media_group={payload.get('media_group_id')}"
            )
        except asyncio.QueueFull:
            # Queue at capacity — spill to Redis overflow list.
            # overflow_drainer() will refill the queue as workers drain it.
            if redis_client is not None:
                try:
                    import json as _json
                    overflow_depth = await redis_client.rpush(
                        LISTENER_OVERFLOW_KEY,
                        _json.dumps(payload, default=str),
                    )
                    logger.warning(
                        f"Queue full — message {message.id} spilled to Redis overflow "
                        f"(overflow depth={overflow_depth}, "
                        f"queue={message_queue.qsize()}/{message_queue.maxsize})"
                    )
                except Exception as oe:
                    logger.error(
                        f"Queue full AND Redis overflow failed — "
                        f"dropping message {message.id}: {oe}"
                    )
            else:
                logger.error(
                    f"Queue full, no Redis client — dropping message {message.id}"
                )

    # Hydrogram routes channel posts (UpdateNewChannelMessage) and regular
    # messages (UpdateNewMessage) through the same on_message handler.
    # filters.chat() matches by numeric ID for both public and private channels.
    @app.on_message(filters.chat(source_chat_id))
    async def on_message(client: Client, message: Message):
        await _handle(client, message)

    # ── Raw update handler — PTS-aware catch-up ─────────────────────────────────
    # Handles UpdateChannelTooLong (channel-specific PTS desync, most precise)
    # and the generic UpdatesTooLong sentinel (session-wide fallback).
    # Uses listener:last_msg_id:{chat_id} as a watermark so only the true gap
    # is fetched — no static cooldown that could permanently stall on busy channels.
    @app.on_raw_update()
    async def _on_raw_update(client: Client, update, users, chats):
        logger.debug(f"[RAW UPDATE] type={type(update).__name__}")

        # UpdateChannelTooLong — channel-specific PTS desync (most precise signal)
        if UpdateChannelTooLong is not None and isinstance(update, UpdateChannelTooLong):
            raw_channel_id = getattr(update, "channel_id", None)
            if raw_channel_id is None:
                return
            channel_chat_id = int(f"-100{raw_channel_id}")
            if channel_chat_id != source_chat_id:
                return  # Not our source channel
            logger.warning(
                f"UpdateChannelTooLong for channel {channel_chat_id} — "
                "running targeted PTS catch-up"
            )
            await _do_channel_catchup(
                client, channel_chat_id, redis_client, message_queue, bot_start_time
            )
            return

        # Generic UpdatesTooLong — session-wide fallback
        if isinstance(update, UpdatesTooLong):
            logger.warning(
                "UpdatesTooLong (generic) — running catch-up on source channel"
            )
            await _do_channel_catchup(
                client, source_chat_id, redis_client, message_queue, bot_start_time
            )


async def _do_channel_catchup(
    client: Client,
    channel_chat_id: int,
    redis_client,
    message_queue: asyncio.Queue,
    bot_start_time: Optional[int],
) -> int:
    """
    Fetch messages newer than the last known ID from channel_chat_id and enqueue them.

    Uses listener:last_msg_id:{chat_id} as a watermark so only the genuine gap
    is fetched. Processes oldest-first to preserve reply-thread ordering.
    Returns the count of messages re-queued.
    """
    last_msg_id = 0
    if redis_client is not None:
        try:
            raw = await redis_client.get(f"{LISTENER_LAST_MSG_ID_PREFIX}:{channel_chat_id}")
            if raw is not None:
                last_msg_id = int(raw)
        except Exception:
            pass

    messages_to_process: list = []  # list of (msg_id, payload)
    try:
        async for message in client.get_chat_history(channel_chat_id, limit=100):
            if last_msg_id > 0 and message.id <= last_msg_id:
                break  # Reached the watermark — gap is fully covered
            if bot_start_time is not None:
                msg_ts = (
                    int(message.date.timestamp())
                    if hasattr(message.date, "timestamp")
                    else int(message.date)
                )
                if msg_ts < (bot_start_time - 300):
                    continue
            payload = normalize_message(message)
            if payload is not None:
                messages_to_process.append((message.id, payload))
    except FloodWait as fw:
        logger.warning(f"FloodWait {fw.value}s during channel catch-up for {channel_chat_id}")
        return 0
    except Exception as ex:
        logger.error(f"Channel catch-up history fetch failed: {ex}", exc_info=True)
        return 0

    # Oldest-first so reply-thread ordering is preserved
    messages_to_process.reverse()

    fetched = 0
    newest_id = last_msg_id
    for msg_id, payload in messages_to_process:
        try:
            message_queue.put_nowait(payload)
            fetched += 1
            if msg_id > newest_id:
                newest_id = msg_id
        except asyncio.QueueFull:
            # Spill to Redis overflow instead of abandoning the catch-up.
            if redis_client is not None:
                try:
                    import json as _json
                    overflow_depth = await redis_client.rpush(
                        LISTENER_OVERFLOW_KEY,
                        _json.dumps(payload, default=str),
                    )
                    fetched += 1
                    if msg_id > newest_id:
                        newest_id = msg_id
                    logger.warning(
                        f"Catch-up queue full — message {msg_id} spilled to overflow "
                        f"(depth={overflow_depth})"
                    )
                except Exception as oe:
                    logger.error(
                        f"Catch-up: queue full AND overflow failed for {msg_id}: {oe}"
                    )
                    break
            else:
                logger.warning(
                    f"Queue full during catch-up for {channel_chat_id} — "
                    f"stopped at message {msg_id} (no Redis to spill to)"
                )
                break

    # Advance the watermark and refresh the liveness key
    if redis_client is not None and newest_id > last_msg_id:
        try:
            await redis_client.set(
                f"{LISTENER_LAST_MSG_ID_PREFIX}:{channel_chat_id}",
                str(newest_id),
                ex=86400,
            )
            if fetched > 0:
                await redis_client.set(
                    LISTENER_LAST_RECEIVED_KEY,
                    str(time.time()),
                    ex=86400,
                )
        except Exception:
            pass

    # Snapshot the current server-side PTS after a successful catch-up.
    # The silence watchdog reads this to confirm whether a stall is real
    # (server PTS advanced past our snapshot) or organic silence.
    if redis_client is not None:
        try:
            import hydrogram.raw.functions.updates as _upd
            state = await client.invoke(_upd.GetState())
            await redis_client.set(LISTENER_LAST_PTS_KEY, str(state.pts), ex=86400)
            logger.debug(f"Channel catch-up: PTS snapshot written ({state.pts})")
        except Exception as pts_err:
            logger.debug(f"Channel catch-up: PTS snapshot skipped: {pts_err}")

    watermark_label = f" (watermark msg_id={last_msg_id})" if last_msg_id else ""
    if fetched > 0:
        logger.info(
            f"Channel catch-up {channel_chat_id}: re-queued {fetched} message(s){watermark_label}"
        )
    else:
        logger.debug(
            f"Channel catch-up {channel_chat_id}: re-queued {fetched} message(s){watermark_label}"
        )
    return fetched


async def overflow_drainer(
    message_queue: asyncio.Queue,
    redis_client,
    shutdown_event: asyncio.Event,
):
    """
    Background task that drains the Redis overflow list back into the asyncio.Queue.

    When on_message can't put_nowait() (queue is at capacity), messages spill
    to LISTENER_OVERFLOW_KEY in Redis. This drainer polls Redis and refills
    the asyncio.Queue as workers free up slots — ensuring no messages are lost
    even during sustained high-volume bursts on large channels.

    Design:
      - Only attempts to drain when the queue has at least one free slot.
      - If put_nowait() races and the queue fills between the check and the put,
        the payload is pushed back to the FRONT of the Redis list (lpush) so
        ordering is preserved.
      - Polls at 200 ms when overflow is empty; tighter at 50 ms when draining.
    """
    import json as _json

    logger.info("Overflow drainer started")
    while not shutdown_event.is_set():
        try:
            # Wait for a free slot before even touching Redis
            if message_queue.full():
                await asyncio.sleep(0.1)
                continue

            payload_json = await redis_client.lpop(LISTENER_OVERFLOW_KEY)
            if payload_json is None:
                # Overflow list is empty — nothing to drain, sleep longer
                await asyncio.sleep(0.2)
                continue

            payload = _json.loads(payload_json)
            try:
                message_queue.put_nowait(payload)
                remaining = await redis_client.llen(LISTENER_OVERFLOW_KEY)
                logger.info(
                    f"Overflow drainer: requeued message {payload.get('message_id')} "
                    f"(queue={message_queue.qsize()}/{message_queue.maxsize}, "
                    f"overflow remaining={remaining})"
                )
                # Keep draining at high speed while overflow has items
                if remaining > 0:
                    continue
                await asyncio.sleep(0.05)
            except asyncio.QueueFull:
                # Race: queue filled between our check and the put — push back to front
                await redis_client.lpush(LISTENER_OVERFLOW_KEY, payload_json)
                await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            logger.info("Overflow drainer cancelled — shutting down")
            return
        except Exception as e:
            logger.error(f"Overflow drainer error: {e}", exc_info=True)
            await asyncio.sleep(1.0)


# make_fallback_poller_factory removed.
# The get_chat_history polling loop was a high-ban-risk approach (Method 4).
# Recovery from missed updates is now handled exclusively by two safe mechanisms:
#   1. UpdateChannelTooLong / UpdatesTooLong raw update → _do_channel_catchup()
#      (event-driven, only fires when Telegram tells us we missed something)
#   2. PTS-audited silence watchdog in main.py
#      (compares updates.GetState() PTS vs listener:last_pts before recycling)
