"""
Userbot media relay — copy messages from source to a bot-accessible chat.

Preferred: private relay channel (RELAY_CHANNEL_ID) where userbot posts and
the sender bot copies — message IDs match Bot API reliably.

Fallback: userbot DM with sender bot + Bot API getUpdates to resolve message IDs.

Protected content channels (Restrict Saving Content enabled) are handled by
downloading media and re-uploading to the relay target instead of using
copy_message / copy_media_group which Telegram blocks.
"""

import asyncio
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass
from typing import List, Optional, TYPE_CHECKING

from hydrogram import Client
from hydrogram.errors import FloodWait

if TYPE_CHECKING:
    from telegram_sender import TelegramBotSender

logger = logging.getLogger(__name__)


@dataclass
class RelayConfig:
    """Relay target shared by userbot (post) and sender bot (copyMessage source)."""

    userbot_target: int
    bot_from_chat: int
    mode: str = "channel"  # "channel" or "dm"


# ── Error classification helpers ──────────────────────────────────────────


def _is_forwards_restricted(exc: Exception) -> bool:
    """True if the error is a CHAT_FORWARDS_RESTRICTED / protected content error."""
    msg = str(exc).upper()
    return "CHAT_FORWARDS_RESTRICTED" in msg or "FORWARDS_RESTRICTED" in msg


def _is_peer_id_invalid(exc: Exception) -> bool:
    """True if the error indicates a stale or missing peer cache entry."""
    return "PEER_ID_INVALID" in str(exc).upper()


def _is_message_ids_empty(exc: Exception) -> bool:
    """True if the error is a MESSAGE_IDS_EMPTY bad request."""
    return "MESSAGE_IDS_EMPTY" in str(exc).upper()


# ── Config resolution ────────────────────────────────────────────────────


async def resolve_relay_config(
    sender: "TelegramBotSender",
    user_id: int,
    relay_channel_id: int = 0,
) -> RelayConfig:
    """
    Build relay config.

    Channel mode (recommended): userbot + bot both access the same channel.
    DM mode: userbot posts to bot; Bot API message ids resolved via getUpdates.
    """
    if relay_channel_id:
        return RelayConfig(
            userbot_target=relay_channel_id,
            bot_from_chat=relay_channel_id,
            mode="channel",
        )

    bot_me = await sender.get_me()
    return RelayConfig(
        userbot_target=bot_me["id"],
        bot_from_chat=user_id,
        mode="dm",
    )


async def ensure_relay_chat(
    app: Client,
    sender: "TelegramBotSender",
    relay: RelayConfig,
) -> bool:
    """Verify relay is accessible; for DM mode auto-send /start to open bot chat."""
    if relay.mode == "channel":
        if await sender.verify_bot_access(relay.bot_from_chat):
            logger.info(f"Relay channel ready (chat_id={relay.bot_from_chat})")
            return True
        logger.error(
            f"Sender bot cannot access relay channel {relay.bot_from_chat}. "
            "Add the bot as admin to the relay channel."
        )
        return False

    if await sender.verify_bot_access(relay.bot_from_chat):
        logger.info(
            f"Relay DM ready (userbot→{relay.userbot_target}, "
            f"bot copies from user {relay.bot_from_chat})"
        )
        return True

    try:
        bot_me = await sender.get_me()
        bot_username = bot_me.get("username")
        if not bot_username:
            logger.error("Sender bot has no @username — cannot auto-open relay chat")
            return False

        logger.info(f"Opening relay chat — userbot sending /start to @{bot_username}")
        await app.send_message(bot_username, "/start")
        await asyncio.sleep(2)

        if await sender.verify_bot_access(relay.bot_from_chat):
            logger.info(f"Relay DM opened via /start (user={relay.bot_from_chat})")
            return True

        logger.error(
            f"Relay DM still inaccessible (user={relay.bot_from_chat}). "
            "Send /start to the sender bot manually."
        )
    except Exception as e:
        logger.error(f"Failed to auto-open relay chat: {e}", exc_info=True)

    return False


# ── Internal relay helpers ────────────────────────────────────────────────


def _parse_mode_html():
    try:
        from hydrogram.enums import ParseMode
        return ParseMode.HTML
    except Exception:
        return "html"


def _text_for_relay(msg) -> tuple:
    """Return (text, parse_mode) preserving Hydrogram HTML entities when present."""
    if not msg.text:
        return "", None
    if getattr(msg.text, "entities", None) and hasattr(msg.text, "html"):
        html = str(msg.text.html)
        if html and html != "None":
            return html, _parse_mode_html()
    plain = str(msg.text)
    return (plain if plain != "None" else ""), None


def _caption_for_relay(msg) -> tuple:
    """Return (caption, parse_mode) preserving caption entities when present."""
    cap = getattr(msg, "caption", None)
    if cap is None:
        return "", None
    if getattr(cap, "entities", None) and hasattr(cap, "html"):
        html = str(cap.html)
        if html and html != "None":
            return html, _parse_mode_html()
    plain = str(cap)
    if plain == "None":
        return "", None
    return plain, None


async def _direct_copy(
    app: Client,
    target: int,
    source_chat_id: int,
    message_ids: List[int],
    is_album: bool,
) -> List[int]:
    """Standard copy_message / copy_media_group (fast path).

    For albums we fetch each item by its known message_id and re-send them as
    a media group using file_ids.  This bypasses Telegram's messages.getMediaGroup
    RPC (used by copy_media_group) which is capped at ±5 items around the anchor
    and silently truncates albums larger than ~6 items when anchored at item[0].
    """
    if is_album and len(message_ids) > 1:
        from hydrogram.types import (
            InputMediaPhoto,
            InputMediaVideo,
            InputMediaDocument,
            InputMediaAudio,
        )

        # Fetch every item by its explicit ID (replies=0 avoids fetching reply
        # parent messages which can raise MESSAGE_IDS_EMPTY in protected channels).
        messages = await app.get_messages(source_chat_id, message_ids, replies=0)
        if not isinstance(messages, list):
            messages = [messages]
        messages = sorted(
            [m for m in messages if m and not m.empty],
            key=lambda m: m.id,
        )

        if not messages:
            raise RuntimeError(
                f"No fetchable messages for album IDs {message_ids} "
                f"in chat {source_chat_id}"
            )

        # Build InputMedia list using file_ids (no disk download required).
        media_group = []
        for msg in messages:
            caption, parse_mode = _caption_for_relay(msg)
            cap_kwargs = {"caption": caption}
            if parse_mode:
                cap_kwargs["parse_mode"] = parse_mode
            if msg.photo:
                media_group.append(InputMediaPhoto(msg.photo.file_id, **cap_kwargs))
            elif msg.video:
                media_group.append(InputMediaVideo(msg.video.file_id, **cap_kwargs))
            elif msg.audio:
                media_group.append(InputMediaAudio(msg.audio.file_id, **cap_kwargs))
            elif msg.document:
                media_group.append(InputMediaDocument(msg.document.file_id, **cap_kwargs))
            else:
                # Unsupported type — skip with a warning rather than crashing.
                logger.warning(
                    f"Unsupported album item type for msg {msg.id} "
                    "— skipping item in relay"
                )

        if not media_group:
            raise RuntimeError(
                "No supported media items found for album relay (all items unsupported type)"
            )

        sent = await app.send_media_group(target, media_group)
        return [m.id for m in sent]

    else:
        copied = await app.copy_message(
            chat_id=target,
            from_chat_id=source_chat_id,
            message_id=message_ids[0],
        )
        return [copied.id]


async def _relay_via_download(
    app: Client,
    target: int,
    source_chat_id: int,
    message_ids: List[int],
    is_album: bool = False,
) -> List[int]:
    """
    Fallback relay for protected-content channels.

    Downloads media from the source channel and re-uploads to the relay
    target.  This bypasses Telegram's CHAT_FORWARDS_RESTRICTED block
    because the bot is creating a *new* message rather than copying.
    """
    temp_dir = tempfile.mkdtemp(prefix="tg_relay_")

    try:
        # Pass replies=0 to prevent Hydrogram from automatically fetching the
        # reply-to message of each album item (default replies=1).  When an
        # album item is a reply, Hydrogram would try to hydrate that parent
        # message via channels.GetMessages, which raises MESSAGE_IDS_EMPTY when
        # the parent is from a protected channel or was deleted.  We only need
        # the media messages themselves for the download+reupload relay.
        messages = await app.get_messages(source_chat_id, message_ids, replies=0)
        if not isinstance(messages, list):
            messages = [messages]
        # Filter out empty/service messages and sort by ID
        messages = sorted(
            [m for m in messages if m and not m.empty],
            key=lambda m: m.id,
        )

        if not messages:
            raise RuntimeError(
                f"No fetchable messages for IDs {message_ids} "
                f"in protected channel {source_chat_id}"
            )

        if is_album and len(messages) > 1:
            return await _download_reupload_album(app, target, messages, temp_dir)
        else:
            return await _download_reupload_single(app, target, messages[0], temp_dir)

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def _get_download_path(msg, temp_dir: str) -> str:
    """Determine a safe target path with a valid file extension."""
    ext = ".bin"
    if getattr(msg, "photo", None):
        ext = ".jpg"
    elif getattr(msg, "video", None) or getattr(msg, "video_note", None) or getattr(msg, "animation", None):
        ext = ".mp4"
    elif getattr(msg, "audio", None):
        ext = ".mp3"
    elif getattr(msg, "voice", None):
        ext = ".ogg"
    elif getattr(msg, "sticker", None):
        ext = ".webp"
    elif getattr(msg, "document", None) and getattr(msg.document, "file_name", None):
        orig_name = msg.document.file_name
        if "." in orig_name:
            ext = "." + orig_name.rsplit(".", 1)[-1]
    return os.path.join(temp_dir, f"{msg.id}{ext}")


async def _download_reupload_single(
    app: Client,
    target: int,
    msg,
    temp_dir: str,
) -> List[int]:
    """Download one message's media and re-send to relay target."""
    # Text-only messages (relayed for rich entities)
    if msg.text and not any([
        msg.photo, msg.video, msg.document, msg.audio,
        msg.voice, msg.video_note, msg.animation, msg.sticker,
    ]):
        text, parse_mode = _text_for_relay(msg)
        if parse_mode:
            sent = await app.send_message(target, text, parse_mode=parse_mode)
        else:
            sent = await app.send_message(target, text)
        return [sent.id]

    file_path = await app.download_media(
        msg, file_name=_get_download_path(msg, temp_dir),
    )
    if not file_path:
        raise RuntimeError(
            f"Failed to download media for message {msg.id} "
            "(protected content channel)"
        )

    caption, parse_mode = _caption_for_relay(msg)
    cap_kwargs = {}
    if caption:
        cap_kwargs["caption"] = caption
    if parse_mode:
        cap_kwargs["parse_mode"] = parse_mode

    if msg.photo:
        sent = await app.send_photo(target, file_path, **cap_kwargs)
    elif msg.video:
        sent = await app.send_video(target, file_path, **cap_kwargs)
    elif msg.document:
        sent = await app.send_document(target, file_path, **cap_kwargs)
    elif msg.audio:
        sent = await app.send_audio(target, file_path, **cap_kwargs)
    elif msg.voice:
        sent = await app.send_voice(target, file_path, **cap_kwargs)
    elif msg.video_note:
        sent = await app.send_video_note(target, file_path)
    elif msg.animation:
        sent = await app.send_animation(target, file_path, **cap_kwargs)
    elif msg.sticker:
        sent = await app.send_sticker(target, file_path)
    else:
        # Unknown media type — send as document
        sent = await app.send_document(target, file_path, **cap_kwargs)

    return [sent.id]


async def _download_reupload_album(
    app: Client,
    target: int,
    messages: list,
    temp_dir: str,
) -> List[int]:
    """Download album items and re-send as a media group."""
    from hydrogram.types import (
        InputMediaPhoto,
        InputMediaVideo,
        InputMediaDocument,
        InputMediaAudio,
    )

    media_group = []
    for msg in messages:
        file_path = await app.download_media(
            msg, file_name=_get_download_path(msg, temp_dir),
        )
        if not file_path:
            logger.warning(
                f"Skipping album item {msg.id} — media download failed "
                "(protected content)"
            )
            continue

        # Guard: Hydrogram may stringify a missing caption as the literal "None".
        caption, parse_mode = _caption_for_relay(msg)
        cap_kwargs = {"caption": caption}
        if parse_mode:
            cap_kwargs["parse_mode"] = parse_mode

        if msg.photo:
            media_group.append(InputMediaPhoto(file_path, **cap_kwargs))
        elif msg.video:
            media_group.append(InputMediaVideo(file_path, **cap_kwargs))
        elif msg.audio:
            media_group.append(InputMediaAudio(file_path, **cap_kwargs))
        else:
            # document, animation, etc.
            media_group.append(InputMediaDocument(file_path, **cap_kwargs))

    if not media_group:
        raise RuntimeError(
            "No media items could be downloaded for protected-content album"
        )

    sent = await app.send_media_group(target, media_group)
    return [m.id for m in sent]


# ── Public API ────────────────────────────────────────────────────────────


async def relay_to_bot(
    app: Client,
    sender: "TelegramBotSender",
    source_chat_id: int,
    message_ids: List[int],
    relay: RelayConfig,
    is_album: bool = False,
) -> List[int]:
    """
    Copy message(s) from source into relay, return Bot-API-compatible message ids.

    Handles three common failure modes in large channels:
      - PEER_ID_INVALID: re-resolves the peer access hash and retries
      - CHAT_FORWARDS_RESTRICTED: downloads media and re-uploads instead of copying
      - MESSAGE_IDS_EMPTY: validates IDs before attempting any API call
    """
    if not message_ids:
        return []

    # Filter out invalid IDs (null, zero) — protects against MESSAGE_IDS_EMPTY
    message_ids = [mid for mid in message_ids if mid and int(mid) > 0]
    if not message_ids:
        logger.warning("All message IDs were invalid (null/zero) — skipping relay")
        return []

    target = relay.userbot_target
    hydrogram_ids: Optional[List[int]] = None

    # ── Step 1: Copy messages to relay target ──────────────────────────
    try:
        hydrogram_ids = await _direct_copy(
            app, target, source_chat_id, message_ids, is_album,
        )
    except FloodWait:
        raise
    except Exception as e:
        if _is_peer_id_invalid(e):
            # Stale peer cache — re-resolve and retry once
            logger.warning(
                f"PEER_ID_INVALID for chat {source_chat_id} — "
                "re-resolving peer and retrying relay"
            )
            try:
                await app.resolve_peer(source_chat_id)
                hydrogram_ids = await _direct_copy(
                    app, target, source_chat_id, message_ids, is_album,
                )
            except FloodWait:
                raise
            except Exception as retry_err:
                if _is_forwards_restricted(retry_err):
                    # Protected content — will fall through to download path
                    pass
                else:
                    logger.error(
                        f"Relay retry after peer re-resolve failed: {retry_err}",
                        exc_info=True,
                    )
                    raise

        elif _is_forwards_restricted(e):
            # Protected content — will fall through to download path
            pass

        elif _is_message_ids_empty(e):
            logger.warning(
                f"MESSAGE_IDS_EMPTY for {message_ids} from {source_chat_id} — "
                "message may have been deleted"
            )
            raise

        else:
            logger.error(
                f"Failed to relay message(s) {message_ids} from "
                f"{source_chat_id}: {e}",
                exc_info=True,
            )
            raise

    # ── Step 1b: Protected content fallback (download + reupload) ──────
    if hydrogram_ids is None:
        logger.warning(
            f"Source channel {source_chat_id} has restricted forwarding — "
            "downloading media and re-uploading to relay target"
        )
        try:
            hydrogram_ids = await _relay_via_download(
                app, target, source_chat_id, message_ids, is_album,
            )
        except FloodWait:
            raise
        except Exception as dl_err:
            logger.error(
                f"Protected-content relay (download+reupload) failed: {dl_err}",
                exc_info=True,
            )
            raise

    count = len(hydrogram_ids)
    logger.info(
        f"Relayed {'album' if is_album else 'message'} "
        f"({count} item{'s' if count != 1 else ''}) "
        f"source={source_chat_id} → {relay.mode} {target}"
    )

    # ── Step 2: Resolve Bot API message IDs (DM mode only) ─────────────
    if relay.mode == "channel":
        return hydrogram_ids

    bot_ids = await sender.resolve_relay_message_ids(
        relay.bot_from_chat, count=len(hydrogram_ids),
    )
    logger.info(
        f"Resolved Bot API relay ids: {bot_ids} (hydrogram {hydrogram_ids})"
    )
    return bot_ids


async def cleanup_relay(
    app: Optional[Client],
    relay: RelayConfig,
    relay_message_ids: List[int],
):
    """Delete relay messages after successful delivery (best-effort)."""
    if not app or not relay_message_ids:
        return
    try:
        await app.delete_messages(relay.userbot_target, relay_message_ids)
        logger.debug(f"Cleaned up relay messages {relay_message_ids}")
    except Exception as e:
        logger.debug(f"Relay cleanup skipped: {e}")
