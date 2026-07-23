"""
Forward attribution — decide when to use Bot API forwardMessage to preserve
the "Forwarded from" channel tag on destination posts.

Eligibility (all required):
  - Feature enabled in channels.yml
  - Message has a channel forward origin (MessageOriginChannel / legacy fields)
  - Sender bot is admin in that origin channel (allowlist and/or getChatMember)
"""

import logging
import time
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from telegram_sender import TelegramBotSender

logger = logging.getLogger(__name__)

# Cache TTL for origin-channel admin checks (seconds)
_ADMIN_CACHE_TTL = 300


def serialize_forward_origin(message) -> Optional[Dict[str, Any]]:
    """
    Extract channel-forward metadata from a Hydrogram Message.

    Supports both modern `forward_origin` (MessageOriginChannel) and
    legacy `forward_from_chat` / `forward_from_message_id` fields.
    Returns None for non-channel forwards (user, hidden, chat, etc.).
    """
    # Prefer modern MessageOriginChannel when available
    origin = getattr(message, "forward_origin", None)
    if origin is not None:
        chat = getattr(origin, "chat", None)
        message_id = getattr(origin, "message_id", None)
        # Channel origins have both chat and message_id; other origin types do not
        if chat is not None and message_id is not None and getattr(chat, "id", None) is not None:
            type_name = _origin_type_name(origin)
            if type_name and type_name != "channel":
                return None
            result: Dict[str, Any] = {
                "type": "channel",
                "chat_id": int(chat.id),
                "message_id": int(message_id),
            }
            date = getattr(origin, "date", None)
            if date is not None:
                result["date"] = date.isoformat() if hasattr(date, "isoformat") else str(date)
            signature = getattr(origin, "author_signature", None)
            if signature:
                result["author_signature"] = signature
            return result
        return None

    # Legacy Pyrogram fields
    fwd_chat = getattr(message, "forward_from_chat", None)
    fwd_msg_id = getattr(message, "forward_from_message_id", None)
    if fwd_chat is not None and fwd_msg_id is not None and getattr(fwd_chat, "id", None) is not None:
        # Only channel/supergroup chats can be re-forwarded with attribution
        chat_type = getattr(fwd_chat, "type", None)
        type_val = chat_type.value if hasattr(chat_type, "value") else str(chat_type) if chat_type else ""
        if type_val and type_val not in ("channel", "supergroup", "ChatType.CHANNEL", "ChatType.SUPERGROUP"):
            # Still allow if type string contains "channel"
            if "channel" not in type_val.lower():
                return None

        result = {
            "type": "channel",
            "chat_id": int(fwd_chat.id),
            "message_id": int(fwd_msg_id),
        }
        fwd_date = getattr(message, "forward_date", None)
        if fwd_date is not None:
            result["date"] = fwd_date.isoformat() if hasattr(fwd_date, "isoformat") else str(fwd_date)
        signature = getattr(message, "forward_signature", None)
        if signature:
            result["author_signature"] = signature
        return result

    return None


def _origin_type_name(origin) -> Optional[str]:
    """Normalize MessageOrigin.type to a lowercase string like 'channel'."""
    t = getattr(origin, "type", None)
    if t is None:
        # Infer from class name if type enum missing
        name = type(origin).__name__
        if "Channel" in name:
            return "channel"
        return None
    if hasattr(t, "value"):
        return str(t.value).lower()
    return str(t).lower().replace("messageorigintype.", "")


def extract_channel_origin(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Get a single channel forward origin from a message or album payload.

    For albums: all items must share the same origin chat_id; otherwise None
    (mixed attribution → fall back to copy/send).
    """
    if payload.get("type") == "album":
        items = payload.get("items") or []
        if not items:
            return None
        origins = []
        for item in items:
            fo = item.get("forward_origin")
            if not fo or fo.get("type") != "channel":
                return None
            if fo.get("chat_id") is None or fo.get("message_id") is None:
                return None
            origins.append(fo)
        chat_ids = {int(o["chat_id"]) for o in origins}
        if len(chat_ids) != 1:
            logger.info(
                "Album has mixed/missing forward origins — skipping native forward"
            )
            return None
        # Return representative origin; message_ids collected separately
        return {
            "type": "channel",
            "chat_id": int(origins[0]["chat_id"]),
            "message_ids": [int(o["message_id"]) for o in origins],
            "message_id": int(origins[0]["message_id"]),
        }

    fo = payload.get("forward_origin")
    if not fo or fo.get("type") != "channel":
        return None
    if fo.get("chat_id") is None or fo.get("message_id") is None:
        return None
    return {
        "type": "channel",
        "chat_id": int(fo["chat_id"]),
        "message_id": int(fo["message_id"]),
        "message_ids": [int(fo["message_id"])],
    }


class ForwardAttributionChecker:
    """
    Evaluates whether a payload should use native Bot API forwardMessage.

    Caches getChatMember results for origin channels (TTL ~5 min).
    """

    def __init__(self, sender: "TelegramBotSender"):
        self.sender = sender
        self._bot_id: Optional[int] = None
        # chat_id -> (is_admin: bool, expires_at: float)
        self._admin_cache: Dict[int, tuple] = {}

    async def _get_bot_id(self) -> int:
        if self._bot_id is None:
            me = await self.sender.get_me()
            self._bot_id = int(me["id"])
        return self._bot_id

    def _allowlist_ids(self, config) -> Optional[set]:
        """
        Return set of allowed origin chat IDs, or None if allowlist is empty
        (meaning: use runtime getChatMember for any origin).
        """
        settings = getattr(config, "settings", config)
        fa = getattr(settings, "forward_attribution", None)
        if fa is None:
            return set()  # no config → treat as empty allowlist with enabled check separate
        allowed = getattr(fa, "allowed_origin_channels", None) or []
        if not allowed:
            return None  # empty allowlist → runtime check
        return {int(c.chat_id if hasattr(c, "chat_id") else c["chat_id"]) for c in allowed}

    def _feature_enabled(self, config) -> bool:
        settings = getattr(config, "settings", config)
        fa = getattr(settings, "forward_attribution", None)
        if fa is None:
            return False
        return bool(getattr(fa, "enabled", False))

    async def is_bot_admin_in_channel(self, chat_id: int) -> bool:
        """Return True if sender bot is administrator/creator in chat_id."""
        now = time.time()
        cached = self._admin_cache.get(chat_id)
        if cached and cached[1] > now:
            return cached[0]

        is_admin = False
        try:
            bot_id = await self._get_bot_id()
            member = await self.sender.get_chat_member(chat_id, bot_id)
            status = (member.get("status") or "").lower()
            is_admin = status in ("administrator", "creator")
            if is_admin:
                # Prefer can_post_messages when present (channels); creators always ok
                perms = member.get("can_post_messages")
                if perms is False and status != "creator":
                    is_admin = False
        except Exception as e:
            err_msg = str(e)
            if "chat not found" in err_msg.lower() or "chat_id_invalid" in err_msg.lower():
                logger.debug(f"Sender bot is not a member of origin channel {chat_id} (normal for external forwards): {e}")
            else:
                logger.warning(f"Admin check failed for origin {chat_id}: {e}")
            is_admin = False

        self._admin_cache[chat_id] = (is_admin, now + _ADMIN_CACHE_TTL)
        return is_admin

    async def should_use_native_forward(
        self,
        payload: Dict[str, Any],
        config,
    ) -> bool:
        """
        Decide if this payload should be delivered via forwardMessage.

        When True, word replacements and media relay are skipped.
        """
        if not self._feature_enabled(config):
            return False

        origin = extract_channel_origin(payload)
        if not origin:
            return False

        origin_chat_id = int(origin["chat_id"])
        allowlist = self._allowlist_ids(config)

        if allowlist is not None:
            if origin_chat_id not in allowlist:
                logger.debug(
                    f"Origin {origin_chat_id} not in allowed_origin_channels — "
                    "skipping native forward"
                )
                return False
            # On allowlist: still verify bot can actually access (admin check)
            if not await self.is_bot_admin_in_channel(origin_chat_id):
                logger.warning(
                    f"Origin {origin_chat_id} is allowlisted but bot is not admin there"
                )
                return False
            return True

        # Empty allowlist → runtime getChatMember for any channel origin
        return await self.is_bot_admin_in_channel(origin_chat_id)


def attach_native_forward_flag(payload: Dict[str, Any], use: bool) -> Dict[str, Any]:
    """Mark payload for native forward delivery (mutates and returns payload)."""
    payload["use_native_forward"] = use
    if use:
        origin = extract_channel_origin(payload)
        if origin:
            payload["native_forward_origin"] = {
                "chat_id": origin["chat_id"],
                "message_id": origin["message_id"],
                "message_ids": origin.get("message_ids") or [origin["message_id"]],
            }
            logger.info(
                f"Using native forward from origin chat={origin['chat_id']} "
                f"ids={origin.get('message_ids')} (word replacements skipped)"
            )
    return payload
