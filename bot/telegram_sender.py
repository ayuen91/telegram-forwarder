"""
Telegram Bot API sender — delivers to destination channels.

Text is sent with sendMessage (content from the userbot payload).
Media is copied from a userbot relay chat via copyMessage / copyMessages
so the sender bot never needs access to the source channel.
"""

import asyncio
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import aiohttp

logger = logging.getLogger(__name__)


def _normalize_html_for_bot_api(html: str) -> str:
    """Normalize Pyrogram-generated HTML to tags accepted by the Bot API HTML parser.

    Pyrogram's HTML.unparse() produces tags that differ from what the Telegram
    Bot API HTML mode expects for two entity types:

      * Spoiler:      Pyrogram → <spoiler>…</spoiler>
                      Bot API  → <tg-spoiler>…</tg-spoiler>

      * Custom emoji: Pyrogram → <emoji id="123">…</emoji>
                      Bot API  → <tg-emoji emoji-id="123">…</tg-emoji>

      * Expandable blockquote: Hydrogram may produce <blockquote expandable >
                      Bot API  → <blockquote expandable>  (no trailing space)

    Without this normalisation those entities are silently stripped (or cause a
    "Failed to parse entities" error) whenever we call sendMessage with
    parse_mode="HTML" — e.g. after a word-replacement rule fires.
    """
    # <spoiler> → <tg-spoiler>
    html = re.sub(r'<spoiler>', '<tg-spoiler>', html)
    html = re.sub(r'</spoiler>', '</tg-spoiler>', html)
    # <emoji id="…"> → <tg-emoji emoji-id="…">
    html = re.sub(r'<emoji\s+id="([^"]+)">', r'<tg-emoji emoji-id="\1">', html)
    html = re.sub(r'</emoji>', '</tg-emoji>', html)
    # Normalize expandable blockquote: strip extra whitespace in the opening tag
    html = re.sub(r'<blockquote\s+expandable\s*>', '<blockquote expandable>', html)
    return html


class TelegramFloodWait(Exception):
    """Bot API returned 429 — caller should sleep and retry."""

    def __init__(self, retry_after: int):
        self.retry_after = retry_after
        super().__init__(f"FloodWait {retry_after}s")


class ReplyNotReady(Exception):
    """Parent message for reply threading has not been forwarded yet."""


class TelegramBotSender:
    """Send messages to destinations using Telegram Bot API."""

    API_BASE = "https://api.telegram.org/bot{token}/{method}"

    def __init__(self, bot_token: str, connect_timeout: float = 5.0, read_timeout: float = 60.0):
        self.bot_token = bot_token
        self.timeout = aiohttp.ClientTimeout(
            connect=connect_timeout,
            total=read_timeout + connect_timeout,
        )
        self._session: Optional[aiohttp.ClientSession] = None
        self._update_offset: int = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _call(self, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Call a Bot API method and return the result object."""
        url = self.API_BASE.format(token=self.bot_token, method=method)
        session = await self._get_session()

        async with session.post(url, json=payload) as resp:
            body = await resp.json(content_type=None)

            if resp.status == 429:
                retry_after = 1
                if isinstance(body, dict):
                    params = body.get("parameters") or {}
                    retry_after = int(params.get("retry_after", 1))
                raise TelegramFloodWait(retry_after)

            if not isinstance(body, dict) or not body.get("ok"):
                description = body.get("description", str(body)) if isinstance(body, dict) else str(body)
                raise RuntimeError(f"Bot API {method} failed: {description}")

            return body["result"]

    @staticmethod
    def _reply_params(
        reply_to_message_id: Optional[Union[int, str]],
        quote: Optional[str] = None,
        quote_parse_mode: Optional[str] = None,
        quote_position: Optional[int] = None,
        allow_sending_without_reply: bool = True,
    ) -> Optional[Dict[str, Any]]:
        if reply_to_message_id is None:
            return None
        params: Dict[str, Any] = {
            "message_id": int(reply_to_message_id),
            # If the replied-to message has been deleted or is not found,
            # still deliver the message rather than failing entirely.
            "allow_sending_without_reply": allow_sending_without_reply,
        }
        if quote:
            params["quote"] = quote
            # Only set quote_parse_mode when it has an explicit value.
            # None means plain-text matching (no HTML parsing) which is the
            # safe default — the Bot API does a raw substring check.
            if quote_parse_mode:
                params["quote_parse_mode"] = quote_parse_mode
            if quote_position is not None:
                params["quote_position"] = int(quote_position)
        return params

    @staticmethod
    def _is_quote_error(exc: Exception) -> bool:
        """Return True when the error is caused by a bad quote string.

        Telegram returns these error descriptions when the quote text does not
        match the replied-to message (e.g. because entity re-encoding produced
        a slightly different string in the destination):
          - QUOTE_TEXT_INVALID
          - Bad Request: message quote text not found
          - Bad Request: quote text is not found in the message
        """
        msg = str(exc).lower()
        return (
            "quote_text_invalid" in msg
            or "quote text not found" in msg
            or "quote text is not found" in msg
            or "invalid quote" in msg
        )

    # _bot_api_entities removed: formatting is now transported as an HTML
    # string (text_html / caption_html) and decoded by Telegram via
    # parse_mode="HTML".  No manual entity-object serialization is needed.

    async def get_me(self) -> Dict[str, Any]:
        return await self._call("getMe", {})

    async def get_chat(self, chat_id: Union[int, str]) -> Dict[str, Any]:
        return await self._call("getChat", {"chat_id": chat_id})

    async def get_chat_member(self, chat_id: Union[int, str], user_id: int) -> Dict[str, Any]:
        """Return ChatMember object for user_id in chat_id."""
        return await self._call(
            "getChatMember",
            {"chat_id": chat_id, "user_id": int(user_id)},
        )

    async def forward_message(
        self,
        chat_id: Union[int, str],
        from_chat_id: Union[int, str],
        message_id: Union[int, str],
        reply_to_message_id: Optional[Union[int, str]] = None,
        quote: Optional[str] = None,
        quote_parse_mode: Optional[str] = None,
        quote_position: Optional[int] = None,
    ) -> int:
        """
        Forward a message preserving the 'Forwarded from' attribution.
        Returns the new message_id in the destination chat.
        """
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "from_chat_id": from_chat_id,
            "message_id": int(message_id),
        }
        reply_params = self._reply_params(
            reply_to_message_id, quote=quote, quote_parse_mode=quote_parse_mode, quote_position=quote_position
        )
        if reply_params:
            payload["reply_parameters"] = reply_params

        result = await self._call("forwardMessage", payload)
        return int(result["message_id"])

    async def forward_messages(
        self,
        chat_id: Union[int, str],
        from_chat_id: Union[int, str],
        message_ids: List[Union[int, str]],
        reply_to_message_id: Optional[Union[int, str]] = None,
        quote: Optional[str] = None,
        quote_parse_mode: Optional[str] = None,
        quote_position: Optional[int] = None,
    ) -> List[int]:
        """
        Forward multiple messages (e.g. album) preserving attribution.
        Returns destination message_ids in order.
        """
        # Validate before sending — an empty list causes MESSAGE_IDS_EMPTY
        valid_ids = [int(mid) for mid in message_ids if mid and int(mid) > 0]
        if not valid_ids:
            raise RuntimeError(
                "forward_messages called with no valid message_ids "
                "(all were None or zero)"
            )

        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "from_chat_id": from_chat_id,
            "message_ids": valid_ids,
        }
        reply_params = self._reply_params(
            reply_to_message_id, quote=quote, quote_parse_mode=quote_parse_mode, quote_position=quote_position
        )
        if reply_params:
            payload["reply_parameters"] = reply_params

        result = await self._call("forwardMessages", payload)
        return [int(item["message_id"]) for item in result]

    async def _consume_updates(self) -> List[Dict[str, Any]]:
        """Fetch pending Bot API updates (sender bot must not use a webhook)."""
        params: Dict[str, Any] = {
            "timeout": 0,
            "allowed_updates": ["message"],
            "limit": 100,
        }
        if self._update_offset:
            params["offset"] = self._update_offset

        updates = await self._call("getUpdates", params)
        if updates:
            self._update_offset = updates[-1]["update_id"] + 1
        return updates

    async def resolve_relay_message_ids(
        self,
        user_id: int,
        count: int = 1,
        wait_seconds: float = 1.0,
    ) -> List[int]:
        """
        After userbot posts media to the bot DM, resolve message id(s) the Bot API can copy.

        Hydrogram message ids in a user→bot DM do not always match Bot API ids.
        """
        await asyncio.sleep(wait_seconds)

        for attempt in range(6):
            updates = await self._consume_updates()
            matched: List[int] = []
            for upd in updates:
                msg = upd.get("message")
                if not msg:
                    continue
                chat = msg.get("chat") or {}
                from_user = msg.get("from") or {}
                if chat.get("id") == user_id and from_user.get("id") == user_id:
                    matched.append(int(msg["message_id"]))

            matched.sort()
            if len(matched) >= count:
                return matched[-count:]

            await asyncio.sleep(0.5 * (attempt + 1))

        raise RuntimeError(
            f"Bot API did not receive relay message from user {user_id} "
            f"(needed {count} id(s))"
        )

    async def pin_chat_message(
        self,
        chat_id: Union[int, str],
        message_id: Union[int, str],
        disable_notification: bool = False,
    ) -> bool:
        """Pin a message in a chat using Telegram Bot API."""
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": int(message_id),
            "disable_notification": disable_notification,
        }
        return await self._call("pinChatMessage", payload)

    async def delete_message(
        self,
        chat_id: Union[int, str],
        message_id: Union[int, str],
    ) -> bool:
        """
        Delete a message from a chat via Bot API deleteMessage.

        Returns True on success.  Silently treats "message not found" /
        "message can't be deleted" as success since the message may already
        have been removed by another means or the bot may have re-started.

        Requires the bot to be an administrator with can_delete_messages = True
        in the target chat/channel.
        """
        try:
            result = await self._call(
                "deleteMessage",
                {"chat_id": chat_id, "message_id": int(message_id)},
            )
            return bool(result)
        except RuntimeError as e:
            err_msg = str(e).lower()
            if (
                "message to delete not found" in err_msg
                or "message_id_invalid" in err_msg
                or "message can't be deleted" in err_msg
                or "message is not modified" in err_msg
            ):
                # Already deleted or inaccessible — treat as success to avoid
                # unnecessary retries.
                logger.debug(
                    f"delete_message: {chat_id}/{message_id} already gone or "
                    f"inaccessible: {e}"
                )
                return True
            raise

    async def delete_messages_batch(
        self,
        chat_id: Union[int, str],
        message_ids: List[Union[int, str]],
    ) -> bool:
        """
        Delete up to 100 messages at once via Bot API deleteMessages (Bot API 7.0+).

        Falls back to sequential deleteMessage calls when deleteMessages is
        unavailable (older Bot API endpoints or older Telegram server versions).
        Returns True when all deletes succeeded (or were already gone).
        """
        if not message_ids:
            return True

        valid_ids = [int(mid) for mid in message_ids if mid and int(mid) > 0]
        if not valid_ids:
            return True

        # deleteMessages accepts up to 100 IDs per call (Bot API 7.0+).
        try:
            await self._call(
                "deleteMessages",
                {"chat_id": chat_id, "message_ids": valid_ids[:100]},
            )
            return True
        except RuntimeError as e:
            err_msg = str(e).lower()
            if "message to delete not found" in err_msg or "message_id_invalid" in err_msg:
                return True  # All already deleted
            if "method not found" in err_msg or "not supported" in err_msg:
                logger.debug(
                    "deleteMessages not available — falling back to sequential deleteMessage"
                )
            else:
                raise

        # Sequential fallback for servers that don't support deleteMessages
        all_ok = True
        for mid in valid_ids:
            try:
                await self.delete_message(chat_id, mid)
            except Exception as e:
                logger.warning(
                    f"delete_messages_batch: failed to delete {mid} from {chat_id}: {e}"
                )
                all_ok = False
        return all_ok

    async def copy_message(
        self,
        chat_id: Union[int, str],
        from_chat_id: Union[int, str],
        message_id: Union[int, str],
        caption: Optional[str] = None,
        parse_mode: Optional[str] = "HTML",
        reply_to_message_id: Optional[Union[int, str]] = None,
        reply_markup: Optional[Dict[str, Any]] = None,
        quote: Optional[str] = None,
        quote_parse_mode: Optional[str] = None,
        quote_position: Optional[int] = None,
    ) -> int:
        """Copy a single message. Returns the new message_id in the destination chat."""
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "from_chat_id": from_chat_id,
            "message_id": int(message_id),
        }
        if caption is not None:
            payload["caption"] = caption
            if parse_mode:
                payload["parse_mode"] = parse_mode
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        reply_params = self._reply_params(
            reply_to_message_id, quote=quote, quote_parse_mode=quote_parse_mode, quote_position=quote_position
        )
        if reply_params:
            payload["reply_parameters"] = reply_params

        last_error = None
        for attempt in range(6):
            try:
                result = await self._call("copyMessage", payload)
                return int(result["message_id"])
            except RuntimeError as e:
                last_error = e
                err_msg = str(e).lower()
                if (
                    "message to copy not found" in err_msg
                    or "media_empty" in err_msg
                    or "photo_invalid" in err_msg
                ) and attempt < 5:
                    wait = 0.5 * (2**attempt)
                    logger.warning(
                        f"copyMessage delay/not found (from={from_chat_id}, id={message_id}), "
                        f"retry in {wait:.1f}s (attempt {attempt + 1}/6)"
                    )
                    await asyncio.sleep(wait)
                    continue

                if (
                    "cannot_use_custom_emoji" in err_msg
                    or "cannot use custom emoji" in err_msg
                    or "failed to parse entities" in err_msg
                    or "entity_bounds_invalid" in err_msg
                ) and payload.get("caption"):
                    logger.warning(
                        f"copyMessage custom emoji/entity error ({e}) — "
                        "retrying with sanitized caption and parse_mode=None"
                    )
                    sanitized = re.sub(r'<tg-emoji[^>]*>(.*?)</tg-emoji>', r'\1', payload["caption"], flags=re.DOTALL)
                    sanitized = re.sub(r'<emoji[^>]*>(.*?)</emoji>', r'\1', sanitized, flags=re.DOTALL)
                    payload["caption"] = re.sub(r'<[^>]+>', '', sanitized)
                    payload.pop("parse_mode", None)
                    try:
                        result = await self._call("copyMessage", payload)
                        return int(result["message_id"])
                    except Exception:
                        pass
                raise

        if last_error is not None:
            raise last_error  # pragma: no cover

    async def copy_messages(
        self,
        chat_id: Union[int, str],
        from_chat_id: Union[int, str],
        message_ids: List[Union[int, str]],
        reply_to_message_id: Optional[Union[int, str]] = None,
        quote: Optional[str] = None,
        quote_parse_mode: Optional[str] = None,
        quote_position: Optional[int] = None,
    ) -> List[int]:
        """Copy an album (media group). Returns message_ids in destination chat, in order."""
        # Validate before sending — an empty list causes MESSAGE_IDS_EMPTY
        valid_ids = [int(mid) for mid in message_ids if mid and int(mid) > 0]
        if not valid_ids:
            raise RuntimeError(
                "copy_messages called with no valid message_ids "
                "(all were None or zero)"
            )

        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "from_chat_id": from_chat_id,
            "message_ids": valid_ids,
        }
        reply_params = self._reply_params(
            reply_to_message_id, quote=quote, quote_parse_mode=quote_parse_mode, quote_position=quote_position
        )
        if reply_params:
            payload["reply_parameters"] = reply_params

        result = await self._call("copyMessages", payload)
        return [int(item["message_id"]) for item in result]

    async def send_message(
        self,
        chat_id: Union[int, str],
        text: str,
        parse_mode: Optional[str] = "HTML",
        reply_to_message_id: Optional[Union[int, str]] = None,
        reply_markup: Optional[Dict[str, Any]] = None,
        quote: Optional[str] = None,
        quote_parse_mode: Optional[str] = None,
        quote_position: Optional[int] = None,
    ) -> int:
        """Send a message with HTML formatting.  text should already be an HTML string."""
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        reply_params = self._reply_params(
            reply_to_message_id, quote=quote, quote_parse_mode=quote_parse_mode, quote_position=quote_position
        )
        if reply_params:
            payload["reply_parameters"] = reply_params

        result = await self._call("sendMessage", payload)
        return int(result["message_id"])

    @staticmethod
    def _unix_timestamp(value: Any) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, datetime):
            return int(value.timestamp())
        if isinstance(value, str):
            return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
        return None

    async def send_poll(
        self,
        chat_id: Union[int, str],
        poll: Dict[str, Any],
        reply_to_message_id: Optional[Union[int, str]] = None,
        reply_markup: Optional[Dict[str, Any]] = None,
        quote: Optional[str] = None,
        quote_parse_mode: Optional[str] = None,
        quote_position: Optional[int] = None,
    ) -> int:
        """Recreate a poll via sendPoll."""
        poll_type = poll.get("type", "regular")
        correct_option_id = poll.get("correct_option_id")

        if poll_type == "quiz" and correct_option_id is None:
            logger.warning(
                "Quiz poll missing correct_option_id — sending as regular poll"
            )
            poll_type = "regular"

        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "question": poll["question"],
            # Bot API 10.0+ requires options as InputPollOption objects.
            # Older payloads may contain plain strings — coerce them here.
            "options": [
                opt if isinstance(opt, dict) else {"text": str(opt)}
                for opt in poll.get("options", [])
            ],
            "is_anonymous": poll.get("is_anonymous", True),
            "type": poll_type,
        }
        if poll.get("allows_multiple_answers"):
            payload["allows_multiple_answers"] = True
        if poll_type == "quiz" and correct_option_id is not None:
            payload["correct_option_id"] = int(correct_option_id)
        if poll.get("explanation"):
            payload["explanation"] = poll["explanation"]
        if poll.get("open_period"):
            payload["open_period"] = int(poll["open_period"])
        close_date = self._unix_timestamp(poll.get("close_date"))
        if close_date is not None:
            payload["close_date"] = close_date

        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        reply_params = self._reply_params(
            reply_to_message_id, quote=quote, quote_parse_mode=quote_parse_mode, quote_position=quote_position
        )
        if reply_params:
            payload["reply_parameters"] = reply_params

        result = await self._call("sendPoll", payload)
        return int(result["message_id"])

    async def send_location(
        self,
        chat_id: Union[int, str],
        location: Dict[str, Any],
        reply_to_message_id: Optional[Union[int, str]] = None,
        reply_markup: Optional[Dict[str, Any]] = None,
        quote: Optional[str] = None,
        quote_parse_mode: Optional[str] = None,
        quote_position: Optional[int] = None,
    ) -> int:
        """Recreate a location pin via sendLocation."""
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "latitude": location["latitude"],
            "longitude": location["longitude"],
        }
        if location.get("horizontal_accuracy") is not None:
            payload["horizontal_accuracy"] = location["horizontal_accuracy"]
        # Live location fields (Bot API sendLocation)
        if location.get("live_period"):
            payload["live_period"] = int(location["live_period"])
        if location.get("heading") is not None:
            payload["heading"] = int(location["heading"])
        if location.get("proximity_alert_radius") is not None:
            payload["proximity_alert_radius"] = int(location["proximity_alert_radius"])

        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        reply_params = self._reply_params(
            reply_to_message_id, quote=quote, quote_parse_mode=quote_parse_mode, quote_position=quote_position
        )
        if reply_params:
            payload["reply_parameters"] = reply_params

        result = await self._call("sendLocation", payload)
        return int(result["message_id"])

    async def send_venue(
        self,
        chat_id: Union[int, str],
        venue: Dict[str, Any],
        reply_to_message_id: Optional[Union[int, str]] = None,
        reply_markup: Optional[Dict[str, Any]] = None,
        quote: Optional[str] = None,
        quote_parse_mode: Optional[str] = None,
        quote_position: Optional[int] = None,
    ) -> int:
        """Recreate a venue via sendVenue."""
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "latitude": venue["latitude"],
            "longitude": venue["longitude"],
            "title": venue["title"],
            "address": venue["address"],
        }
        if venue.get("foursquare_id"):
            payload["foursquare_id"] = venue["foursquare_id"]
        if venue.get("foursquare_type"):
            payload["foursquare_type"] = venue["foursquare_type"]
        if venue.get("google_place_id"):
            payload["google_place_id"] = venue["google_place_id"]

        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        reply_params = self._reply_params(
            reply_to_message_id, quote=quote, quote_parse_mode=quote_parse_mode, quote_position=quote_position
        )
        if reply_params:
            payload["reply_parameters"] = reply_params

        result = await self._call("sendVenue", payload)
        return int(result["message_id"])

    async def send_contact(
        self,
        chat_id: Union[int, str],
        contact: Dict[str, Any],
        reply_to_message_id: Optional[Union[int, str]] = None,
        reply_markup: Optional[Dict[str, Any]] = None,
        quote: Optional[str] = None,
        quote_parse_mode: Optional[str] = None,
        quote_position: Optional[int] = None,
    ) -> int:
        """Recreate a shared contact via sendContact."""
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "phone_number": contact["phone_number"],
            "first_name": contact["first_name"],
        }
        if contact.get("last_name"):
            payload["last_name"] = contact["last_name"]
        if contact.get("vcard"):
            payload["vcard"] = contact["vcard"]

        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        reply_params = self._reply_params(
            reply_to_message_id, quote=quote, quote_parse_mode=quote_parse_mode, quote_position=quote_position
        )
        if reply_params:
            payload["reply_parameters"] = reply_params

        result = await self._call("sendContact", payload)
        return int(result["message_id"])

    async def send_dice(
        self,
        chat_id: Union[int, str],
        emoji: str = "\U0001f3b2",
        reply_to_message_id: Optional[Union[int, str]] = None,
        reply_markup: Optional[Dict[str, Any]] = None,
        quote: Optional[str] = None,
        quote_parse_mode: Optional[str] = None,
        quote_position: Optional[int] = None,
    ) -> int:
        """Send an animated dice via sendDice."""
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "emoji": emoji,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        reply_params = self._reply_params(
            reply_to_message_id, quote=quote, quote_parse_mode=quote_parse_mode, quote_position=quote_position
        )
        if reply_params:
            payload["reply_parameters"] = reply_params
        result = await self._call("sendDice", payload)
        return int(result["message_id"])

    async def edit_message_caption(
        self,
        chat_id: Union[int, str],
        message_id: Union[int, str],
        caption: str,
        parse_mode: Optional[str] = "HTML",
    ) -> None:
        params: Dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": int(message_id),
            "caption": caption,
        }
        if parse_mode:
            params["parse_mode"] = parse_mode
        await self._call("editMessageCaption", params)

    async def verify_bot_access(self, chat_id: Union[int, str]) -> bool:
        """Return True if the bot can access the given chat."""
        try:
            await self.get_chat(chat_id)
            return True
        except Exception as e:
            logger.warning(f"Bot cannot access chat {chat_id}: {e}")
            return False

    async def verify_destinations(self, destinations: List[Dict[str, Any]]) -> List[str]:
        """Return list of error messages for destinations the bot cannot access."""
        errors = []
        for dest in destinations:
            if not dest.get("enabled", True):
                continue
            chat_id = dest["chat_id"]
            if not await self.verify_bot_access(chat_id):
                name = dest.get("name", chat_id)
                errors.append(
                    f"Bot cannot access destination '{name}' ({chat_id}). "
                    "Add the sender bot as admin with post permission."
                )
        return errors

    async def forward_to_destination(
        self,
        dest_chat_id: Union[int, str],
        msg_type: str,
        payload: Dict[str, Any],
        processed_payload: Dict[str, Any],
        relay_chat_id: Optional[Union[int, str]] = None,
        relay_message_ids: Optional[List[int]] = None,
        reply_to_message_id: Optional[Union[int, str]] = None,
    ) -> Dict[str, Any]:
        """
        Forward one message/album to a single destination.

        Text uses sendMessage from payload content (no source-channel access).
        Media uses copyMessage from the userbot relay chat.
        When use_native_forward is set, uses forwardMessage from the origin channel
        so destinations show the "Forwarded from" tag.

        Returns:
            {
                "sent_message_id": int | None,
                "reply_mappings": [(source_id, sent_id), ...],
            }
        """
        # Prefer the HTML-encoded quote so Telegram can match it precisely
        # against the rich-text destination copy.  Fall back to plain text
        # (quote_parse_mode omitted → raw substring match) when no HTML quote
        # is available.  Both paths go through _attempt_with_quote_fallback so
        # a mismatch still delivers the message as a normal reply.
        quote_html = (
            processed_payload.get("processed_reply_to_quote_html")
            or payload.get("reply_to_quote_html")
        )
        quote_plain = (
            processed_payload.get("processed_reply_to_quote")
            or payload.get("reply_to_quote_text")
        )
        if quote_html:
            quote = quote_html
            _quote_parse_mode: Optional[str] = "HTML"
        elif quote_plain:
            quote = quote_plain
            _quote_parse_mode = None  # plain text — raw substring match, no parsing
        else:
            quote = None
            _quote_parse_mode = None
        quote_position = payload.get("reply_to_quote_position")
        reply_kwargs: Dict[str, Any] = {"reply_to_message_id": reply_to_message_id}
        if quote:
            reply_kwargs["quote"] = quote
            reply_kwargs["quote_parse_mode"] = _quote_parse_mode
        if quote_position is not None:
            reply_kwargs["quote_position"] = quote_position

        if payload.get("use_native_forward"):
            return await self._native_forward_to_destination(
                dest_chat_id=dest_chat_id,
                msg_type=msg_type,
                payload=payload,
                **reply_kwargs,
            )

        if msg_type == "album":
            items = processed_payload.get("items") or payload.get("items") or []
            sorted_items = sorted(items, key=lambda x: int(x.get("message_id", 0)))
            source_message_ids = [int(item["message_id"]) for item in sorted_items]

            if not relay_chat_id or not relay_message_ids:
                raise RuntimeError("Media album requires userbot relay before Bot API delivery")

            sent_ids = await self.copy_messages(
                chat_id=dest_chat_id,
                from_chat_id=relay_chat_id,
                message_ids=relay_message_ids,
                **reply_kwargs,
            )

            for item, sent_id in zip(sorted_items, sent_ids):
                # Only edit the caption when a replacement rule actually changed it.
                #
                # Use the authoritative per-item flag set by replacements.py first.
                # If it's absent (e.g. old payloads / test data without the flag),
                # fall back to a like-for-like text comparison:
                #   - HTML caption present → compare processed_caption_html vs caption_html
                #   - plain caption only   → compare processed_caption vs caption
                # This avoids the old bug where processed_caption (HTML-sourced) was
                # compared against item["caption"] (plain text), causing a false
                # positive on every album item with formatted captions.
                caption_changed = bool(item.get("caption_changed"))
                processed_html = item.get("processed_caption_html")
                processed_plain = item.get("processed_caption")

                if not caption_changed:
                    if processed_html is not None:
                        # Both sides are HTML — compare HTML to HTML
                        caption_changed = processed_html != (item.get("caption_html") or "")
                    elif processed_plain is not None:
                        # Plain text only — compare plain to plain
                        caption_changed = processed_plain != (item.get("caption") or "")

                if caption_changed:
                    try:
                        if processed_html:
                            # Replacement fired on an HTML-formatted caption — preserve
                            # rich formatting by sending the HTML version with parse_mode.
                            await self.edit_message_caption(
                                dest_chat_id, sent_id,
                                _normalize_html_for_bot_api(processed_html),
                                parse_mode="HTML",
                            )
                        elif processed_plain:
                            # Plain-text caption — send without parse_mode.
                            await self.edit_message_caption(
                                dest_chat_id, sent_id, processed_plain,
                                parse_mode=None,
                            )
                    except Exception as cap_err:
                        logger.warning(
                            f"Failed to edit caption for album item {sent_id} in {dest_chat_id}: {cap_err}"
                        )


            reply_mappings = [
                (int(item["message_id"]), sent_id)
                for item, sent_id in zip(sorted_items, sent_ids)
            ]
            return {
                "sent_message_id": sent_ids[0] if sent_ids else None,
                "reply_mappings": reply_mappings,
            }

        msg_id = int(payload["message_id"])
        reply_markup = processed_payload.get("reply_markup") or payload.get("reply_markup")
        extra_kwargs = {"reply_markup": reply_markup} if reply_markup is not None else {}

        if msg_type == "text":
            text_changed = processed_payload.get("text_changed", False)

            # When replacements altered the text the destination copy differs
            # from the source — the quote substring won't match the destination
            # message, so Telegram will reject it.  Strip the quote in that case.
            safe_reply_kwargs = dict(reply_kwargs)
            if text_changed:
                safe_reply_kwargs.pop("quote", None)
                safe_reply_kwargs.pop("quote_parse_mode", None)
                safe_reply_kwargs.pop("quote_position", None)

            if not text_changed and relay_message_ids and relay_chat_id:
                # Message has rich entities (blockquotes, spoilers, dates, etc.)
                # and was already relayed via Hydrogram copy_message (MTProto).
                # Use Bot API copyMessage so ALL entities survive intact —
                # the HTML parser in Hydrogram cannot encode these newer types.
                sent_id = await self._attempt_with_quote_fallback(
                    self.copy_message,
                    dict(
                        chat_id=dest_chat_id,
                        from_chat_id=relay_chat_id,
                        message_id=relay_message_ids[0],
                        caption=None,  # text messages have no caption field
                        parse_mode=None,  # entities are copied natively, not via parse_mode
                        **safe_reply_kwargs,
                        **extra_kwargs,
                    ),
                )
            elif text_changed:
                # Replacement altered the text — send processed HTML text if available.
                raw_text = (
                    processed_payload.get("processed_text_html")
                    or processed_payload.get("processed_text")
                    or payload.get("text_html")
                    or payload.get("text")
                    or ""
                )
                use_html = bool(
                    processed_payload.get("processed_text_html")
                    or payload.get("text_html")
                )
                text = _normalize_html_for_bot_api(raw_text) if use_html else raw_text
                sent_id = await self._attempt_with_quote_fallback(
                    self.send_message,
                    dict(
                        chat_id=dest_chat_id,
                        text=text,
                        parse_mode="HTML" if use_html else None,
                        **safe_reply_kwargs,
                        **extra_kwargs,
                    ),
                )
            else:
                # No entities, no replacement — simple plain-text send.
                # Falls back to text_html (bold/italic/links) if available.
                has_html = bool(payload.get("text_html"))
                raw_text = payload.get("text_html") or payload.get("text") or ""
                text = _normalize_html_for_bot_api(raw_text) if has_html else raw_text
                sent_id = await self._attempt_with_quote_fallback(
                    self.send_message,
                    dict(
                        chat_id=dest_chat_id,
                        text=text,
                        parse_mode="HTML" if has_html else None,
                        **safe_reply_kwargs,
                        **extra_kwargs,
                    ),
                )
        elif msg_type == "poll":
            sent_id = await self.send_poll(
                chat_id=dest_chat_id,
                poll=payload["poll"],
                **reply_kwargs,
                **extra_kwargs,
            )
        elif msg_type == "location":
            sent_id = await self.send_location(
                chat_id=dest_chat_id,
                location=payload["location"],
                **reply_kwargs,
                **extra_kwargs,
            )
        elif msg_type == "venue":
            sent_id = await self.send_venue(
                chat_id=dest_chat_id,
                venue=payload["venue"],
                **reply_kwargs,
                **extra_kwargs,
            )
        elif msg_type == "contact":
            sent_id = await self.send_contact(
                chat_id=dest_chat_id,
                contact=payload["contact"],
                **reply_kwargs,
                **extra_kwargs,
            )
        elif msg_type == "dice":
            dice = payload.get("dice") or {}
            sent_id = await self.send_dice(
                chat_id=dest_chat_id,
                emoji=dice.get("emoji", "\U0001f3b2"),
                **reply_kwargs,
                **extra_kwargs,
            )
        else:
            if not relay_chat_id or not relay_message_ids:
                raise RuntimeError("Media message requires userbot relay before Bot API delivery")

            caption = None
            parse_mode = None
            caption_changed = bool(processed_payload.get("caption_changed"))
            if caption_changed:
                # Replacement changed the caption — send processed HTML caption if available
                raw_caption = (
                    processed_payload.get("processed_caption_html")
                    or processed_payload.get("processed_caption")
                    or payload.get("caption_html")
                    or payload.get("caption")
                )
                use_caption_html = bool(
                    processed_payload.get("processed_caption_html") or payload.get("caption_html")
                )
                caption = _normalize_html_for_bot_api(raw_caption) if use_caption_html else raw_caption
                parse_mode = "HTML" if use_caption_html else None
            elif not payload.get("caption") and not payload.get("caption_html"):
                # Source message had no caption at all (e.g. a captionless GIF).
                # Force caption="" so the Bot API copyMessage call explicitly
                # clears any spurious "None" string that may have appeared in
                # the relay message due to Hydrogram stringifying a missing caption.
                caption = ""
                parse_mode = None

            # When caption_changed is True the destination caption differs from
            # the source — drop the quote to avoid QUOTE_TEXT_INVALID errors.
            safe_reply_kwargs = dict(reply_kwargs)
            if caption_changed:
                safe_reply_kwargs.pop("quote", None)
                safe_reply_kwargs.pop("quote_parse_mode", None)
                safe_reply_kwargs.pop("quote_position", None)

            # When caption_changed is False, caption=None & parse_mode=None are passed to copy_message.
            # This allows Telegram Bot API copyMessage to preserve the original relay message's
            # caption AND all native entities (premium emojis, custom emojis, animated emojis, etc.)
            # directly without running the HTML parser or hitting CANNOT_USE_CUSTOM_EMOJI errors.
            sent_id = await self._attempt_with_quote_fallback(
                self.copy_message,
                dict(
                    chat_id=dest_chat_id,
                    from_chat_id=relay_chat_id,
                    message_id=relay_message_ids[0],
                    caption=caption,
                    parse_mode=parse_mode,
                    **safe_reply_kwargs,
                    **extra_kwargs,
                ),
            )

        return {
            "sent_message_id": sent_id,
            "reply_mappings": [(msg_id, sent_id)],
        }

    async def _attempt_with_quote_fallback(self, func, kwargs: Dict[str, Any]):
        """Call *func(**kwargs)*, retrying without the quote on quote-mismatch errors.

        Telegram rejects a quoted reply when the quote text is not an exact
        substring of the destination copy of the replied-to message.  This can
        happen even when no replacement rule fired — entity re-encoding (HTML
        escape differences, zero-width joiners, etc.) can produce a slightly
        different string in the destination.

        If the first attempt fails with any quote-related error we transparently
        retry without the quote so the message still arrives as a normal reply.
        """
        try:
            return await func(**kwargs)
        except Exception as e:
            if self._is_quote_error(e) and "quote" in kwargs:
                logger.warning(
                    f"Quote mismatch on reply — retrying without quote: {e}"
                )
                no_quote_kwargs = {
                    k: v for k, v in kwargs.items()
                    if k not in ("quote", "quote_parse_mode", "quote_position")
                }
                return await func(**no_quote_kwargs)
            raise

    async def _native_forward_to_destination(
        self,
        dest_chat_id: Union[int, str],
        msg_type: str,
        payload: Dict[str, Any],
        reply_to_message_id: Optional[Union[int, str]] = None,
        quote: Optional[str] = None,
        quote_parse_mode: Optional[str] = None,  # None = plain-text raw match
        quote_position: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Deliver via forwardMessage(s) from the original channel."""
        origin = payload.get("native_forward_origin") or {}
        from_chat_id = origin.get("chat_id")
        origin_ids = origin.get("message_ids") or (
            [origin["message_id"]] if origin.get("message_id") is not None else []
        )
        if from_chat_id is None or not origin_ids:
            raise RuntimeError("Native forward missing origin chat_id / message_ids")

        fwd_kwargs: Dict[str, Any] = {"reply_to_message_id": reply_to_message_id}
        if quote:
            fwd_kwargs["quote"] = quote
            # Pass through the parse mode determined by forward_to_destination.
            # None means plain-text raw match; "HTML" means rich-text quote.
            if quote_parse_mode:
                fwd_kwargs["quote_parse_mode"] = quote_parse_mode
        if quote_position is not None:
            fwd_kwargs["quote_position"] = quote_position

        if msg_type == "album":
            items = payload.get("items") or []
            sorted_items = sorted(items, key=lambda x: int(x.get("message_id", 0)))
            # Prefer per-item forward_origin message_ids (same order as sorted items)
            fwd_ids = []
            for item in sorted_items:
                fo = item.get("forward_origin") or {}
                fwd_ids.append(int(fo.get("message_id") or 0))
            if not all(fwd_ids):
                fwd_ids = [int(mid) for mid in origin_ids]

            sent_ids = await self.forward_messages(
                chat_id=dest_chat_id,
                from_chat_id=from_chat_id,
                message_ids=fwd_ids,
                **fwd_kwargs,
            )
            reply_mappings = [
                (int(item["message_id"]), sent_id)
                for item, sent_id in zip(sorted_items, sent_ids)
            ]
            return {
                "sent_message_id": sent_ids[0] if sent_ids else None,
                "reply_mappings": reply_mappings,
            }

        msg_id = int(payload["message_id"])
        sent_id = await self.forward_message(
            chat_id=dest_chat_id,
            from_chat_id=from_chat_id,
            message_id=int(origin_ids[0]),
            **fwd_kwargs,
        )
        return {
            "sent_message_id": sent_id,
            "reply_mappings": [(msg_id, sent_id)],
        }

    async def call_with_flood_wait(
        self,
        coro_factory,
        max_retries: int = 3,
    ):
        """
        Execute a coroutine, sleeping on FloodWait (up to max_retries times).

        Each FloodWait resets the retry counter — we keep going as long as
        Telegram tells us to wait, rather than giving up after a single 429.
        """
        for attempt in range(max_retries):
            try:
                return await coro_factory()
            except TelegramFloodWait as e:
                if attempt >= max_retries - 1:
                    raise  # Exhaust retries — let the worker re-queue
                wait = e.retry_after + 1
                logger.warning(
                    f"Bot API FloodWait({e.retry_after}s), "
                    f"sleeping {wait}s (attempt {attempt + 1}/{max_retries})"
                )
                await asyncio.sleep(wait)
        # Should never reach here, but satisfy type checker
        return await coro_factory()
