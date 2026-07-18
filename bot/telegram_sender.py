"""
Telegram Bot API sender — delivers to destination channels.

Text is sent with sendMessage (content from the userbot payload).
Media is copied from a userbot relay chat via copyMessage / copyMessages
so the sender bot never needs access to the source channel.
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import aiohttp

logger = logging.getLogger(__name__)


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
    def _reply_params(reply_to_message_id: Optional[Union[int, str]]) -> Optional[Dict[str, Any]]:
        if reply_to_message_id is None:
            return None
        return {"message_id": int(reply_to_message_id)}

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
        reply_params = self._reply_params(reply_to_message_id)
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
    ) -> List[int]:
        """
        Forward multiple messages (e.g. album) preserving attribution.
        Returns destination message_ids in order.
        """
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "from_chat_id": from_chat_id,
            "message_ids": [int(mid) for mid in message_ids],
        }
        reply_params = self._reply_params(reply_to_message_id)
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

        Pyrogram message ids in a user→bot DM do not always match Bot API ids.
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

    async def copy_message(
        self,
        chat_id: Union[int, str],
        from_chat_id: Union[int, str],
        message_id: Union[int, str],
        caption: Optional[str] = None,
        parse_mode: Optional[str] = "HTML",
        reply_to_message_id: Optional[Union[int, str]] = None,
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
        reply_params = self._reply_params(reply_to_message_id)
        if reply_params:
            payload["reply_parameters"] = reply_params

        last_error = None
        for attempt in range(4):
            try:
                result = await self._call("copyMessage", payload)
                return int(result["message_id"])
            except RuntimeError as e:
                last_error = e
                if "message to copy not found" in str(e).lower() and attempt < 3:
                    wait = 1.0 * (attempt + 1)
                    logger.warning(
                        f"copyMessage not found (from={from_chat_id}, id={message_id}), "
                        f"retry in {wait}s"
                    )
                    await asyncio.sleep(wait)
                    continue
                raise

        raise last_error  # pragma: no cover

    async def copy_messages(
        self,
        chat_id: Union[int, str],
        from_chat_id: Union[int, str],
        message_ids: List[Union[int, str]],
        reply_to_message_id: Optional[Union[int, str]] = None,
    ) -> List[int]:
        """Copy an album (media group). Returns message_ids in destination chat, in order."""
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "from_chat_id": from_chat_id,
            "message_ids": [int(mid) for mid in message_ids],
        }
        reply_params = self._reply_params(reply_to_message_id)
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
    ) -> int:
        """Send a message with HTML formatting.  text should already be an HTML string."""
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        reply_params = self._reply_params(reply_to_message_id)
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
            "options": poll["options"],
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

        reply_params = self._reply_params(reply_to_message_id)
        if reply_params:
            payload["reply_parameters"] = reply_params

        result = await self._call("sendPoll", payload)
        return int(result["message_id"])

    async def send_location(
        self,
        chat_id: Union[int, str],
        location: Dict[str, Any],
        reply_to_message_id: Optional[Union[int, str]] = None,
    ) -> int:
        """Recreate a location pin via sendLocation."""
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "latitude": location["latitude"],
            "longitude": location["longitude"],
        }
        if location.get("horizontal_accuracy") is not None:
            payload["horizontal_accuracy"] = location["horizontal_accuracy"]

        reply_params = self._reply_params(reply_to_message_id)
        if reply_params:
            payload["reply_parameters"] = reply_params

        result = await self._call("sendLocation", payload)
        return int(result["message_id"])

    async def send_venue(
        self,
        chat_id: Union[int, str],
        venue: Dict[str, Any],
        reply_to_message_id: Optional[Union[int, str]] = None,
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

        reply_params = self._reply_params(reply_to_message_id)
        if reply_params:
            payload["reply_parameters"] = reply_params

        result = await self._call("sendVenue", payload)
        return int(result["message_id"])

    async def send_contact(
        self,
        chat_id: Union[int, str],
        contact: Dict[str, Any],
        reply_to_message_id: Optional[Union[int, str]] = None,
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

        reply_params = self._reply_params(reply_to_message_id)
        if reply_params:
            payload["reply_parameters"] = reply_params

        result = await self._call("sendContact", payload)
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
        if payload.get("use_native_forward"):
            return await self._native_forward_to_destination(
                dest_chat_id=dest_chat_id,
                msg_type=msg_type,
                payload=payload,
                reply_to_message_id=reply_to_message_id,
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
                reply_to_message_id=reply_to_message_id,
            )

            for item, sent_id in zip(sorted_items, sent_ids):
                original = item.get("caption") or ""
                processed = item.get("processed_caption") or original
                if processed != original:
                    # Replacement fired — send plain text (entities were dropped)
                    await self.edit_message_caption(
                        dest_chat_id, sent_id, processed,
                        parse_mode=None,
                    )
                elif item.get("caption_html") and item.get("caption_html") != original:
                    # No replacement but caption has formatting — push HTML
                    await self.edit_message_caption(
                        dest_chat_id, sent_id, item["caption_html"],
                        parse_mode="HTML",
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

        if msg_type == "text":
            text_changed = processed_payload.get("text_changed", False)

            if not text_changed and relay_message_ids and relay_chat_id:
                # Message has rich entities (blockquotes, spoilers, dates, etc.)
                # and was already relayed via Pyrogram copy_message (MTProto).
                # Use Bot API copyMessage so ALL entities survive intact —
                # the HTML parser in Pyrogram cannot encode these newer types.
                sent_id = await self.copy_message(
                    chat_id=dest_chat_id,
                    from_chat_id=relay_chat_id,
                    message_id=relay_message_ids[0],
                    caption=None,  # text messages have no caption field
                    parse_mode=None,  # entities are copied natively, not via parse_mode
                    reply_to_message_id=reply_to_message_id,
                )
            elif text_changed:
                # Replacement altered the text — send plain processed text.
                # Formatting cannot be preserved after arbitrary text edits.
                text = processed_payload.get("processed_text") or payload.get("text") or ""
                sent_id = await self.send_message(
                    chat_id=dest_chat_id,
                    text=text,
                    parse_mode=None,
                    reply_to_message_id=reply_to_message_id,
                )
            else:
                # No entities, no replacement — simple plain-text send.
                # Falls back to text_html (bold/italic/links) if available.
                text = payload.get("text_html") or payload.get("text") or ""
                sent_id = await self.send_message(
                    chat_id=dest_chat_id,
                    text=text,
                    parse_mode="HTML" if payload.get("text_html") else None,
                    reply_to_message_id=reply_to_message_id,
                )
        elif msg_type == "poll":
            sent_id = await self.send_poll(
                chat_id=dest_chat_id,
                poll=payload["poll"],
                reply_to_message_id=reply_to_message_id,
            )
        elif msg_type == "location":
            sent_id = await self.send_location(
                chat_id=dest_chat_id,
                location=payload["location"],
                reply_to_message_id=reply_to_message_id,
            )
        elif msg_type == "venue":
            sent_id = await self.send_venue(
                chat_id=dest_chat_id,
                venue=payload["venue"],
                reply_to_message_id=reply_to_message_id,
            )
        elif msg_type == "contact":
            sent_id = await self.send_contact(
                chat_id=dest_chat_id,
                contact=payload["contact"],
                reply_to_message_id=reply_to_message_id,
            )
        else:
            if not relay_chat_id or not relay_message_ids:
                raise RuntimeError("Media message requires userbot relay before Bot API delivery")

            caption = None
            parse_mode = None
            if processed_payload.get("caption_changed"):
                # Replacement changed the caption — use plain processed text
                caption = processed_payload.get("processed_caption") or payload.get("caption")
            elif payload.get("caption_html"):
                # No replacement — forward the HTML caption so formatting is kept
                caption = payload.get("caption_html")
                parse_mode = "HTML"
            sent_id = await self.copy_message(
                chat_id=dest_chat_id,
                from_chat_id=relay_chat_id,
                message_id=relay_message_ids[0],
                caption=caption,
                parse_mode=parse_mode,
                reply_to_message_id=reply_to_message_id,
            )

        return {
            "sent_message_id": sent_id,
            "reply_mappings": [(msg_id, sent_id)],
        }

    async def _native_forward_to_destination(
        self,
        dest_chat_id: Union[int, str],
        msg_type: str,
        payload: Dict[str, Any],
        reply_to_message_id: Optional[Union[int, str]] = None,
    ) -> Dict[str, Any]:
        """Deliver via forwardMessage(s) from the original channel."""
        origin = payload.get("native_forward_origin") or {}
        from_chat_id = origin.get("chat_id")
        origin_ids = origin.get("message_ids") or (
            [origin["message_id"]] if origin.get("message_id") is not None else []
        )
        if from_chat_id is None or not origin_ids:
            raise RuntimeError("Native forward missing origin chat_id / message_ids")

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
                reply_to_message_id=reply_to_message_id,
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
            reply_to_message_id=reply_to_message_id,
        )
        return {
            "sent_message_id": sent_id,
            "reply_mappings": [(msg_id, sent_id)],
        }

    async def call_with_flood_wait(self, coro_factory):
        """Execute a coroutine, sleeping once on FloodWait and retrying."""
        try:
            return await coro_factory()
        except TelegramFloodWait as e:
            wait = e.retry_after + 1
            logger.warning(f"Bot API FloodWait({e.retry_after}s), sleeping {wait}s")
            await asyncio.sleep(wait)
            return await coro_factory()
