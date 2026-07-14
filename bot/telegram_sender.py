"""
Telegram Bot API sender — delivers to destination channels.

Text is sent with sendMessage (content from the userbot payload).
Media is copied from a userbot relay chat via copyMessage / copyMessages
so the sender bot never needs access to the source channel.
"""

import asyncio
import logging
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

    @staticmethod
    def _bot_api_entities(entities: Optional[List[Dict[str, Any]]]) -> Optional[List[Dict[str, Any]]]:
        """Convert serialized Pyrogram entities to Bot API MessageEntity objects."""
        if not entities:
            return None

        result = []
        for entity in entities:
            entry: Dict[str, Any] = {
                "type": entity["type"],
                "offset": entity["offset"],
                "length": entity["length"],
            }
            if entity.get("url"):
                entry["url"] = entity["url"]
            if entity.get("user_id"):
                entry["user"] = {"id": entity["user_id"]}
            if entity.get("language"):
                entry["language"] = entity["language"]
            if entity.get("custom_emoji_id"):
                entry["custom_emoji_id"] = entity["custom_emoji_id"]
            result.append(entry)
        return result

    async def get_me(self) -> Dict[str, Any]:
        return await self._call("getMe", {})

    async def get_chat(self, chat_id: Union[int, str]) -> Dict[str, Any]:
        return await self._call("getChat", {"chat_id": chat_id})

    async def copy_message(
        self,
        chat_id: Union[int, str],
        from_chat_id: Union[int, str],
        message_id: Union[int, str],
        caption: Optional[str] = None,
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
        reply_params = self._reply_params(reply_to_message_id)
        if reply_params:
            payload["reply_parameters"] = reply_params

        result = await self._call("copyMessage", payload)
        return int(result["message_id"])

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
        entities: Optional[List[Dict[str, Any]]] = None,
        reply_to_message_id: Optional[Union[int, str]] = None,
    ) -> int:
        """Send plain text with optional formatting entities."""
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
        }
        bot_entities = self._bot_api_entities(entities)
        if bot_entities:
            payload["entities"] = bot_entities
        reply_params = self._reply_params(reply_to_message_id)
        if reply_params:
            payload["reply_parameters"] = reply_params

        result = await self._call("sendMessage", payload)
        return int(result["message_id"])

    async def edit_message_caption(
        self,
        chat_id: Union[int, str],
        message_id: Union[int, str],
        caption: str,
    ) -> None:
        await self._call(
            "editMessageCaption",
            {
                "chat_id": chat_id,
                "message_id": int(message_id),
                "caption": caption,
            },
        )

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

        Returns:
            {
                "sent_message_id": int | None,
                "reply_mappings": [(source_id, sent_id), ...],
            }
        """
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
                    await self.edit_message_caption(dest_chat_id, sent_id, processed)

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
            text = (
                processed_payload.get("processed_text")
                if processed_payload.get("text_changed")
                else payload.get("text")
            ) or ""
            entities = None
            if not processed_payload.get("text_changed"):
                entities = payload.get("entities") or []

            sent_id = await self.send_message(
                chat_id=dest_chat_id,
                text=text,
                entities=entities,
                reply_to_message_id=reply_to_message_id,
            )
        else:
            if not relay_chat_id or not relay_message_ids:
                raise RuntimeError("Media message requires userbot relay before Bot API delivery")

            caption = None
            if processed_payload.get("caption_changed"):
                caption = processed_payload.get("processed_caption") or payload.get("caption")
            sent_id = await self.copy_message(
                chat_id=dest_chat_id,
                from_chat_id=relay_chat_id,
                message_id=relay_message_ids[0],
                caption=caption,
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
