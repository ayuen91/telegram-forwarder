"""
Userbot media relay — copy messages from source to a bot-accessible chat.

The sender bot only needs access to destination channels. Media is copied
server-side by the Pyrogram user account into a private relay chat (default:
the user's DM with the bot), then the bot uses copyMessage from that relay.
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import List, Optional, TYPE_CHECKING, Union

from pyrogram import Client
from pyrogram.errors import FloodWait

if TYPE_CHECKING:
    from telegram_sender import TelegramBotSender

logger = logging.getLogger(__name__)


@dataclass
class RelayConfig:
    """
    Two chat IDs for the media relay hop.

    userbot_target: where the Pyrogram user copies media (the bot's DM).
    bot_from_chat:  Bot API from_chat_id (the user's ID in that private chat).
    """

    userbot_target: int
    bot_from_chat: int


async def resolve_relay_config(
    app: Client,
    sender: "TelegramBotSender",
    user_id: int,
    override_userbot_target: int = 0,
    override_bot_from_chat: int = 0,
) -> RelayConfig:
    """Build relay config from bot + user ids."""
    bot_me = await sender.get_me()
    return RelayConfig(
        userbot_target=override_userbot_target or bot_me["id"],
        bot_from_chat=override_bot_from_chat or user_id,
    )


async def ensure_relay_chat(
    app: Client,
    sender: "TelegramBotSender",
    relay: RelayConfig,
) -> bool:
    """
    Ensure the sender bot can access the relay chat.

    Opens the private chat automatically by sending /start from the userbot.
    """
    if await sender.verify_bot_access(relay.bot_from_chat):
        logger.info(
            f"Relay chat ready (userbot→{relay.userbot_target}, "
            f"bot copies from {relay.bot_from_chat})"
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
            logger.info(f"Relay chat opened via /start (bot_from_chat={relay.bot_from_chat})")
            return True

        logger.error(
            f"Relay chat still inaccessible (bot_from_chat={relay.bot_from_chat}). "
            "Send /start to the sender bot from the user account manually."
        )
    except Exception as e:
        logger.error(f"Failed to auto-open relay chat: {e}", exc_info=True)

    return False


async def relay_to_bot(
    app: Client,
    source_chat_id: int,
    message_ids: List[int],
    relay: RelayConfig,
    is_album: bool = False,
) -> List[int]:
    """
    Copy message(s) from the source channel into the bot DM via userbot.

    Returns message ids in the bot DM (for Bot API copyMessage).
    """
    if not message_ids:
        return []

    target = relay.userbot_target

    try:
        if is_album or len(message_ids) > 1:
            copied = await app.copy_media_group(
                chat_id=target,
                from_chat_id=source_chat_id,
                message_id=message_ids[0],
            )
            relay_ids = [msg.id for msg in copied]
            logger.info(
                f"Relayed album ({len(relay_ids)} items) "
                f"source={source_chat_id} → bot DM {target}"
            )
            return relay_ids

        copied = await app.copy_message(
            chat_id=target,
            from_chat_id=source_chat_id,
            message_id=message_ids[0],
        )
        logger.info(
            f"Relayed message {message_ids[0]} "
            f"source={source_chat_id} → bot DM {target} (id={copied.id})"
        )
        return [copied.id]

    except FloodWait as e:
        raise
    except Exception as e:
        logger.error(
            f"Failed to relay message(s) {message_ids} from {source_chat_id}: {e}",
            exc_info=True,
        )
        raise


async def cleanup_relay(
    app: Optional[Client],
    relay: RelayConfig,
    relay_message_ids: List[int],
):
    """Delete relay messages from bot DM after successful delivery."""
    if not app or not relay_message_ids:
        return
    try:
        await app.delete_messages(relay.userbot_target, relay_message_ids)
        logger.debug(f"Cleaned up relay messages {relay_message_ids}")
    except Exception as e:
        logger.debug(f"Relay cleanup skipped: {e}")
