"""
Userbot media relay — copy messages from source to a bot-accessible chat.

Preferred: private relay channel (RELAY_CHANNEL_ID) where userbot posts and
the sender bot copies — message IDs match Bot API reliably.

Fallback: userbot DM with sender bot + Bot API getUpdates to resolve message IDs.
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import List, Optional, TYPE_CHECKING

from pyrogram import Client
from pyrogram.errors import FloodWait

if TYPE_CHECKING:
    from telegram_sender import TelegramBotSender

logger = logging.getLogger(__name__)


@dataclass
class RelayConfig:
    """Relay target shared by userbot (post) and sender bot (copyMessage source)."""

    userbot_target: int
    bot_from_chat: int
    mode: str = "channel"  # "channel" or "dm"


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
            pyrogram_ids = [msg.id for msg in copied]
            logger.info(
                f"Relayed album ({len(pyrogram_ids)} items) "
                f"source={source_chat_id} → {relay.mode} {target}"
            )
        else:
            copied = await app.copy_message(
                chat_id=target,
                from_chat_id=source_chat_id,
                message_id=message_ids[0],
            )
            pyrogram_ids = [copied.id]
            logger.info(
                f"Relayed message {message_ids[0]} "
                f"source={source_chat_id} → {relay.mode} {target} (pyrogram id={copied.id})"
            )

        if relay.mode == "channel":
            return pyrogram_ids

        bot_ids = await sender.resolve_relay_message_ids(
            relay.bot_from_chat, count=len(pyrogram_ids)
        )
        logger.info(f"Resolved Bot API relay ids: {bot_ids} (pyrogram {pyrogram_ids})")
        return bot_ids

    except FloodWait:
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
    """Delete relay messages after successful delivery (best-effort)."""
    if not app or not relay_message_ids:
        return
    try:
        await app.delete_messages(relay.userbot_target, relay_message_ids)
        logger.debug(f"Cleaned up relay messages {relay_message_ids}")
    except Exception as e:
        logger.debug(f"Relay cleanup skipped: {e}")
