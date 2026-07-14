"""
Userbot media relay — copy messages from source to a bot-accessible chat.

The sender bot only needs access to destination channels. Media is copied
server-side by the Pyrogram user account into a private relay chat (default:
the user's DM with the bot), then the bot uses copyMessage from that relay.
"""

import logging
from typing import List, Optional

from pyrogram import Client
from pyrogram.errors import FloodWait

logger = logging.getLogger(__name__)


async def relay_to_bot(
    app: Client,
    source_chat_id: int,
    message_ids: List[int],
    relay_chat_id: int,
    is_album: bool = False,
) -> List[int]:
    """
    Copy message(s) from the source channel into the relay chat via userbot.

    Returns relay-side message ids (same order as source ids for albums).
    """
    if not message_ids:
        return []

    try:
        if is_album or len(message_ids) > 1:
            copied = await app.copy_media_group(
                chat_id=relay_chat_id,
                from_chat_id=source_chat_id,
                message_id=message_ids[0],
            )
            relay_ids = [msg.id for msg in copied]
            logger.debug(
                f"Relayed album ({len(relay_ids)} items) "
                f"source={source_chat_id} -> relay={relay_chat_id}"
            )
            return relay_ids

        copied = await app.copy_message(
            chat_id=relay_chat_id,
            from_chat_id=source_chat_id,
            message_id=message_ids[0],
        )
        logger.debug(
            f"Relayed message {message_ids[0]} "
            f"source={source_chat_id} -> relay={relay_chat_id} (id={copied.id})"
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
    relay_chat_id: int,
    relay_message_ids: List[int],
):
    """Delete relay messages after successful delivery (best-effort)."""
    if not app or not relay_message_ids:
        return
    try:
        await app.delete_messages(relay_chat_id, relay_message_ids)
        logger.debug(f"Cleaned up relay messages {relay_message_ids}")
    except Exception as e:
        logger.debug(f"Relay cleanup skipped: {e}")
