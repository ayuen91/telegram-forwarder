"""
One-shot interactive Hydrogram session generator.
Run this inside the bot container with -it (interactive TTY) to create
a fresh session file at /app/sessions/user_session.session.

Usage (from repo root on the VPS):
  sudo docker compose run --rm -it --entrypoint python bot /app/generate_session.py
"""

import asyncio
import os
import sys

# Same monkey-patch as main.py — required for 64-bit channel IDs
from hydrogram import utils

def get_peer_type_new(peer_id: int) -> str:
    peer_id_str = str(peer_id)
    if not peer_id_str.startswith("-"):
        return "user"
    elif peer_id_str.startswith("-100"):
        return "channel"
    else:
        return "chat"

utils.get_peer_type = get_peer_type_new

from hydrogram import Client
from dotenv import load_dotenv

load_dotenv()


async def main():
    api_id = int(os.environ["API_ID"])
    api_hash = os.environ["API_HASH"]
    phone_number = os.environ["PHONE_NUMBER"]
    workdir = os.getenv("HYDROGRAM_WORKDIR", os.getenv("PYROGRAM_WORKDIR", "/app/sessions"))

    print(f"\n{'='*60}")
    print("Hydrogram Session Generator")
    print(f"{'='*60}")
    print(f"API ID     : {api_id}")
    print(f"Phone      : {phone_number}")
    print(f"Session dir: {workdir}")
    print(f"{'='*60}\n")

    app = Client(
        name="user_session",
        api_id=api_id,
        api_hash=api_hash,
        phone_number=phone_number,
        workdir=workdir,
    )

    async with app:
        me = await app.get_me()
        print(f"\n✅ Successfully authenticated as: {me.first_name} (ID: {me.id})")
        print(f"Session saved to: {workdir}/user_session.session")
        print("\nYou can now restart the bot with: sudo docker compose up -d bot\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nAborted.")
        sys.exit(1)
