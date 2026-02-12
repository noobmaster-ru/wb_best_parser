from __future__ import annotations

import argparse
import asyncio

from telethon import TelegramClient


async def auth(api_id: int, api_hash: str, session: str) -> None:
    client = TelegramClient(session, api_id, api_hash)
    await client.start()
    me = await client.get_me()
    print(f"Session saved: {session}. Logged in as: {me.username or me.id}")
    await client.disconnect()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create local Telethon session")
    parser.add_argument("--api-id", type=int, required=True)
    parser.add_argument("--api-hash", required=True)
    parser.add_argument("--session", default="sessions/user")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(auth(args.api_id, args.api_hash, args.session))


if __name__ == "__main__":
    main()
