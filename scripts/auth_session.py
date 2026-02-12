from __future__ import annotations

import argparse
import asyncio
from getpass import getpass
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError


async def auth(api_id: int, api_hash: str, session: str, reset: bool) -> None:
    session_file = Path(f"{session}.session")
    if reset and session_file.exists():
        session_file.unlink()
        print(f"[auth] Removed existing session file: {session_file}", flush=True)

    print("[auth] Connecting to Telegram...", flush=True)
    client = TelegramClient(session, api_id, api_hash)
    await client.connect()

    if await client.is_user_authorized():
        me = await client.get_me()
        print(f"[auth] Already authorized as: {me.username or me.id}", flush=True)
        print(f"Session saved: {session}", flush=True)
        await client.disconnect()
        return

    phone = input("Enter your phone in international format (e.g. +79990001122): ").strip()
    print("[auth] Sending login code...", flush=True)
    sent = await client.send_code_request(phone)
    code = input("Enter code from Telegram: ").strip()

    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=sent.phone_code_hash)
    except SessionPasswordNeededError:
        password = getpass("2FA password: ")
        await client.sign_in(password=password)

    me = await client.get_me()
    print(f"[auth] Success. Logged in as: {me.username or me.id}", flush=True)
    print(f"Session saved: {session}", flush=True)
    await client.disconnect()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create local Telethon session")
    parser.add_argument("--api-id", type=int, required=True)
    parser.add_argument("--api-hash", required=True)
    parser.add_argument("--session", default="sessions/user")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete existing session file before login",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(auth(args.api_id, args.api_hash, args.session, args.reset))


if __name__ == "__main__":
    main()
