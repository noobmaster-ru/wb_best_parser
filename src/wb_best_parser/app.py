from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from telethon import TelegramClient, events

from wb_best_parser.config import Settings, get_settings
from wb_best_parser.filters import OfferFilter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("wb_best_parser")


def build_message_header(source_title: str, score: int, reasons: list[str]) -> str:
    reason_text = ", ".join(reasons) if reasons else "no-reason"
    return (
        f"🔥 Интересное предложение\n"
        f"Источник: {source_title}\n"
        f"Score: {score} ({reason_text})"
    )


def ensure_session_path(session_name: str) -> str:
    session_path = Path(session_name)
    if session_path.parent and str(session_path.parent) != ".":
        session_path.parent.mkdir(parents=True, exist_ok=True)
    return str(session_path)


async def run(settings: Settings) -> None:
    session_name = ensure_session_path(settings.tg_session)
    file_sources = settings.load_source_chats_from_file()
    source_chats = file_sources or settings.source_chats_list()

    if not source_chats:
        raise ValueError(
            "No source chats configured. Fill targets.txt or SOURCE_CHATS in .env"
        )

    client = TelegramClient(
        session=session_name,
        api_id=settings.tg_api_id,
        api_hash=settings.tg_api_hash,
    )

    offer_filter = OfferFilter(
        include_keywords=settings.include_keywords_list(),
        exclude_keywords=settings.exclude_keywords_list(),
        min_score=settings.min_score,
    )

    source_entity_cache: dict[int, str] = {}

    @client.on(events.NewMessage(chats=source_chats))
    async def on_new_message(event: events.NewMessage.Event) -> None:
        message = event.message
        text = message.message or ""
        result = offer_filter.match(text)

        if not result.is_interesting:
            logger.debug("Skip message %s, score=%s", message.id, result.score)
            return

        chat = await event.get_chat()
        chat_id = event.chat_id or 0
        if chat_id not in source_entity_cache:
            source_entity_cache[chat_id] = getattr(chat, "title", None) or str(chat_id)

        source_title = source_entity_cache[chat_id]
        header = build_message_header(source_title, result.score, result.reasons)
        composed_text = f"{header}\n\n{text}".strip()

        if settings.dry_run:
            logger.info("[DRY_RUN] matched from %s: %s", source_title, composed_text[:250])
            return

        await client.send_message(settings.target_chat, composed_text)
        if message.media:
            await client.forward_messages(settings.target_chat, message)

        logger.info(
            "Published from %s (message_id=%s, score=%s)",
            source_title,
            message.id,
            result.score,
        )

    logger.info("Starting parser. Listening channels: %s", ", ".join(source_chats))
    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError(
            "Telegram session is not authorized. Recreate sessions/user.session via "
            "scripts/auth_session.py using the same TG_API_ID/TG_API_HASH from .env."
        )
    await client.run_until_disconnected()


def main() -> None:
    settings = get_settings()
    try:
        asyncio.run(run(settings))
    except KeyboardInterrupt:
        logger.info("Stopped by user")


if __name__ == "__main__":
    main()
