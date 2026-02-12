from __future__ import annotations

import asyncio
import logging
import tempfile
from pathlib import Path

from telethon import TelegramClient, events

from wb_best_parser.config import Settings, get_settings
from wb_best_parser.dedup import DedupStore
from wb_best_parser.filters import OfferFilter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("wb_best_parser")
MEDIA_CAPTION_LIMIT = 1024


def ensure_session_path(session_name: str) -> str:
    session_path = Path(session_name)
    if session_path.parent and str(session_path.parent) != ".":
        session_path.parent.mkdir(parents=True, exist_ok=True)
    return str(session_path)


async def run(settings: Settings) -> None:
    session_name = ensure_session_path(settings.tg_session)
    try:
        file_sources = settings.load_source_chats_from_file()
    except OSError as exc:
        logger.warning(
            "Failed reading %s (%s). Falling back to SOURCE_CHATS.",
            settings.targets_file,
            exc,
        )
        file_sources = []
    source_chats = file_sources or settings.source_chats_list()

    if not source_chats:
        raise ValueError(
            "No source chats configured. Fill targets.txt or SOURCE_CHATS in .env"
        )
    if file_sources:
        logger.info("Loaded %s source chats from %s", len(file_sources), settings.targets_file)
    else:
        logger.info("Loaded %s source chats from SOURCE_CHATS", len(source_chats))

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
    dedup_store = DedupStore(
        path=settings.dedup_store_file,
        max_items=settings.dedup_max_items,
    )
    dedup_lock = asyncio.Lock()

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
        composed_text = text.strip()
        fingerprint = DedupStore.fingerprint(composed_text)
        if fingerprint:
            async with dedup_lock:
                if dedup_store.contains(fingerprint):
                    logger.info("Skip duplicate post from %s (message_id=%s)", source_title, message.id)
                    return
                # Reserve fingerprint immediately to avoid race between concurrent events.
                dedup_store.add(fingerprint)
                dedup_store.flush()

        if settings.dry_run:
            logger.info("[DRY_RUN] matched from %s: %s", source_title, composed_text[:250])
            return

        if message.media:
            with tempfile.TemporaryDirectory() as tmpdir:
                downloaded_media = await client.download_media(message, file=tmpdir)
                if downloaded_media:
                    caption = composed_text[:MEDIA_CAPTION_LIMIT] if composed_text else None
                    tail = composed_text[MEDIA_CAPTION_LIMIT:].strip() if composed_text else ""
                    await client.send_file(
                        settings.target_chat,
                        file=downloaded_media,
                        caption=caption,
                    )
                    if tail:
                        await client.send_message(settings.target_chat, tail)
                else:
                    await client.send_message(settings.target_chat, composed_text)
        else:
            await client.send_message(settings.target_chat, composed_text)

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
