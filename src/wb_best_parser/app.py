from __future__ import annotations

import asyncio
import logging
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from telethon import TelegramClient, events
from telethon.utils import get_peer_id

from infrastructure.openai import OpenAIConfig, OpenAIGateway
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
    openai_gateway: OpenAIGateway | None = None
    if settings.rewrite_with_ai and settings.openai_api_key:
        openai_gateway = OpenAIGateway(
            OpenAIConfig(
                openai_api_key=settings.openai_api_key,
                model=settings.openai_model,
                proxy=settings.openai_proxy,
            )
        )
        logger.info("AI rewrite is enabled (%s)", settings.openai_model)
    elif settings.rewrite_with_ai and not settings.openai_api_key:
        logger.warning("REWRITE_WITH_AI is enabled but OPENAI_API_KEY is empty. AI rewrite disabled.")

    @dataclass(slots=True)
    class Candidate:
        message: object
        source_title: str
        score: int
        reasons: list[str]
        original_text: str
        created_at: datetime

    source_entity_cache: dict[int, str] = {}
    top_mode_enabled = settings.publish_top_n > 0
    top_window_seconds = max(60, settings.top_window_minutes * 60)

    def parse_channel_id(raw_source: str) -> int | None:
        raw = raw_source.strip()
        if not raw or not raw.startswith("-100"):
            return None
        suffix = raw[4:]
        return int(suffix) if suffix.isdigit() else None

    async def resolve_sources(raw_sources: list[str]) -> tuple[list, list[str]]:
        resolved_entities = []
        resolved_titles = []
        dialogs_cache = None

        for raw_source in raw_sources:
            try:
                input_entity = await client.get_input_entity(raw_source)
                entity = await client.get_entity(input_entity)
            except Exception:
                entity = None
                channel_id = parse_channel_id(raw_source)
                if channel_id is not None:
                    if dialogs_cache is None:
                        dialogs_cache = [d async for d in client.iter_dialogs()]
                    for dialog in dialogs_cache:
                        dialog_entity = getattr(dialog, "entity", None)
                        if getattr(dialog_entity, "id", None) == channel_id:
                            entity = dialog_entity
                            break

                if entity is None:
                    logger.warning("Skip source %s: cannot resolve entity", raw_source)
                    continue

            resolved_entities.append(entity)
            title = getattr(entity, "title", None) or getattr(entity, "username", None) or str(raw_source)
            resolved_titles.append(title)

            chat_id = getattr(entity, "id", None)
            if isinstance(chat_id, int):
                source_entity_cache[chat_id] = title
            try:
                source_entity_cache[get_peer_id(entity)] = title
            except Exception:
                pass

        return resolved_entities, resolved_titles

    async def publish_message(message, composed_text: str, downloaded_media: str | None) -> None:
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
        elif message.media:
            await client.send_message(settings.target_chat, composed_text)
        else:
            await client.send_message(settings.target_chat, composed_text)

    async def publish_with_dedup(
        message,
        source_title: str,
        composed_text: str,
        score: int,
        reasons: list[str],
    ) -> bool:
        text_fingerprint = DedupStore.fingerprint(composed_text)
        media_fingerprint: str | None = None
        media_temp_dir: tempfile.TemporaryDirectory[str] | None = None
        downloaded_media: str | None = None

        if message.media:
            media_temp_dir = tempfile.TemporaryDirectory()
            downloaded = await client.download_media(message, file=media_temp_dir.name)
            if isinstance(downloaded, str):
                downloaded_media = downloaded
                if settings.dedup_media:
                    try:
                        media_bytes = Path(downloaded_media).read_bytes()
                        raw_hash = DedupStore.fingerprint_bytes(media_bytes)
                        media_fingerprint = f"img:{raw_hash}" if raw_hash else None
                    except OSError as exc:
                        logger.warning("Failed to hash media for message %s: %s", message.id, exc)

        dedup_keys: list[str] = []
        if text_fingerprint:
            dedup_keys.append(f"txt:{text_fingerprint}")
        if media_fingerprint:
            dedup_keys.append(media_fingerprint)

        reserved_keys: list[str] = []
        if dedup_keys:
            async with dedup_lock:
                duplicate_key = next((key for key in dedup_keys if dedup_store.contains(key)), None)
                if duplicate_key:
                    logger.info(
                        "Skip duplicate post from %s (message_id=%s, key=%s)",
                        source_title,
                        message.id,
                        duplicate_key,
                    )
                    if media_temp_dir:
                        media_temp_dir.cleanup()
                    return False
                for key in dedup_keys:
                    dedup_store.add(key)
                    reserved_keys.append(key)
                dedup_store.flush()

        if settings.dry_run:
            logger.info("[DRY_RUN] matched from %s: %s", source_title, composed_text[:250])
            if media_temp_dir:
                media_temp_dir.cleanup()
            return True

        try:
            await publish_message(message, composed_text, downloaded_media)
        except Exception:
            if reserved_keys:
                async with dedup_lock:
                    for key in reserved_keys:
                        dedup_store.remove(key)
                    dedup_store.flush()
            if media_temp_dir:
                media_temp_dir.cleanup()
            raise
        finally:
            if media_temp_dir:
                media_temp_dir.cleanup()

        logger.info(
            "Published from %s (message_id=%s, score=%s, reasons=%s)",
            source_title,
            message.id,
            score,
            reasons,
        )
        return True

    def to_utc(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    async def build_candidate(message, source_title: str) -> Candidate | None:
        text = message.message or ""
        result = offer_filter.match(text)

        if not result.is_interesting:
            logger.debug("Skip message %s, score=%s", message.id, result.score)
            return None

        created_at = to_utc(getattr(message, "date", None)) or datetime.now(UTC)
        return Candidate(
            message=message,
            source_title=source_title,
            score=result.score,
            reasons=result.reasons,
            original_text=text.strip(),
            created_at=created_at,
        )

    async def publish_candidate(candidate: Candidate) -> bool:
        composed_text = candidate.original_text
        if openai_gateway and composed_text:
            composed_text = await openai_gateway.rewrite_offer(composed_text)

        return await publish_with_dedup(
            message=candidate.message,
            source_title=candidate.source_title,
            composed_text=composed_text,
            score=candidate.score,
            reasons=candidate.reasons,
        )

    def source_title_for_entity(entity) -> str:
        title = getattr(entity, "title", None) or str(getattr(entity, "id", "unknown"))
        entity_id = getattr(entity, "id", None)
        if isinstance(entity_id, int):
            source_entity_cache[entity_id] = title
        return title

    def select_top_candidates(candidates: list[Candidate], limit: int | None = None) -> list[Candidate]:
        candidates.sort(
            key=lambda c: (
                c.score,
                c.created_at,
                getattr(c.message, "id", 0),
            ),
            reverse=True,
        )
        if limit is None:
            return candidates
        return candidates[:limit]

    async def process_message_immediate(message, source_title: str) -> None:
        candidate = await build_candidate(message, source_title)
        if not candidate:
            return
        await publish_candidate(candidate)

    async def on_new_message(event: events.NewMessage.Event) -> None:
        chat = await event.get_chat()
        chat_id = event.chat_id or 0
        if chat_id not in source_entity_cache:
            source_entity_cache[chat_id] = getattr(chat, "title", None) or str(chat_id)
        await process_message_immediate(event.message, source_entity_cache[chat_id])

    async def process_backfill(resolved_entities: list) -> None:
        if settings.backfill_hours <= 0:
            return

        since_utc = datetime.now(UTC) - timedelta(hours=settings.backfill_hours)
        logger.info("Backfill started: checking last %s hour(s)", settings.backfill_hours)

        for entity in resolved_entities:
            try:
                source_title = source_title_for_entity(entity)
            except Exception as exc:
                logger.warning("Backfill skip source: %s", exc)
                continue

            recent_messages = []
            async for msg in client.iter_messages(entity, limit=settings.backfill_limit_per_chat):
                msg_date = to_utc(getattr(msg, "date", None))
                if not msg_date:
                    continue
                if msg_date < since_utc:
                    break
                recent_messages.append(msg)

            for msg in reversed(recent_messages):
                await process_message_immediate(msg, source_title)

            logger.info(
                "Backfill checked %s message(s) for %s",
                len(recent_messages),
                source_title,
            )

    async def collect_candidates_for_window(
        resolved_entities: list,
        window_start_utc: datetime,
        window_end_utc: datetime,
    ) -> list[Candidate]:
        candidates: list[Candidate] = []

        for entity in resolved_entities:
            source_title = source_title_for_entity(entity)
            scanned_count = 0
            matched_count = 0

            async for msg in client.iter_messages(entity, limit=settings.backfill_limit_per_chat):
                msg_date = to_utc(getattr(msg, "date", None))
                if not msg_date:
                    continue
                if msg_date < window_start_utc:
                    break
                if msg_date > window_end_utc:
                    continue

                scanned_count += 1
                candidate = await build_candidate(msg, source_title)
                if not candidate:
                    continue
                matched_count += 1
                candidates.append(candidate)

            logger.info(
                "Window scan: %s inspected=%s matched=%s",
                source_title,
                scanned_count,
                matched_count,
            )

        return candidates

    async def publish_window_top(
        resolved_entities: list,
        window_start_utc: datetime,
        window_end_utc: datetime,
        reason: str,
    ) -> None:
        candidates = await collect_candidates_for_window(
            resolved_entities=resolved_entities,
            window_start_utc=window_start_utc,
            window_end_utc=window_end_utc,
        )
        if not candidates:
            logger.info(
                "Top mode flush (%s): no candidates for window %s -> %s",
                reason,
                window_start_utc.isoformat(),
                window_end_utc.isoformat(),
            )
            return

        ranked_candidates = select_top_candidates(candidates, limit=None)
        logger.info(
            "Top mode flush (%s): ranked %s candidate(s), trying publish top non-duplicate",
            reason,
            len(ranked_candidates),
        )
        for index, candidate in enumerate(ranked_candidates, start=1):
            published = await publish_candidate(candidate)
            if published:
                logger.info(
                    "Top mode flush (%s): published rank=%s message_id=%s score=%s from %s",
                    reason,
                    index,
                    candidate.message.id,
                    candidate.score,
                    candidate.source_title,
                )
                return

        logger.info(
            "Top mode flush (%s): all ranked candidates are duplicates, nothing published",
            reason,
        )

    async def top_mode_loop(resolved_entities: list) -> None:
        if not top_mode_enabled:
            return
        logger.info(
            "Top mode enabled: publishing top1 every %s minute(s)",
            settings.top_window_minutes,
        )
        initial_window_delta = (
            timedelta(hours=settings.backfill_hours)
            if settings.backfill_hours > 0
            else timedelta(seconds=top_window_seconds)
        )
        window_start_utc = datetime.now(UTC) - initial_window_delta
        next_run_utc = datetime.now(UTC)
        while True:
            now_utc = datetime.now(UTC)
            sleep_seconds = (next_run_utc - now_utc).total_seconds()
            if sleep_seconds > 0:
                await asyncio.sleep(sleep_seconds)

            window_end_utc = datetime.now(UTC)
            await publish_window_top(
                resolved_entities=resolved_entities,
                window_start_utc=window_start_utc,
                window_end_utc=window_end_utc,
                reason="scheduled",
            )
            window_start_utc = window_end_utc
            next_run_utc = window_end_utc + timedelta(seconds=top_window_seconds)

    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError(
            "Telegram session is not authorized. Recreate sessions/user.session via "
            "scripts/auth_session.py using the same TG_API_ID/TG_API_HASH from .env."
        )

    resolved_entities, resolved_titles = await resolve_sources(source_chats)
    if not resolved_entities:
        raise ValueError("No resolvable source channels/chats. Check targets.txt and account access.")

    logger.info("Starting parser. Listening channels: %s", ", ".join(resolved_titles))
    top_task: asyncio.Task[None] | None = None
    if top_mode_enabled:
        top_task = asyncio.create_task(top_mode_loop(resolved_entities))
    else:
        client.add_event_handler(on_new_message, events.NewMessage(chats=resolved_entities))
        await process_backfill(resolved_entities)
    try:
        await client.run_until_disconnected()
    finally:
        if top_task:
            top_task.cancel()
            with suppress(asyncio.CancelledError):
                await top_task


def main() -> None:
    settings = get_settings()
    try:
        asyncio.run(run(settings))
    except KeyboardInterrupt:
        logger.info("Stopped by user")


if __name__ == "__main__":
    main()
