from __future__ import annotations

import asyncio
import json
import logging
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from telethon import TelegramClient, events
from telethon.utils import get_peer_id

from infrastructure.openai import OpenAIConfig, OpenAIGateway
from wb_best_parser.config import Settings, get_settings
from wb_best_parser.constants import (
    BACKFILL_HOURS,
    BACKFILL_LIMIT_PER_CHAT,
    CACHED_SCORE_THRESHOLD,
    DEDUP_MAX_ITEMS,
    DEDUP_MEDIA,
    DEDUP_STORE_FILE,
    DRY_RUN,
    EVENING_PEAK_END_HOUR,
    EVENING_PEAK_INTERVAL_MINUTES,
    EVENING_PEAK_START_HOUR,
    EXCLUDE_KEYWORDS_LIST,
    INCLUDE_KEYWORDS_LIST,
    MIN_SCORE,
    OPENAI_MODEL,
    PUBLISH_TOP_N,
    QUIET_END_HOUR,
    QUIET_START_HOUR,
    REWRITE_WITH_AI,
    SCHEDULE_TIMEZONE,
    TOP_CACHE_HASHES_FILE,
    TOP_CACHE_ITEMS_FILE,
    TOP_CACHE_MAX_ITEMS,
    TOP_WINDOW_MINUTES,
)
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
        include_keywords=INCLUDE_KEYWORDS_LIST,
        exclude_keywords=EXCLUDE_KEYWORDS_LIST,
        min_score=MIN_SCORE,
    )
    dedup_store = DedupStore(
        path=DEDUP_STORE_FILE,
        max_items=DEDUP_MAX_ITEMS,
    )
    top_cache_store = DedupStore(
        path=TOP_CACHE_HASHES_FILE,
        max_items=TOP_CACHE_MAX_ITEMS,
    )
    dedup_lock = asyncio.Lock()
    top_cache_lock = asyncio.Lock()
    openai_gateway: OpenAIGateway | None = None
    if REWRITE_WITH_AI and settings.openai_api_key:
        openai_gateway = OpenAIGateway(
            OpenAIConfig(
                openai_api_key=settings.openai_api_key,
                model=OPENAI_MODEL,
                proxy=settings.openai_proxy,
            )
        )
        logger.info("AI rewrite is enabled (%s)", OPENAI_MODEL)
    elif REWRITE_WITH_AI and not settings.openai_api_key:
        logger.warning("REWRITE_WITH_AI is enabled but OPENAI_API_KEY is empty. AI rewrite disabled.")

    @dataclass(slots=True)
    class Candidate:
        message: Any
        source_title: str
        score: int
        reasons: list[str]
        original_text: str
        created_at: datetime

    @dataclass(slots=True)
    class CachedCandidate:
        cache_key: str
        source_title: str
        source_peer_id: int | None
        message_id: int
        score: int
        reasons: list[str]
        original_text: str
        created_at: str

    source_entity_cache: dict[int, str] = {}
    source_entity_lookup: dict[int, Any] = {}
    top_cached_candidates: list[CachedCandidate] = []
    top_cache_items_path = Path(TOP_CACHE_ITEMS_FILE)
    top_mode_enabled = PUBLISH_TOP_N > 0
    base_window_seconds = max(60, TOP_WINDOW_MINUTES * 60)
    evening_peak_window_seconds = max(60, EVENING_PEAK_INTERVAL_MINUTES * 60)
    schedule_tz = ZoneInfo(SCHEDULE_TIMEZONE)

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
                source_entity_lookup[chat_id] = entity
            try:
                peer_id = get_peer_id(entity)
                source_entity_cache[peer_id] = title
                source_entity_lookup[peer_id] = entity
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
        dedup_text: str | None = None,
    ) -> bool:
        dedup_base_text = dedup_text if dedup_text is not None else composed_text
        text_fingerprint = DedupStore.fingerprint(dedup_base_text)
        media_fingerprint: str | None = None
        media_temp_dir: tempfile.TemporaryDirectory[str] | None = None
        downloaded_media: str | None = None

        if message.media:
            media_temp_dir = tempfile.TemporaryDirectory()
            downloaded = await client.download_media(message, file=media_temp_dir.name)
            if isinstance(downloaded, str):
                downloaded_media = downloaded
                if DEDUP_MEDIA:
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

        if DRY_RUN:
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

    async def is_candidate_duplicate(candidate: Candidate) -> bool:
        text_fingerprint = DedupStore.fingerprint(candidate.original_text)
        text_key = f"txt:{text_fingerprint}" if text_fingerprint else None
        media_key: str | None = None
        media_temp_dir: tempfile.TemporaryDirectory[str] | None = None

        if candidate.message.media and DEDUP_MEDIA:
            media_temp_dir = tempfile.TemporaryDirectory()
            downloaded = await client.download_media(candidate.message, file=media_temp_dir.name)
            if isinstance(downloaded, str):
                try:
                    media_bytes = Path(downloaded).read_bytes()
                    media_hash = DedupStore.fingerprint_bytes(media_bytes)
                    media_key = f"img:{media_hash}" if media_hash else None
                except OSError as exc:
                    logger.warning(
                        "Failed to hash media for duplicate pre-check message %s: %s",
                        candidate.message.id,
                        exc,
                    )

        dedup_keys = [key for key in (text_key, media_key) if key]
        if not dedup_keys:
            if media_temp_dir:
                media_temp_dir.cleanup()
            return False

        async with dedup_lock:
            duplicate_key = next((key for key in dedup_keys if dedup_store.contains(key)), None)

        if media_temp_dir:
            media_temp_dir.cleanup()

        if duplicate_key:
            logger.info(
                "Top mode skip before rewrite: duplicate key=%s for message_id=%s from %s",
                duplicate_key,
                candidate.message.id,
                candidate.source_title,
            )
        return duplicate_key is not None

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
            dedup_text=candidate.original_text,
        )

    def candidate_cache_key(candidate: Candidate) -> str | None:
        message_id = getattr(candidate.message, "id", None)
        peer_id = None
        with suppress(Exception):
            peer = getattr(candidate.message, "peer_id", None)
            if peer is not None:
                peer_id = get_peer_id(peer)

        if isinstance(peer_id, int) and isinstance(message_id, int):
            return f"msg:{peer_id}:{message_id}"

        text_fingerprint = DedupStore.fingerprint(candidate.original_text)
        if text_fingerprint:
            return f"txt:{text_fingerprint}"
        return None

    def parse_cached_created_at(raw_value: str) -> datetime:
        try:
            parsed = datetime.fromisoformat(raw_value)
        except ValueError:
            return datetime.now(UTC)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    def candidate_to_cached(candidate: Candidate) -> CachedCandidate | None:
        cache_key = candidate_cache_key(candidate)
        message_id = getattr(candidate.message, "id", None)
        if not cache_key or not isinstance(message_id, int):
            return None

        source_peer_id: int | None = None
        with suppress(Exception):
            peer = getattr(candidate.message, "peer_id", None)
            if peer is not None:
                source_peer_id = get_peer_id(peer)

        return CachedCandidate(
            cache_key=cache_key,
            source_title=candidate.source_title,
            source_peer_id=source_peer_id,
            message_id=message_id,
            score=candidate.score,
            reasons=list(candidate.reasons),
            original_text=candidate.original_text,
            created_at=candidate.created_at.isoformat(),
        )

    def load_top_cache_items() -> list[CachedCandidate]:
        if not top_cache_items_path.exists():
            return []

        loaded: list[CachedCandidate] = []
        try:
            lines = top_cache_items_path.read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            logger.warning("Top cache load failed (%s): %s", top_cache_items_path, exc)
            return []

        for line in lines:
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue

            if not isinstance(payload, dict):
                continue

            cache_key = payload.get("cache_key")
            source_title = payload.get("source_title")
            message_id = payload.get("message_id")
            score = payload.get("score")
            original_text = payload.get("original_text")
            created_at = payload.get("created_at")
            reasons_raw = payload.get("reasons")
            source_peer_raw = payload.get("source_peer_id")

            if not isinstance(cache_key, str) or not cache_key:
                continue
            if not isinstance(source_title, str):
                continue
            if not isinstance(message_id, int):
                continue
            if not isinstance(score, int):
                continue
            if not isinstance(original_text, str):
                continue
            if not isinstance(created_at, str):
                continue

            reasons: list[str] = []
            if isinstance(reasons_raw, list):
                reasons = [value for value in reasons_raw if isinstance(value, str)]

            source_peer_id = source_peer_raw if isinstance(source_peer_raw, int) else None
            loaded.append(
                CachedCandidate(
                    cache_key=cache_key,
                    source_title=source_title,
                    source_peer_id=source_peer_id,
                    message_id=message_id,
                    score=score,
                    reasons=reasons,
                    original_text=original_text,
                    created_at=created_at,
                )
            )

        if len(loaded) > TOP_CACHE_MAX_ITEMS:
            loaded = loaded[-TOP_CACHE_MAX_ITEMS:]
        return loaded

    def flush_top_cache_items() -> None:
        payloads = [
            json.dumps(
                {
                    "cache_key": item.cache_key,
                    "source_title": item.source_title,
                    "source_peer_id": item.source_peer_id,
                    "message_id": item.message_id,
                    "score": item.score,
                    "reasons": item.reasons,
                    "original_text": item.original_text,
                    "created_at": item.created_at,
                },
                ensure_ascii=False,
            )
            for item in top_cached_candidates[-TOP_CACHE_MAX_ITEMS:]
        ]
        content = "\n".join(payloads)
        if content:
            content = f"{content}\n"
        top_cache_items_path.parent.mkdir(parents=True, exist_ok=True)
        top_cache_items_path.write_text(content, encoding="utf-8")

    async def remove_from_top_cache(cache_key: str) -> None:
        async with top_cache_lock:
            previous_len = len(top_cached_candidates)
            top_cached_candidates[:] = [item for item in top_cached_candidates if item.cache_key != cache_key]
            if len(top_cached_candidates) != previous_len:
                flush_top_cache_items()

    async def materialize_cached_candidate(cached: CachedCandidate) -> Candidate | None:
        if cached.source_peer_id is None:
            return None

        entity = source_entity_lookup.get(cached.source_peer_id)
        if entity is None:
            return None

        cached_message = await client.get_messages(entity, ids=cached.message_id)
        if isinstance(cached_message, list):
            cached_message = cached_message[0] if cached_message else None
        if not cached_message:
            return None

        created_at = to_utc(getattr(cached_message, "date", None)) or parse_cached_created_at(cached.created_at)
        original_text = cached.original_text or (getattr(cached_message, "message", "") or "")
        return Candidate(
            message=cached_message,
            source_title=cached.source_title,
            score=cached.score,
            reasons=list(cached.reasons),
            original_text=original_text,
            created_at=created_at,
        )

    loaded_cached_candidates = load_top_cache_items()
    if loaded_cached_candidates:
        top_cached_candidates.extend(loaded_cached_candidates)
        for cached in loaded_cached_candidates:
            top_cache_store.add(cached.cache_key)
        top_cache_store.flush()
        logger.info(
            "Top cache: loaded %s candidate(s) from %s",
            len(loaded_cached_candidates),
            TOP_CACHE_ITEMS_FILE,
        )

    async def add_to_top_cache(candidates: list[Candidate]) -> None:
        added = 0
        async with top_cache_lock:
            for candidate in candidates:
                cached = candidate_to_cached(candidate)
                if not cached or top_cache_store.contains(cached.cache_key):
                    continue
                top_cache_store.add(cached.cache_key)
                top_cached_candidates.append(cached)
                added += 1
            if added:
                if len(top_cached_candidates) > TOP_CACHE_MAX_ITEMS:
                    top_cached_candidates[:] = top_cached_candidates[-TOP_CACHE_MAX_ITEMS:]
                top_cache_store.flush()
                flush_top_cache_items()
        if added:
            logger.info("Top cache: added %s candidate(s)", added)

    async def publish_from_top_cache(reason: str) -> bool:
        async with top_cache_lock:
            ranked_cached = list(top_cached_candidates)
        if not ranked_cached:
            return False

        ranked_cached.sort(
            key=lambda c: (
                c.score,
                parse_cached_created_at(c.created_at),
                c.message_id,
            ),
            reverse=True,
        )
        logger.info("Top mode flush (%s): trying %s cached candidate(s)", reason, len(ranked_cached))

        for cached in ranked_cached:
            candidate = await materialize_cached_candidate(cached)
            if candidate is None:
                await remove_from_top_cache(cached.cache_key)
                continue

            is_duplicate = await is_candidate_duplicate(candidate)
            if not is_duplicate:
                published = await publish_candidate(candidate)
                if published:
                    logger.info(
                        "Top mode flush (%s): published from cache message_id=%s score=%s from %s",
                        reason,
                        candidate.message.id,
                        candidate.score,
                        candidate.source_title,
                    )
                await remove_from_top_cache(cached.cache_key)
                return True
            await remove_from_top_cache(cached.cache_key)

        return False

    def source_title_for_entity(entity) -> str:
        title = getattr(entity, "title", None) or str(getattr(entity, "id", "unknown"))
        entity_id = getattr(entity, "id", None)
        if isinstance(entity_id, int):
            source_entity_cache[entity_id] = title
            source_entity_lookup[entity_id] = entity
        with suppress(Exception):
            peer_id = get_peer_id(entity)
            source_entity_cache[peer_id] = title
            source_entity_lookup[peer_id] = entity
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
        if BACKFILL_HOURS <= 0:
            return

        since_utc = datetime.now(UTC) - timedelta(hours=BACKFILL_HOURS)
        logger.info("Backfill started: checking last %s hour(s)", BACKFILL_HOURS)

        for entity in resolved_entities:
            try:
                source_title = source_title_for_entity(entity)
            except Exception as exc:
                logger.warning("Backfill skip source: %s", exc)
                continue

            recent_messages = []
            async for msg in client.iter_messages(entity, limit=BACKFILL_LIMIT_PER_CHAT):
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

            async for msg in client.iter_messages(entity, limit=BACKFILL_LIMIT_PER_CHAT):
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
            published_from_cache = await publish_from_top_cache(reason)
            if not published_from_cache:
                logger.info(
                    "Top mode flush (%s): no candidates for window %s -> %s",
                    reason,
                    window_start_utc.isoformat(),
                    window_end_utc.isoformat(),
                )
            return

        ranked_candidates = select_top_candidates(candidates, limit=None)
        max_score = ranked_candidates[0].score
        top_score_candidates = [c for c in ranked_candidates if c.score == max_score]

        fresh_priority_candidates = [c for c in ranked_candidates if c.score > CACHED_SCORE_THRESHOLD]
        fresh_fallback_candidates = [c for c in ranked_candidates if c.score <= CACHED_SCORE_THRESHOLD]

        published_from_fresh = False
        published_cache_key: str | None = None

        score_groups: dict[int, list[Candidate]] = {}
        for candidate in fresh_priority_candidates:
            score_groups.setdefault(candidate.score, []).append(candidate)

        for score in sorted(score_groups, reverse=True):
            group = score_groups[score]
            logger.info(
                "Top mode flush (%s): trying %s candidate(s) with score=%s",
                reason,
                len(group),
                score,
            )
            for candidate in group:
                if await is_candidate_duplicate(candidate):
                    continue

                published = await publish_candidate(candidate)
                if published:
                    published_from_fresh = True
                    published_cache_key = candidate_cache_key(candidate)
                    logger.info(
                        "Top mode flush (%s): published message_id=%s score=%s from %s",
                        reason,
                        candidate.message.id,
                        candidate.score,
                        candidate.source_title,
                    )
                    break
            if published_from_fresh:
                break

        top_score_to_cache = [
            candidate
            for candidate in top_score_candidates
            if candidate.score > CACHED_SCORE_THRESHOLD
            and candidate_cache_key(candidate) != published_cache_key
        ]
        if top_score_to_cache:
            await add_to_top_cache(top_score_to_cache)
        if published_from_fresh:
            return

        if not fresh_priority_candidates:
            published_from_cache = await publish_from_top_cache(reason)
            if published_from_cache:
                return

            if fresh_fallback_candidates:
                logger.info(
                    "Top mode flush (%s): no score>%s, trying %s fresh fallback candidate(s)",
                    reason,
                    CACHED_SCORE_THRESHOLD,
                    len(fresh_fallback_candidates),
                )
                for candidate in fresh_fallback_candidates:
                    if await is_candidate_duplicate(candidate):
                        continue
                    published = await publish_candidate(candidate)
                    if published:
                        logger.info(
                            "Top mode flush (%s): published fallback message_id=%s score=%s from %s",
                            reason,
                            candidate.message.id,
                            candidate.score,
                            candidate.source_title,
                        )
                        return

        if not published_from_fresh:
            logger.info(
                "Top mode flush (%s): no non-duplicate candidates, nothing published",
                reason,
            )

    async def top_mode_loop(resolved_entities: list) -> None:
        if not top_mode_enabled:
            return
        logger.info(
            (
                "Top mode enabled: timezone=%s, quiet=%02d:00-%02d:00, "
                "day=%s min, evening_peak(%02d:00-%02d:00)=%s min"
            ),
            SCHEDULE_TIMEZONE,
            QUIET_START_HOUR,
            QUIET_END_HOUR,
            TOP_WINDOW_MINUTES,
            EVENING_PEAK_START_HOUR,
            EVENING_PEAK_END_HOUR,
            EVENING_PEAK_INTERVAL_MINUTES,
        )
        initial_window_delta = (
            timedelta(hours=BACKFILL_HOURS)
            if BACKFILL_HOURS > 0
            else timedelta(seconds=base_window_seconds)
        )
        window_start_utc = datetime.now(UTC) - initial_window_delta
        next_run_utc = datetime.now(UTC)
        while True:
            now_utc = datetime.now(UTC)
            sleep_seconds = (next_run_utc - now_utc).total_seconds()
            if sleep_seconds > 0:
                await asyncio.sleep(sleep_seconds)

            window_end_utc = datetime.now(UTC)
            local_now = window_end_utc.astimezone(schedule_tz)

            if QUIET_START_HOUR <= local_now.hour < QUIET_END_HOUR:
                local_midnight = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
                local_morning_start = local_now.replace(
                    hour=QUIET_END_HOUR,
                    minute=0,
                    second=0,
                    microsecond=0,
                )
                if local_now.hour >= QUIET_END_HOUR:
                    local_morning_start += timedelta(days=1)

                window_start_utc = local_midnight.astimezone(UTC)
                next_run_utc = local_morning_start.astimezone(UTC)
                logger.info(
                    "Quiet window (%s): no publish until %s",
                    SCHEDULE_TIMEZONE,
                    local_morning_start.isoformat(),
                )
                continue

            await publish_window_top(
                resolved_entities=resolved_entities,
                window_start_utc=window_start_utc,
                window_end_utc=window_end_utc,
                reason="scheduled",
            )
            window_start_utc = window_end_utc

            if EVENING_PEAK_START_HOUR <= local_now.hour < EVENING_PEAK_END_HOUR:
                interval_seconds = evening_peak_window_seconds
            else:
                interval_seconds = base_window_seconds

            next_run_utc = window_end_utc + timedelta(seconds=interval_seconds)

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
