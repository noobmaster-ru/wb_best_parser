from __future__ import annotations

import errno
import time
from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    tg_api_id: int = Field(alias="TG_API_ID")
    tg_api_hash: str = Field(alias="TG_API_HASH")
    tg_session: str = Field(default="sessions/user", alias="TG_SESSION")
    targets_file: str = Field(default="targets.txt", alias="TARGETS_FILE")

    source_chats: str = Field(default="", alias="SOURCE_CHATS")
    target_chat: str = Field(alias="TARGET_CHAT")

    include_keywords: str = Field(default="", alias="INCLUDE_KEYWORDS")
    exclude_keywords: str = Field(default="", alias="EXCLUDE_KEYWORDS")

    min_score: int = Field(default=2, alias="MIN_SCORE")
    dry_run: bool = Field(default=False, alias="DRY_RUN")
    dedup_store_file: str = Field(default="sessions/dedup_hashes.txt", alias="DEDUP_STORE_FILE")
    dedup_max_items: int = Field(default=5000, alias="DEDUP_MAX_ITEMS")
    backfill_hours: int = Field(default=1, alias="BACKFILL_HOURS")
    backfill_limit_per_chat: int = Field(default=200, alias="BACKFILL_LIMIT_PER_CHAT")
    rewrite_with_ai: bool = Field(default=False, alias="REWRITE_WITH_AI")
    openai_api_key: str = Field(default="", alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4.1-mini", alias="OPENAI_MODEL")
    openai_proxy: str = Field(default="", alias="OPENAI_PROXY")

    @field_validator("target_chat", mode="before")
    @classmethod
    def normalize_target_chat(cls, value: str) -> str:
        return value.strip() if isinstance(value, str) else value

    @staticmethod
    def parse_csv(value: str) -> list[str]:
        if not value:
            return []
        return [item.strip() for item in value.split(",") if item.strip()]

    def source_chats_list(self) -> list[str]:
        return self.parse_csv(self.source_chats)

    def include_keywords_list(self) -> list[str]:
        return self.parse_csv(self.include_keywords)

    def exclude_keywords_list(self) -> list[str]:
        return self.parse_csv(self.exclude_keywords)

    def load_source_chats_from_file(self) -> list[str]:
        path = Path(self.targets_file)
        if not path.exists():
            return []

        lines: list[str] = []
        for attempt in range(3):
            try:
                with path.open("r", encoding="utf-8") as f:
                    lines = f.readlines()
                break
            except OSError as exc:
                if exc.errno == errno.EDEADLK and attempt < 2:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                raise

        chats: list[str] = []
        for line in lines:
            value = line.strip()
            if not value or value.startswith("#"):
                continue
            chats.append(value)
        return chats

    @model_validator(mode="after")
    def validate_required(self) -> Settings:
        if not self.target_chat:
            raise ValueError("TARGET_CHAT must be set")
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
