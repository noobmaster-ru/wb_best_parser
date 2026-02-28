from __future__ import annotations

# AI and runtime behavior
OPENAI_MODEL = "gpt-5.2"
REWRITE_WITH_AI = True
DRY_RUN = False

# Dedup settings
DEDUP_MEDIA = True
DEDUP_MAX_ITEMS = 10000
DEDUP_STORE_FILE = "sessions/dedup_hashes.txt"

# Filtering settings
INCLUDE_KEYWORDS = "ВБ,МП,распродажа,скидка,кэшбек,️КЭШБЕК"
EXCLUDE_KEYWORDS = (
    "подготовка,подработка,опт,опт-дистрибуция,вакансия,исчерпали,НОСКИ,носки,"
    "Оплата,оплата,Нужен,требуется,помощник,Уборка,Склад,склад,Разбор,разбор,"
    "человека,Ищу,расчёт,Халтура,поддонов,Разбор"
)
MIN_SCORE = 5

# Top mode schedule
PUBLISH_TOP_N = 1
TOP_WINDOW_MINUTES = 60
BACKFILL_HOURS = 1
BACKFILL_LIMIT_PER_CHAT = 20


def parse_csv(value: str) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


INCLUDE_KEYWORDS_LIST = parse_csv(INCLUDE_KEYWORDS)
EXCLUDE_KEYWORDS_LIST = parse_csv(EXCLUDE_KEYWORDS)
