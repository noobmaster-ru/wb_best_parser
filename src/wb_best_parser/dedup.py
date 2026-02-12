from __future__ import annotations

import hashlib
import re
from collections import deque
from pathlib import Path


class DedupStore:
    def __init__(self, path: str, max_items: int = 5000) -> None:
        self.path = Path(path)
        self.max_items = max_items
        self._items: deque[str] = deque()
        self._set: set[str] = set()
        self._dirty = 0
        self._load()

    @staticmethod
    def fingerprint(text: str) -> str | None:
        normalized = re.sub(r"\s+", " ", (text or "")).strip().lower()
        if not normalized:
            return None
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    def _load(self) -> None:
        if not self.path.exists():
            return
        lines = [line.strip() for line in self.path.read_text(encoding="utf-8").splitlines()]
        for value in lines[-self.max_items :]:
            if not value:
                continue
            self._items.append(value)
            self._set.add(value)

    def contains(self, key: str) -> bool:
        return key in self._set

    def add(self, key: str) -> None:
        if key in self._set:
            return

        self._items.append(key)
        self._set.add(key)

        while len(self._items) > self.max_items:
            removed = self._items.popleft()
            self._set.discard(removed)

        self._dirty += 1
        if self._dirty >= 25:
            self.flush()

    def remove(self, key: str) -> None:
        if key not in self._set:
            return
        self._set.discard(key)
        self._items = deque(item for item in self._items if item != key)
        self._dirty += 1

    def flush(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        content = "\n".join(self._items)
        self.path.write_text(f"{content}\n" if content else "", encoding="utf-8")
        self._dirty = 0
