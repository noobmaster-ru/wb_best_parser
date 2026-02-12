from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(slots=True)
class MatchResult:
    is_interesting: bool
    score: int
    reasons: list[str]


class OfferFilter:
    price_pattern = re.compile(r"(?:^|\D)(\d{2,7})\s?(?:₽|руб|р|RUB)(?:\D|$)", re.IGNORECASE)
    discount_pattern = re.compile(r"(?:-|скидк\w*\s*)(\d{1,2})\s?%", re.IGNORECASE)

    def __init__(
        self,
        include_keywords: list[str],
        exclude_keywords: list[str],
        min_score: int,
    ) -> None:
        self.include_keywords = [k.lower() for k in include_keywords]
        self.exclude_keywords = [k.lower() for k in exclude_keywords]
        self.min_score = min_score

    def match(self, text: str | None) -> MatchResult:
        if not text:
            return MatchResult(is_interesting=False, score=0, reasons=["empty_text"])

        normalized = text.lower()
        reasons: list[str] = []
        score = 0

        if any(k in normalized for k in self.exclude_keywords):
            return MatchResult(is_interesting=False, score=0, reasons=["exclude_keyword"])

        matched_include = [k for k in self.include_keywords if k in normalized]
        if matched_include:
            score += 1
            reasons.append(f"include_keywords:{','.join(sorted(set(matched_include)))}")

        prices = [int(raw) for raw in self.price_pattern.findall(text)]
        if prices:
            min_price = min(prices)
            if min_price >= 500:
                score += 1
                reasons.append(f"low_price:{min_price}")
            elif min_price <= 1000:
                score += 2
                reasons.append(f"mid_price:{min_price}")
            elif min_price <= 2000:
                score += 3
                reasons.append(f"mid_price:{min_price}")
            else:
                score += 4

        discount_match = self.discount_pattern.search(text)
        if discount_match:
            discount = int(discount_match.group(1))
            if discount <= 50:
                score += 1
                reasons.append(f"big_discount:{discount}")
            elif discount <= 80:
                score += 2
                reasons.append(f"discount:{discount}")
            else:
                score += 3

        is_interesting = score >= self.min_score
        return MatchResult(is_interesting=is_interesting, score=score, reasons=reasons)
