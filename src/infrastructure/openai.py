from __future__ import annotations

import json
import logging
from contextlib import suppress
from dataclasses import dataclass

from httpx import AsyncClient, AsyncHTTPTransport
from openai import AsyncOpenAI
from openai.types.responses import Response

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class OpenAIConfig:
    openai_api_key: str
    model: str 
    proxy: str 
    timeout_seconds: float = 30.0


class OpenAIGateway:
    def __init__(self, config: OpenAIConfig) -> None:
        self._client = AsyncOpenAI(
            api_key=config.openai_api_key,
            http_client=AsyncClient(
                proxy=config.proxy,
                transport=AsyncHTTPTransport(local_address="0.0.0.0"),
                timeout=config.timeout_seconds,
            ),
        )
        self._model = config.model


    @staticmethod
    def _extract_response_text(response: Response) -> str | None:
        """Извлекает текст ответа из response объекта OpenAI"""
        for item in response.output:
            if getattr(item, "content", None):
                for block in item.content:
                    text = getattr(block, "text", None)
                    if text:
                        return text.strip()
        return None

    async def rewrite_offer(self, original_text: str) -> str:
        prompt = f"""
            Перепиши текст объявления для Telegram в едином стиле. 
            Сохрани факты, цену, условия, контакты и эмодзи по смыслу. 
            Не добавляй вымышленные данные. Верни только итоговый текст поста без пояснений.
            
            СТРУКТУРА ОТВЕТА:
            - [название товара] (если есть)
            - Цена на МП: [цена без кэшбека] (если есть)
            - Цена с кэшбеком: [цена с кэшбеком] (если есть)
            - Кэшбек: [процент кэшбека]% (если есть)
            - [условия заказа + ссылка на аккаунт в телеграме] (если есть)
            
            ФОРМАТ ОТВЕТА:
            Если отсутствует какой-то из пунктов, то не включай его в ответ. Итоговый текст напиши творчески, со смайликами , с эмодзи различными.
            Текст не должен быть слишком скучный. Если кэшбек > 50%, то выдели его жирным и поставь 🔥
            Разделяй пункты: цена, кэшбек, ссылку на аккаунт для связи. 
            
            Исходный текст:
            {original_text}
        """
      
        try:
            response = await self._client.responses.create(
                model=self._model,
                input=[
                    {
                        "role": "system",
                        "content": [
                            {
                                "type": "input_text",
                                "text": "Ты редактор Telegram-канала с акцентом на короткий, чистый и продающий стиль.",
                            }
                        ],
                    },
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "input_text", 
                                "text": prompt
                            }
                        ],
                    },
                ]
            )
            rewritten = self._extract_response_text(response)
            if not rewritten:
                return original_text

            logger.info("OpenAI rewrite success")
            return rewritten
        except Exception as exc:
            logger.warning("OpenAI rewrite failed: %s", exc)
            with suppress(Exception):
                logger.debug("OpenAI error payload: %s", json.dumps({"error": str(exc)}))
            return original_text