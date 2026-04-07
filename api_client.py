"""
Клиент для отправки постов на LLM API.

Таймаут HTTP должен быть не меньше, чем время ответа API (в т.ч. Ollama):
см. OLLAMA_REQUEST_TIMEOUT на стороне ai_api (по умолчанию 300 с).
"""
import asyncio
import os
from pathlib import Path
from typing import Any, Optional

import aiohttp


def llm_api_timeout_seconds() -> float:
    """HTTP timeout к ai_api (должен покрывать OLLAMA_REQUEST_TIMEOUT + запас)."""
    raw = os.getenv("LLM_API_TIMEOUT", "360").strip()
    try:
        return max(30.0, float(raw))
    except ValueError:
        return 360.0


def llm_system_prompt_payload() -> dict[str, str]:
    """
    Доп. поля для тела POST: свой system_prompt вместо дефолта на API.

    Приоритет: LLM_SYSTEM_PROMPT_FILE (если путь задан и файл есть), иначе LLM_SYSTEM_PROMPT из env.
    Если переменная LLM_SYSTEM_PROMPT задана (даже пустая) — уходит в API и отключает встроенный промпт.
    Если ни файл, ни переменная не заданы — ключ не добавляется, на API остаётся дефолтный SYSTEM_PROMPT.
    """
    fp = (os.getenv("LLM_SYSTEM_PROMPT_FILE") or "").strip()
    if fp:
        path = Path(fp)
        if path.is_file():
            return {"system_prompt": path.read_text(encoding="utf-8")}
        print(f"[LLM] LLM_SYSTEM_PROMPT_FILE не найден или не файл: {fp}")

    if "LLM_SYSTEM_PROMPT" in os.environ:
        return {"system_prompt": os.environ.get("LLM_SYSTEM_PROMPT", "")}

    return {}


class LLMAPIClient:
    def __init__(self, api_url: Optional[str] = None):
        self.api_url = api_url or os.getenv("LLM_API_URL")
        self.enabled = bool(self.api_url)
        self.timeout_s = llm_api_timeout_seconds()
        self._ingest_secret = (os.getenv("INGEST_SECRET") or "").strip()
        self._session: Optional[aiohttp.ClientSession] = None

    def _headers(self) -> dict[str, str]:
        h: dict[str, str] = {}
        if self._ingest_secret:
            h["X-Ingest-Secret"] = self._ingest_secret
        return h

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def send_job(self, job_data: dict[str, Any]) -> bool:
        """
        Отправить пост на API.

        Args:
            job_data: Словарь с данными поста

        Returns:
            True если отправка успешна, False иначе
        """
        if not self.enabled:
            return False

        timeout = aiohttp.ClientTimeout(total=self.timeout_s)
        try:
            session = await self._get_session()
            async with session.post(
                self.api_url,
                json=job_data,
                timeout=timeout,
                headers=self._headers(),
            ) as resp:
                success = resp.status in (200, 201)
                if not success:
                    text = await resp.text()
                    print(f"[API] Ошибка {resp.status}: {text[:200]}")
                return success
        except asyncio.TimeoutError:
            print(
                f"[API] Таймаут {self.timeout_s:.0f}s при отправке поста #{job_data.get('id')} "
                f"(увеличьте LLM_API_TIMEOUT на боте при необходимости)"
            )
            return False
        except Exception as e:
            print(f"[API] Ошибка при отправке: {e}")
            return False

    async def aclose(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None


# Глобальный инстанс клиента
_client: Optional[LLMAPIClient] = None


def get_llm_client() -> LLMAPIClient:
    """Получить или создать глобальный клиент."""
    global _client
    if _client is None:
        _client = LLMAPIClient()
    return _client
