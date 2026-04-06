"""
Клиент для отправки постов на LLM API.
"""
import asyncio
import os
from typing import Optional

import aiohttp


class LLMAPIClient:
    def __init__(self, api_url: Optional[str] = None):
        self.api_url = api_url or os.getenv("LLM_API_URL")
        self.enabled = bool(self.api_url)
    
    async def send_job(self, job_data: dict) -> bool:
        """
        Отправить пост на API.
        
        Args:
            job_data: Словарь с данными поста
            
        Returns:
            True если отправка успешна, False иначе
        """
        if not self.enabled:
            return False
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.api_url,
                    json=job_data,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    success = resp.status in (200, 201)
                    if not success:
                        text = await resp.text()
                        print(f"[API] Ошибка {resp.status}: {text[:100]}")
                    return success
        except asyncio.TimeoutError:
            print(f"[API] Таймаут при отправке поста #{job_data.get('id')}")
            return False
        except Exception as e:
            print(f"[API] Ошибка при отправке: {e}")
            return False


# Глобальный инстанс клиента
_client: Optional[LLMAPIClient] = None


def get_llm_client() -> LLMAPIClient:
    """Получить или создать глобальный клиент."""
    global _client
    if _client is None:
        _client = LLMAPIClient()
    return _client
