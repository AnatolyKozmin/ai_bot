import asyncio
import os
from typing import Any, Optional

import aiohttp
from sqlalchemy.orm import Session, sessionmaker

from api_client import llm_api_timeout_seconds, llm_system_prompt_payload
from config import load_settings
from database import make_session_factory
from models import Job


def _request_headers() -> dict[str, str]:
    secret = (os.getenv("INGEST_SECRET") or "").strip()
    if secret:
        return {"X-Ingest-Secret": secret}
    return {}


async def send_job_to_api(
    session: aiohttp.ClientSession,
    job: Job,
    api_url: str,
    timeout: aiohttp.ClientTimeout,
    headers: dict[str, str],
) -> bool:
    """Отправить один пост на API LLM."""
    payload: dict[str, Any] = {
        "id": job.id,
        "chat_id": job.chat_id,
        "chat_title": job.chat_title,
        "channel_username": getattr(job, "channel_username", None),
        "message_id": job.message_id,
        "sender_id": job.sender_id,
        "date_utc": job.date_utc,
        "text": job.text,
        "url": job.url,
        "inserted_at_utc": job.inserted_at_utc,
        **llm_system_prompt_payload(),
    }

    try:
        async with session.post(
            api_url, json=payload, timeout=timeout, headers=headers
        ) as resp:
            if resp.status in (200, 201):
                print(f"✓ Отправлен пост #{job.id} (чат: {job.chat_title or job.chat_id})")
                return True
            else:
                text = await resp.text()
                print(f"✗ Ошибка {resp.status} для поста #{job.id}: {text[:100]}")
                return False
    except Exception as e:
        print(f"✗ Ошибка при отправке поста #{job.id}: {e}")
        return False


def get_pending_jobs(session_factory: sessionmaker[Session], limit: Optional[int] = None) -> list[Job]:
    """Получить посты из базы данных."""
    with session_factory() as session:
        query = session.query(Job).order_by(Job.id.desc())
        if limit:
            query = query.limit(limit)
        return query.all()


async def main():
    # Загрузить настройки
    settings = load_settings()
    api_url = os.getenv("LLM_API_URL")
    batch_size = int(os.getenv("BATCH_SIZE", "10"))
    
    if not api_url:
        print("❌ Укажите LLM_API_URL в переменных окружения")
        return
    
    print(f"🚀 Запуск отправки постов на {api_url}")
    print(f"📦 Размер батча: {batch_size}")
    
    # Создать фабрику сессий
    session_factory = make_session_factory(settings.db_path)
    
    # Получить посты
    jobs = get_pending_jobs(session_factory, limit=None)
    
    if not jobs:
        print("✓ Нет постов для отправки")
        return
    
    print(f"📨 Найдено {len(jobs)} постов")
    
    # Отправить батчами
    successful = 0
    failed = 0
    
    timeout_sec = llm_api_timeout_seconds()
    req_timeout = aiohttp.ClientTimeout(total=timeout_sec)
    headers = _request_headers()
    print(f"⏱ HTTP таймаут к API: {timeout_sec:.0f}s (LLM_API_TIMEOUT)")

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(jobs), batch_size):
            batch = jobs[i:i + batch_size]
            print(f"\n📤 Отправка батча {i // batch_size + 1}/{(len(jobs) + batch_size - 1) // batch_size}")

            tasks = [
                send_job_to_api(session, job, api_url, req_timeout, headers)
                for job in batch
            ]
            results = await asyncio.gather(*tasks)
            
            successful += sum(results)
            failed += len(results) - sum(results)
            
            # Пауза между батчами
            if i + batch_size < len(jobs):
                await asyncio.sleep(1)
    
    print(f"\n✅ Статистика:")
    print(f"   Успешно: {successful}")
    print(f"   Ошибок: {failed}")
    print(f"   Всего: {successful + failed}")


if __name__ == "__main__":
    asyncio.run(main())
