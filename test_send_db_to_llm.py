#!/usr/bin/env python3
"""
Отправка постов из SQLite в ai_api и вывод результата в консоль (для проверки пайплайна).

Использует тот же .env, что и бот: LLM_API_URL, LLM_API_TIMEOUT, INGEST_SECRET,
LLM_SYSTEM_PROMPT* (если заданы).

Примеры:
  python test_send_db_to_llm.py              # только sent=False, по 1 запросу
  python test_send_db_to_llm.py --all        # все строки в jobs
  python test_send_db_to_llm.py -n 5         # первые 5 подходящих
  python test_send_db_to_llm.py --dry-run    # только посчитать
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from typing import Any

import aiohttp
from sqlalchemy.orm import Session, sessionmaker

from api_client import llm_api_timeout_seconds, llm_system_prompt_payload
from config import load_settings
from database import make_session_factory
from models import Job


def _headers() -> dict[str, str]:
    secret = (os.getenv("INGEST_SECRET") or "").strip()
    if secret:
        return {"X-Ingest-Secret": secret}
    return {}


def _pick_jobs(
    session_factory: sessionmaker[Session],
    *,
    only_unsent: bool,
    limit: int | None,
) -> list[Job]:
    with session_factory() as session:
        q = session.query(Job).order_by(Job.id.asc())
        if only_unsent:
            q = q.filter(Job.sent.is_(False))
        if limit is not None:
            q = q.limit(limit)
        return list(q.all())


def _payload(job: Job) -> dict[str, Any]:
    return {
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


async def _post_one(
    session: aiohttp.ClientSession,
    api_url: str,
    timeout: aiohttp.ClientTimeout,
    headers: dict[str, str],
    job: Job,
    verbose_json: bool,
) -> bool:
    payload = _payload(job)
    try:
        async with session.post(
            api_url, json=payload, timeout=timeout, headers=headers
        ) as resp:
            raw = await resp.text()
            ok = resp.status in (200, 201)
            prefix = "OK " if ok else f"FAIL {resp.status} "
            print(f"\n--- #{job.id} {prefix}(чат: {job.chat_title or job.chat_id}) ---")
            if ok:
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    print(raw[:500])
                    return ok
                llm = (data.get("llm_output") or "")[:400]
                parsed = data.get("parsed")
                sheets = data.get("sheets_appended")
                print(f"llm_output (обрезано): {llm!r}")
                print(f"parsed keys: {list(parsed.keys()) if isinstance(parsed, dict) else parsed}")
                print(f"sheets_appended: {sheets}")
                if verbose_json:
                    print(json.dumps(data, ensure_ascii=False, indent=2)[:8000])
            else:
                print(raw[:500])
            return ok
    except Exception as e:
        print(f"\n--- #{job.id} ERROR: {e} ---")
        return False


async def _run(args: argparse.Namespace) -> int:
    settings = load_settings()
    api_url = (os.getenv("LLM_API_URL") or "").strip()
    if not api_url:
        print("Задайте LLM_API_URL в .env (полный URL …/parse_post)")
        return 1

    session_factory = make_session_factory(settings.db_path)
    jobs = _pick_jobs(
        session_factory,
        only_unsent=not args.all,
        limit=args.n,
    )

    if args.dry_run:
        mode = "все" if args.all else "только sent=False"
        print(f"Сухой прогон: {len(jobs)} постов ({mode}), API={api_url}")
        return 0

    if not jobs:
        print("Нет постов. Попробуйте --all или проверьте БД.")
        return 0

    timeout = aiohttp.ClientTimeout(total=llm_api_timeout_seconds())
    headers = _headers()
    print(f"API: {api_url}")
    print(f"Постов: {len(jobs)}, таймаут HTTP: {timeout.total:.0f}s, последовательно")

    ok_n = 0
    async with aiohttp.ClientSession() as session:
        for job in jobs:
            if await _post_one(
                session, api_url, timeout, headers, job, args.verbose
            ):
                ok_n += 1

    print(f"\nИтого: успех {ok_n}/{len(jobs)}")
    return 0 if ok_n == len(jobs) else 2


def main() -> None:
    p = argparse.ArgumentParser(description="Тест: посты из БД → ai_api → консоль")
    p.add_argument(
        "--all",
        action="store_true",
        help="Взять все записи jobs (иначе только sent=False)",
    )
    p.add_argument(
        "-n", "--limit", type=int, default=None, metavar="N",
        help="Максимум N записей",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Только показать, сколько записей уйдёт в API",
    )
    p.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Печатать больше JSON ответа API",
    )
    args = p.parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
