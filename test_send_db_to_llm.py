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
import sys
from dataclasses import dataclass
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


@dataclass
class PostOutcome:
    http_ok: bool
    status_code: int
    accepted_by_api: bool  # success: true в JSON
    looks_like_vacancy: bool  # непустой parsed (как в API перед записью в sheets)
    sheets_appended: bool
    err: str | None = None


def _vacancy_from_parsed(parsed: Any) -> bool:
    return isinstance(parsed, dict) and bool(parsed)


async def _post_one(
    session: aiohttp.ClientSession,
    api_url: str,
    timeout: aiohttp.ClientTimeout,
    headers: dict[str, str],
    job: Job,
    verbose_json: bool,
    index: int,
    total: int,
) -> PostOutcome:
    chat = job.chat_title or job.chat_id
    prefix = f"[{index}/{total}] job#{job.id} ({chat})"

    payload = _payload(job)
    try:
        async with session.post(
            api_url, json=payload, timeout=timeout, headers=headers
        ) as resp:
            raw = await resp.text()
            code = resp.status
            http_ok = code in (200, 201)

            if not http_ok:
                tail = raw.replace("\n", " ")[:200]
                print(
                    f"{prefix} | НЕ ПРИНЯТО | HTTP {code} | {tail}",
                    file=sys.stderr,
                )
                return PostOutcome(
                    http_ok=False,
                    status_code=code,
                    accepted_by_api=False,
                    looks_like_vacancy=False,
                    sheets_appended=False,
                    err=f"HTTP {code}",
                )

            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                print(
                    f"{prefix} | НЕ ПРИНЯТО | ответ не JSON | {raw[:200]!r}",
                    file=sys.stderr,
                )
                return PostOutcome(
                    http_ok=True,
                    status_code=code,
                    accepted_by_api=False,
                    looks_like_vacancy=False,
                    sheets_appended=False,
                    err="invalid JSON",
                )

            accepted = bool(data.get("success"))
            parsed = data.get("parsed")
            vac = _vacancy_from_parsed(parsed)
            sheets = bool(data.get("sheets_appended"))

            vac_txt = "да" if vac else "нет (пустой JSON / не вакансия)"
            sheet_txt = "да" if sheets else "нет"
            status_txt = "ПРИНЯТО API" if accepted else "ОШИБКА В ОТВЕТЕ (success=false)"

            print(
                f"{prefix} | {status_txt} | вакансия по parsed: {vac_txt} | "
                f"строка в Sheets: {sheet_txt}"
            )

            if verbose_json:
                llm = (data.get("llm_output") or "")[:600]
                print(f"    llm_output (фрагмент): {llm!r}")
                if isinstance(parsed, dict):
                    print(f"    parsed keys: {list(parsed.keys())}")
                err = data.get("sheets_error")
                if err:
                    print(f"    sheets_error: {err!r}")
                print(json.dumps(data, ensure_ascii=False, indent=2)[:8000])

            return PostOutcome(
                http_ok=True,
                status_code=code,
                accepted_by_api=accepted,
                looks_like_vacancy=vac,
                sheets_appended=sheets,
            )

    except Exception as e:
        print(f"{prefix} | НЕ ПРИНЯТО | исключение: {e}", file=sys.stderr)
        return PostOutcome(
            http_ok=False,
            status_code=0,
            accepted_by_api=False,
            looks_like_vacancy=False,
            sheets_appended=False,
            err=str(e),
        )


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
        limit=args.limit,
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
    print(
        f"Постов: {len(jobs)}, таймаут HTTP: {timeout.total:.0f}s, последовательно\n"
        "Формат строки: [i/N] job#id (чат) | ПРИНЯТО API | вакансия по parsed: да/нет | "
        "строка в Sheets: да/нет\n"
    )

    totals = {
        "http_ok": 0,
        "api_success": 0,
        "vacancy": 0,
        "sheets": 0,
        "failed": 0,
    }
    async with aiohttp.ClientSession() as session:
        for i, job in enumerate(jobs, start=1):
            out = await _post_one(
                session,
                api_url,
                timeout,
                headers,
                job,
                args.verbose,
                i,
                len(jobs),
            )
            if out.http_ok:
                totals["http_ok"] += 1
            if not out.http_ok or out.err:
                totals["failed"] += 1
            if out.accepted_by_api:
                totals["api_success"] += 1
            if out.looks_like_vacancy:
                totals["vacancy"] += 1
            if out.sheets_appended:
                totals["sheets"] += 1

    print(
        "\n=== сводка ===\n"
        f"  HTTP 2xx:        {totals['http_ok']}/{len(jobs)}\n"
        f"  success в JSON:  {totals['api_success']}/{len(jobs)}\n"
        f"  вакансия (parsed не пустой): {totals['vacancy']}\n"
        f"  записано в Sheets: {totals['sheets']}\n"
        f"  ошибок (HTTP / сеть / не JSON): {totals['failed']}"
    )
    return 0 if totals["failed"] == 0 and totals["http_ok"] == len(jobs) else 2


def main() -> None:
    p = argparse.ArgumentParser(description="Тест: посты из БД → ai_api → консоль")
    p.add_argument(
        "--all",
        action="store_true",
        help="Взять все записи jobs (иначе только sent=False)",
    )
    p.add_argument(
        "-n",
        "--limit",
        type=int,
        default=None,
        metavar="N",
        dest="limit",
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
