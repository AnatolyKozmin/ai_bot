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
  python test_send_db_to_llm.py --json-log out.jsonl   # только JSON-строки в JSONL-файл
  python test_send_db_to_llm.py --tee run.log          # копия ВСЕГО вывода в файл (+ терминал)
  python test_send_db_to_llm.py --errors-to-stdout     # ошибки и в stderr, и в stdout (удобно в IDE)
  python test_send_db_to_llm.py --text-limit 0        # полный text в JSON (до 500k)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from typing import Any, TextIO

import aiohttp
from sqlalchemy.orm import Session, sessionmaker

from api_client import llm_api_timeout_seconds, llm_system_prompt_payload
from config import load_settings
from database import make_session_factory
from models import Job

# Дублирование вывода: терминал (stdout/stderr) + опционально файл (--tee)
_tee_fp: TextIO | None = None
_errors_also_stdout: bool = False


def _out(*args: Any, **kwargs: Any) -> None:
    kwargs.setdefault("flush", True)
    print(*args, **kwargs)
    if _tee_fp is not None:
        rest = {k: v for k, v in kwargs.items() if k not in ("file", "flush")}
        print(*args, file=_tee_fp, flush=kwargs.get("flush", True), **rest)


def _err(*args: Any, **kwargs: Any) -> None:
    kwargs.setdefault("flush", True)
    print(*args, file=sys.stderr, **kwargs)
    fl = kwargs.get("flush", True)
    rest = {k: v for k, v in kwargs.items() if k not in ("file", "flush")}
    if _errors_also_stdout:
        print("! ", end="", file=sys.stdout, flush=True)
        print(*args, file=sys.stdout, flush=fl, **rest)
    if _tee_fp is not None:
        print(*args, file=_tee_fp, flush=fl, **rest)


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


def _clip_text(text: str, limit: int | None) -> tuple[str, bool]:
    """Вернуть (фрагмент, обрезан ли). limit None или 0 = без лимита (макс. 500k)."""
    if not text:
        return "", False
    max_len = 500_000
    if limit is not None and limit > 0:
        max_len = min(max_len, limit)
    if len(text) <= max_len:
        return text, False
    return text[:max_len], True


def _emit_jsonl(
    record: dict[str, Any],
    log_fp: TextIO | None,
) -> None:
    line = json.dumps(record, ensure_ascii=False)
    _out(line)
    if log_fp is not None:
        log_fp.write(line + "\n")
        log_fp.flush()


def _debug_record(
    *,
    index: int,
    total: int,
    job: Job,
    outcome: PostOutcome,
    text_for_json: str,
    text_was_truncated: bool,
    http_status: int,
    api_data: dict[str, Any] | None,
    raw_response: str | None,
) -> dict[str, Any]:
    """
    принято = HTTP 2xx + ответ распарсен в JSON + success=true (как в теле API).
    """
    принято = (
        outcome.http_ok
        and outcome.err is None
        and outcome.accepted_by_api
    )
    rec: dict[str, Any] = {
        "i": index,
        "n": total,
        "job_id": job.id,
        "message_id": job.message_id,
        "chat_id": job.chat_id,
        "chat_title": job.chat_title,
        "channel_username": getattr(job, "channel_username", None),
        "url": job.url,
        "text": text_for_json,
        "text_full_len": len(job.text or ""),
        "text_truncated": text_was_truncated,
        "принято": принято,
        "статус": "принято" if принято else "не принято",
        "http_status": http_status,
        "api_success": outcome.accepted_by_api,
        "вакансия_parsed_непустой": outcome.looks_like_vacancy,
        "sheets_appended": outcome.sheets_appended,
        "error": outcome.err,
    }
    if api_data is not None:
        lo = api_data.get("llm_output") or ""
        rec["llm_output_preview"] = lo[:800] if isinstance(lo, str) else str(lo)[:800]
        rec["parsed"] = api_data.get("parsed")
        if api_data.get("sheets_error"):
            rec["sheets_error"] = api_data.get("sheets_error")
    if raw_response is not None and not api_data:
        rec["raw_response_preview"] = raw_response.replace("\n", " ")[:500]
    return rec


async def _post_one(
    session: aiohttp.ClientSession,
    api_url: str,
    timeout: aiohttp.ClientTimeout,
    headers: dict[str, str],
    job: Job,
    verbose_json: bool,
    index: int,
    total: int,
    text_limit: int | None,
    log_fp: TextIO | None,
) -> PostOutcome:
    chat = job.chat_title or job.chat_id
    prefix = f"[{index}/{total}] job#{job.id} ({chat})"
    text_json, text_trunc = _clip_text(job.text or "", text_limit)

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
                _err(f"{prefix} | НЕ ПРИНЯТО | HTTP {code} | {tail}")
                out = PostOutcome(
                    http_ok=False,
                    status_code=code,
                    accepted_by_api=False,
                    looks_like_vacancy=False,
                    sheets_appended=False,
                    err=f"HTTP {code}",
                )
                _emit_jsonl(
                    _debug_record(
                        index=index,
                        total=total,
                        job=job,
                        outcome=out,
                        text_for_json=text_json,
                        text_was_truncated=text_trunc,
                        http_status=code,
                        api_data=None,
                        raw_response=raw,
                    ),
                    log_fp,
                )
                return out

            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                _err(f"{prefix} | НЕ ПРИНЯТО | ответ не JSON | {raw[:200]!r}")
                out = PostOutcome(
                    http_ok=True,
                    status_code=code,
                    accepted_by_api=False,
                    looks_like_vacancy=False,
                    sheets_appended=False,
                    err="invalid JSON",
                )
                _emit_jsonl(
                    _debug_record(
                        index=index,
                        total=total,
                        job=job,
                        outcome=out,
                        text_for_json=text_json,
                        text_was_truncated=text_trunc,
                        http_status=code,
                        api_data=None,
                        raw_response=raw,
                    ),
                    log_fp,
                )
                return out

            accepted = bool(data.get("success"))
            parsed = data.get("parsed")
            vac = _vacancy_from_parsed(parsed)
            sheets = bool(data.get("sheets_appended"))

            vac_txt = "да" if vac else "нет (пустой JSON / не вакансия)"
            sheet_txt = "да" if sheets else "нет"
            status_txt = "ПРИНЯТО API" if accepted else "ОШИБКА В ОТВЕТЕ (success=false)"

            _out(
                f"{prefix} | {status_txt} | вакансия по parsed: {vac_txt} | "
                f"строка в Sheets: {sheet_txt}"
            )

            if verbose_json:
                llm = (data.get("llm_output") or "")[:600]
                _out(f"    llm_output (фрагмент): {llm!r}")
                if isinstance(parsed, dict):
                    _out(f"    parsed keys: {list(parsed.keys())}")
                err = data.get("sheets_error")
                if err:
                    _out(f"    sheets_error: {err!r}")
                _out(json.dumps(data, ensure_ascii=False, indent=2)[:8000])

            out = PostOutcome(
                http_ok=True,
                status_code=code,
                accepted_by_api=accepted,
                looks_like_vacancy=vac,
                sheets_appended=sheets,
            )
            _emit_jsonl(
                _debug_record(
                    index=index,
                    total=total,
                    job=job,
                    outcome=out,
                    text_for_json=text_json,
                    text_was_truncated=text_trunc,
                    http_status=code,
                    api_data=data,
                    raw_response=None,
                ),
                log_fp,
            )
            return out

    except Exception as e:
        _err(f"{prefix} | НЕ ПРИНЯТО | исключение: {e}")
        out = PostOutcome(
            http_ok=False,
            status_code=0,
            accepted_by_api=False,
            looks_like_vacancy=False,
            sheets_appended=False,
            err=str(e),
        )
        _emit_jsonl(
            _debug_record(
                index=index,
                total=total,
                job=job,
                outcome=out,
                text_for_json=text_json,
                text_was_truncated=text_trunc,
                http_status=0,
                api_data=None,
                raw_response=None,
            ),
            log_fp,
        )
        return out


async def _run(args: argparse.Namespace) -> int:
    global _tee_fp, _errors_also_stdout
    _errors_also_stdout = bool(args.errors_to_stdout)

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

    tee_path = (args.tee or (os.getenv("TEST_SEND_TEE") or "").strip() or None)
    if tee_path:
        _tee_fp = open(tee_path, "a", encoding="utf-8")

    timeout = aiohttp.ClientTimeout(total=llm_api_timeout_seconds())
    headers = _headers()
    _out(f"API: {api_url}")
    _out(
        f"Постов: {len(jobs)}, таймаут HTTP: {timeout.total:.0f}s, последовательно\n"
        "Человекочитаемо: [i/N] job#… | ПРИНЯТО API | вакансия | Sheets\n"
        "Сразу под ним одна строка JSON («принято», «статус», «text», …).\n"
        "Всё это идёт в stdout терминала"
        + ("; копия в " + tee_path if tee_path else "")
        + ("; ошибки дублируются в stdout" if _errors_also_stdout else "")
        + ".\n"
    )

    log_fp: TextIO | None = None
    if args.json_log:
        log_fp = open(args.json_log, "a", encoding="utf-8")
    try:
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
                    args.text_limit,
                    log_fp,
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
    finally:
        if log_fp is not None:
            log_fp.close()
        if _tee_fp is not None:
            _tee_fp.close()
            _tee_fp = None

    _out(
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
    p.add_argument(
        "--json-log",
        metavar="FILE",
        help="Дополнительно дублировать JSON-строки в файл (append, JSONL)",
    )
    p.add_argument(
        "--text-limit",
        type=int,
        default=12_000,
        metavar="N",
        help="Макс. длина поля text в JSON (0 = до 500k символов). По умолчанию 12000",
    )
    p.add_argument(
        "--tee",
        metavar="FILE",
        help="Дублировать весь вывод (человекочитаемый + JSON) в файл, параллельно терминалу",
    )
    p.add_argument(
        "--errors-to-stdout",
        action="store_true",
        help="Сообщения об ошибках показывать и в stderr, и в stdout (если IDE скрывает stderr)",
    )
    args = p.parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
