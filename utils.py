import re
from datetime import datetime, timezone
from typing import Iterable

from telethon.tl.custom.message import Message


def to_utc_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def build_keywords_regex(keywords: Iterable[str]) -> re.Pattern[str]:
    escaped = [re.escape(k) for k in keywords if k]
    if not escaped:
        return re.compile(r".+", re.IGNORECASE | re.DOTALL)
    pattern = r"\b(" + "|".join(escaped) + r")\b"
    return re.compile(pattern, re.IGNORECASE)


def message_text(msg: Message) -> str:
    return (msg.raw_text or "").strip()


def message_url(msg: Message) -> str | None:
    if msg.chat and getattr(msg.chat, "username", None):
        return f"https://t.me/{msg.chat.username}/{msg.id}"
    return None
