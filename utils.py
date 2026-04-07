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
    """Публичная ссылка на пост: t.me/username/id или t.me/c/... для каналов без username."""
    chat = msg.chat
    if not chat:
        return None
    un = getattr(chat, "username", None)
    if un:
        return f"https://t.me/{un}/{msg.id}"
    cid = msg.chat_id
    if cid is not None:
        s = str(cid)
        if s.startswith("-100"):
            internal = s[4:]
            return f"https://t.me/c/{internal}/{msg.id}"
    return None


def channel_username(chat) -> str | None:
    """Публичный @username канала/чата без символа @; для приватных каналов — None."""
    un = getattr(chat, "username", None)
    if un and isinstance(un, str):
        return un.strip().lstrip("@") or None
    return None
