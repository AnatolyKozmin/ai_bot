import re

from telethon import TelegramClient, events
from telethon.tl.custom.message import Message

from config import Settings, load_settings
from database import make_session_factory
from store import JobStore
from utils import (
    build_keywords_regex,
    message_text,
    message_url,
    to_utc_iso,
)


async def process_message(
    msg: Message,
    regex: re.Pattern[str],
    store: JobStore,
    save_all: bool,
) -> None:
    text = message_text(msg)
    if not text:
        return

    if not save_all and not regex.search(text.lower()):
        return

    chat = await msg.get_chat()
    chat_id = msg.chat_id or 0
    chat_title = getattr(chat, "title", None) or getattr(chat, "username", None)
    sender_id = msg.sender_id
    was_new = store.insert_if_new(
        chat_id=chat_id,
        chat_title=chat_title,
        message_id=msg.id,
        sender_id=sender_id,
        date_utc=to_utc_iso(msg.date),
        text=text,
        url=message_url(msg),
    )
    if was_new:
        print(
            f"[saved] chat={chat_title or chat_id} msg={msg.id} "
            f"text={text[:80].replace(chr(10), ' ')}"
        )


async def run_with_settings(settings: Settings) -> None:
    regex = build_keywords_regex(settings.keywords)
    session_factory = make_session_factory(settings.db_path)
    store = JobStore(session_factory)

    client = TelegramClient(
        settings.session_name,
        settings.api_id,
        settings.api_hash,
    )
    await client.start(phone=settings.phone)

    print("Collector is running.")
    print(f"Channels: {', '.join(settings.channels)}")
    print(f"DB file: {settings.db_path}")
    print(f"SAVE_ALL: {settings.save_all}")

    for channel in settings.channels:
        async for msg in client.iter_messages(channel, limit=settings.backfill_limit):
            await process_message(msg, regex, store, settings.save_all)

    @client.on(events.NewMessage(chats=settings.channels))
    async def on_new_message(event: events.NewMessage.Event) -> None:
        await process_message(event.message, regex, store, settings.save_all)

    await client.run_until_disconnected()


async def run() -> None:
    settings = load_settings()
    await run_with_settings(settings)
