import re
import asyncio

from telethon import TelegramClient, events
from telethon.tl.custom.message import Message

from config import Settings, load_settings
from database import make_session_factory
from store import JobStore
from api_client import get_llm_client
from utils import (
    build_keywords_regex,
    message_text,
    message_url,
    to_utc_iso,
)


async def send_job_to_api(job, llm_client, store: JobStore) -> None:
    """Send a job to API and mark as sent if successful."""
    if not llm_client.enabled:
        return
    
    payload = {
        "id": job.id,
        "chat_id": job.chat_id,
        "chat_title": job.chat_title,
        "message_id": job.message_id,
        "sender_id": job.sender_id,
        "date_utc": job.date_utc,
        "text": job.text,
        "url": job.url,
        "inserted_at_utc": job.inserted_at_utc,
    }
    
    if await llm_client.send_job(payload):
        store.mark_as_sent(job.id)
        print(f"[API] ✓ Отправлен пост #{job.id}")
    else:
        print(f"[API] ✗ Не удалось отправить пост #{job.id}, повторим позже")


async def process_message(
    msg: Message,
    regex: re.Pattern[str],
    store: JobStore,
    save_all: bool,
    llm_client,
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
    job = store.insert_if_new(
        chat_id=chat_id,
        chat_title=chat_title,
        message_id=msg.id,
        sender_id=sender_id,
        date_utc=to_utc_iso(msg.date),
        text=text,
        url=message_url(msg),
    )
    if job:
        print(
            f"[saved] chat={chat_title or chat_id} msg={msg.id} "
            f"text={text[:80].replace(chr(10), ' ')}"
        )
        # Отправить на API асинхронно (не ждём результата)
        asyncio.create_task(send_job_to_api(job, llm_client, store))


async def run_with_settings(settings: Settings) -> None:
    regex = build_keywords_regex(settings.keywords)
    session_factory = make_session_factory(settings.db_path)
    store = JobStore(session_factory)
    llm_client = get_llm_client()

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
    if llm_client.enabled:
        print(f"LLM API: {llm_client.api_url}")
    else:
        print("LLM API: disabled (LLM_API_URL not set)")

    # Backfill: load recent messages from each channel
    for channel in settings.channels:
        async for msg in client.iter_messages(channel, limit=settings.backfill_limit):
            await process_message(msg, regex, store, settings.save_all, llm_client)

    # Retry unsent jobs every 5 minutes
    async def retry_unsent_jobs():
        while True:
            await asyncio.sleep(300)
            unsent = store.get_unsent_jobs(limit=50)
            if unsent:
                print(f"[retry] Попытка отправить {len(unsent)} не отправленных постов")
                for job in unsent:
                    await send_job_to_api(job, llm_client, store)

    # Start retry task
    asyncio.create_task(retry_unsent_jobs())

    # Listen for new messages
    @client.on(events.NewMessage(chats=settings.channels))
    async def on_new_message(event: events.NewMessage.Event) -> None:
        await process_message(event.message, regex, store, settings.save_all, llm_client)

    await client.run_until_disconnected()


async def run() -> None:
    settings = load_settings()
    await run_with_settings(settings)
