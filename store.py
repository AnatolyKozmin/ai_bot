from datetime import datetime, timezone

from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session, sessionmaker

from models import Job


class JobStore:
    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def insert_if_new(
        self,
        chat_id: int,
        chat_title: str | None,
        message_id: int,
        sender_id: int | None,
        date_utc: str | None,
        text: str,
        url: str | None,
    ) -> bool:
        inserted_at = datetime.now(timezone.utc).isoformat()
        stmt = (
            insert(Job)
            .values(
                chat_id=chat_id,
                chat_title=chat_title,
                message_id=message_id,
                sender_id=sender_id,
                date_utc=date_utc,
                text=text,
                url=url,
                inserted_at_utc=inserted_at,
            )
            .on_conflict_do_nothing(index_elements=["chat_id", "message_id"])
        )
        with self._session_factory() as session:
            result = session.execute(stmt)
            session.commit()
            return result.rowcount > 0
