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
    ) -> Job | None:
        """Insert a job if it doesn't exist. Returns the Job object or None if duplicate."""
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
                sent=False,
            )
            .on_conflict_do_nothing(index_elements=["chat_id", "message_id"])
        )
        with self._session_factory() as session:
            result = session.execute(stmt)
            session.commit()
            
            if result.rowcount > 0:
                # Получить только что добавленный объект
                job = session.query(Job).filter(
                    Job.chat_id == chat_id,
                    Job.message_id == message_id
                ).first()
                return job
            return None
    
    def mark_as_sent(self, job_id: int) -> bool:
        """Mark a job as sent."""
        with self._session_factory() as session:
            job = session.query(Job).filter(Job.id == job_id).first()
            if job:
                job.sent = True
                session.commit()
                return True
            return False
    
    def get_unsent_jobs(self, limit: int = 100) -> list[Job]:
        """Get jobs that haven't been sent yet."""
        with self._session_factory() as session:
            return session.query(Job).filter(Job.sent == False).limit(limit).all()
