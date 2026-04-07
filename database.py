from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from models import Base


def _ensure_sqlite_jobs_columns(engine) -> None:
    """Добавить колонки в существующую SQLite-таблицу jobs (create_all их не обновляет)."""
    with engine.begin() as conn:
        rows = conn.execute(text("PRAGMA table_info(jobs)")).fetchall()
        if not rows:
            return
        existing = {r[1] for r in rows}
        if "channel_username" not in existing:
            conn.execute(text("ALTER TABLE jobs ADD COLUMN channel_username TEXT"))


def make_session_factory(db_path: str) -> sessionmaker[Session]:
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    Base.metadata.create_all(engine)
    _ensure_sqlite_jobs_columns(engine)
    return sessionmaker(engine, expire_on_commit=False)
