from sqlalchemy import BigInteger, Integer, String, Text, UniqueConstraint, Boolean
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Job(Base):
    """Сохранённое сообщение (как в исходной схеме: даты — ISO-строки в TEXT)."""

    __tablename__ = "jobs"
    __table_args__ = (
        UniqueConstraint("chat_id", "message_id", name="uq_jobs_chat_message"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    chat_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    message_id: Mapped[int] = mapped_column(Integer, nullable=False)
    sender_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    date_utc: Mapped[str | None] = mapped_column(Text, nullable=True)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str | None] = mapped_column(Text, nullable=True)
    inserted_at_utc: Mapped[str] = mapped_column(Text, nullable=False)
    sent: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
