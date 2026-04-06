from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from models import Base


def make_session_factory(db_path: str) -> sessionmaker[Session]:
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    Base.metadata.create_all(engine)
    return sessionmaker(engine, expire_on_commit=False)
