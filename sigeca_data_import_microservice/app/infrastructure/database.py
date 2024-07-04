from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import urllib.parse

from app.config import DatabaseConfig

Base = declarative_base()


def get_engine(config: DatabaseConfig):
    url = (
        f"postgresql+psycopg2://"
        + urllib.parse.quote_plus(config.username)
        + ":"
        + urllib.parse.quote_plus(config.password)
        + f"@{config.host}:{config.port}/{config.database}"
    )

    return create_engine(url)


def get_session(engine):
    return sessionmaker(bind=engine)
