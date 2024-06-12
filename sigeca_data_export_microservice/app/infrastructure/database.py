from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import urllib.parse

Base = declarative_base()


def get_engine(config: dict):
    url = (
        f"postgresql+psycopg2://"
        + urllib.parse.quote_plus(config["username"])
        + ":"
        + urllib.parse.quote_plus(config["password"])
        + f"@{config['host']}:{config.get('port', 5432)}/{config['database']}"
    )

    return create_engine(url)


def get_session(engine):
    return sessionmaker(bind=engine)
