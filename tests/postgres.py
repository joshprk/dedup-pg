from collections import defaultdict

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import DeclarativeBase

from dedup_pg import DedupIndex
from dedup_pg.backend.sqlalchemy import SQLAlchemyBackend
from dedup_pg.helpers import n_grams

from .utils.postgres import postgres_server


class Base(DeclarativeBase):
    pass


def func(engine: Engine):
    corpus = [
        ("key1", "The quick brown fox jumps over the lazy dog"),
        ("key2", "T e qui k bnown fox jump  over t e  azy  og"),
        ("key3", "An entirely different sentence!"),
    ]

    lsh = DedupIndex(
        SQLAlchemyBackend(
            engine=engine,
            base_or_metadata=Base,
            table_name="lsh_index",
        )
    )

    Base.metadata.create_all(engine)

    n_gram_corpus = [(key, n_grams(text, n=3)) for key, text in corpus]

    duplicate_map = defaultdict(list)
    for key, n_gram in n_gram_corpus:
        bands = lsh.bands(n_gram)
        lsh_items = lsh.items(bands)
        cluster_key = lsh.index(lsh_items)

        duplicate_map[cluster_key].append(key)

    return duplicate_map


def test_postgres(postgres_server: dict[str, str]):
    database_url = (
        f"postgresql+psycopg2://{postgres_server['user']}:"
        f"{postgres_server['password']}@{postgres_server['host']}:"
        f"{postgres_server['port']}/{postgres_server['database']}"
    )

    engine = create_engine(database_url)
    result = list(func(engine).values())

    assert "key1" in result[0] and "key2" in result[0]
    assert "key3" in result[1]
