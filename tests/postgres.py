import cProfile
import pstats
import timeit
import statistics
import string
import random

from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from dedup_pg.backend.sqlalchemy import SQLAlchemyBackend
from dedup_pg.helpers import n_grams
from dedup_pg.index import DedupIndex
from tests.readme import readme_func

from .utils.postgres import postgres_server


class Base(DeclarativeBase):
    pass


def _fmt_database_url(dsn: dict[str, str]) -> str:
    return (
        f"postgresql+psycopg2://{dsn['user']}:"
        f"{dsn['password']}@{dsn['host']}:"
        f"{dsn['port']}/{dsn['database']}"
    )


def _summarize(name, values):
    mean_ = statistics.mean(values)
    med_ = statistics.median(values)
    p95_ = statistics.quantiles(values, n=100)[94]
    p99_ = statistics.quantiles(values, n=100)[98]
    mx_ = max(values)
    total = sum(values)
    rate = len(values) / total

    print(f"\n{name}:")
    print(f"  mean:   {mean_:.6f} s")
    print(f"  median: {med_:.6f} s")
    print(f"  p95:    {p95_:.6f} s")
    print(f"  p99:    {p99_:.6f} s")
    print(f"  max:    {mx_:.6f} s")
    print(f"  ops/sec: {rate:.2f}")


def test_postgres(postgres_server: dict[str, str]) -> None:
    database_url = _fmt_database_url(postgres_server)
    engine = create_engine(database_url)
    backend = SQLAlchemyBackend(
        engine=engine,
        base_or_metadata=Base,
        table_name="lsh_index",
    )

    Base.metadata.create_all(engine)
    result = list(readme_func(backend).values())

    assert "key1" in result[0] and "key2" in result[0]
    assert "key3" in result[1]


def test_postgres_sequential_insert(postgres_server: dict[str, str]) -> None:
    database_url = _fmt_database_url(postgres_server)
    engine = create_engine(database_url)
    SessionLocal = sessionmaker(engine)

    index = DedupIndex(
        SQLAlchemyBackend(
            engine=engine,
            base_or_metadata=Base,
            table_name="seq_lsh_index",
        )
    )

    Base.metadata.create_all(engine)

    latencies = []

    # Refreshes planner
    with SessionLocal() as session:
        _ = session.execute(text("ANALYZE seq_lsh_index"))
        session.commit()

    for i in range(1000):
        rand_str = n_grams("".join([random.choice(string.ascii_letters) for _ in range(15)]))
        t = timeit.timeit(
            lambda s=rand_str: index.query(s),
            number=1,
        )

        latencies.append(t)

    _summarize("Per-call latency over 5000 random sequential inserts", latencies)


def test_postgres_profile_insert(postgres_server: dict[str, str]) -> None:
    database_url = _fmt_database_url(postgres_server)
    engine = create_engine(database_url)

    index = DedupIndex(
        SQLAlchemyBackend(
            engine=engine,
            base_or_metadata=Base,
            table_name="seq_lsh_index",
        )
    )

    Base.metadata.create_all(engine)

    def profile_insert(index: DedupIndex, bands):
        def target():
            index.query(bands)

        with cProfile.Profile() as pr:
            target()

        stats = pstats.Stats(pr)
        stats.sort_stats("tottime").print_stats(40)

    bands = [(i, f"hash{i}") for i in range(32)]
    profile_insert(index, bands)
