import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from uuid import uuid4

import pytest
from sqlalchemy import MetaData, create_engine, event, text

from dedup_pg import DedupIndex
from dedup_pg.backend.sqlalchemy import SQLAlchemyBackend
from tests.postgres import _fmt_database_url
from tests.utils.postgres import postgres_server as postgres_server


@pytest.fixture
def backend_factory(postgres_server):
    engines = []
    metadata = MetaData()

    def create(isolation_level="READ COMMITTED"):
        engine = create_engine(
            _fmt_database_url(postgres_server),
            isolation_level=isolation_level,
            connect_args={"options": "-c statement_timeout=10000"},
        )
        engines.append(engine)
        backend = SQLAlchemyBackend(
            engine=engine,
            base_or_metadata=metadata,
            table_name=f"race_{uuid4().hex}",
        )
        index = DedupIndex(backend, num_perms=3, rows=1)
        metadata.create_all(engine)
        return engine, backend, index

    yield create
    if engines:
        metadata.drop_all(engines[0])
    for engine in engines:
        engine.dispose()


def wait_for_blocked_writer(engine, blocking_pid):
    deadline = time.monotonic() + 5
    # Each poll needs fresh pg_stat_activity data, not a cached transaction snapshot.
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        while time.monotonic() < deadline:
            if conn.execute(
                text("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_stat_activity
                    WHERE :pid = ANY(pg_blocking_pids(pid))
                )
            """),
                {"pid": blocking_pid},
            ).scalar_one():
                return
            time.sleep(0.01)
    pytest.fail("Second writer never waited for the first writer")


@pytest.mark.parametrize(
    "isolation", ["READ COMMITTED", "REPEATABLE READ", "AUTOCOMMIT"]
)
@pytest.mark.parametrize("rollback", [False, True])
@pytest.mark.parametrize(
    "second_bands,seed,fail_repair",
    [
        ([11, 22, 33], None, False),
        ([11, 55, 66], None, False),
        ([44, 22, 66], None, False),
        ([11, 55, 66], [44, 55, 77], False),
        ([44, 22, 66], [44, 55, 77], False),
        ([11, 55, 66], [44, 55, 77], True),
    ],
)
def test_concurrent_inserts_repair_only_their_own_bands(
    backend_factory, isolation, rollback, second_bands, seed, fail_repair
):
    engine, backend, index = backend_factory(isolation)
    seed_uuid = index.index(seed) if seed else None
    inserted = Event()
    release = Event()
    first_pid = None
    statements = []

    def hold_first_insert(conn, cursor, statement, parameters, context, executemany):
        nonlocal first_pid
        assert "pg_advisory" not in statement
        if "pg_stat_activity" in statement:
            return
        statements.append(statement)
        if "WITH vals" in statement and not inserted.is_set():
            first_pid = conn.connection.driver_connection.get_backend_pid()
            inserted.set()
            assert release.wait(10), "Timed out releasing first writer"
            if rollback:
                raise RuntimeError("Force first writer rollback")
        if statement.startswith("UPDATE") and fail_repair:
            raise RuntimeError("Force repair rollback")

    event.listen(engine, "after_cursor_execute", hold_first_insert)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(index.index, [11, 22, 33])
            try:
                assert inserted.wait(10)
                # Disjoint writers must still finish while the first writer is held.
                pool.submit(index.index, [88, 99, 100]).result(timeout=3)
                second = pool.submit(index.index, iter(second_bands))
                wait_for_blocked_writer(engine, first_pid)
                assert not second.done()
            finally:
                release.set()

            if rollback:
                with pytest.raises(RuntimeError, match="first writer rollback"):
                    first.result(timeout=10)
                expected_uuid = seed_uuid
            else:
                first_uuid = first.result(timeout=10)
                # Lowest pre-existing band wins; never merge/reassign old clusters.
                expected_uuid = (
                    seed_uuid if seed and second_bands[0] == seed[0] else first_uuid
                )

            if fail_repair and not rollback:
                with pytest.raises(RuntimeError, match="repair rollback"):
                    second.result(timeout=10)
                for i, h in enumerate(second_bands):
                    if h != [11, 22, 33][i] and (not seed or h != seed[i]):
                        assert backend.query(i, h) is None
            else:
                second_uuid = second.result(timeout=10)
                if expected_uuid is not None:
                    assert second_uuid == expected_uuid
                # Each insert executes the CTE once, including the conflict loser.
                assert sum("WITH vals" in s for s in statements) == 3
                if rollback:
                    assert len(statements) == 3
                else:
                    # Identical losers need only a lookup; partial losers also repair.
                    assert len(statements) == (4 if second_bands == [11, 22, 33] else 5)
                for i, h in enumerate(second_bands):
                    if seed and h == seed[i]:
                        owner = seed_uuid
                    elif not rollback and h == [11, 22, 33][i]:
                        owner = first_uuid
                    else:
                        owner = second_uuid
                    assert backend.query(i, h) == owner
                assert index.index(second_bands) == second_uuid

        if not rollback:
            assert all(
                backend.query(i, h) == first_uuid for i, h in enumerate([11, 22, 33])
            )
        if seed:
            assert all(backend.query(i, h) == seed_uuid for i, h in enumerate(seed))
    finally:
        release.set()
        event.remove(engine, "after_cursor_execute", hold_first_insert)


def test_uncontended_inserts_use_one_execution_and_preserve_bridges(backend_factory):
    engine, backend, index = backend_factory()
    statements = []

    def record(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(engine, "after_cursor_execute", record)
    try:
        first = index.index([11, 22, 33])
        second = index.index([44, 55, 66])
        assert first != second
        assert index.index([11, 55, 77]) == first
        assert index.index([11, 55, 77]) == first
        assert len(statements) == 4
    finally:
        event.remove(engine, "after_cursor_execute", record)
    assert backend.query(1, 55) == second
    assert backend.query(2, 77) == first


def test_concurrent_identical_queries(backend_factory):
    _, _, index = backend_factory()
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: index.query(["same", "document"]), range(40)))
    assert len(set(results)) == 1


def test_third_writer_sees_repaired_owner_not_provisional_uuid(backend_factory):
    engine, backend, index = backend_factory()
    winner_uuid = uuid4()
    inserted = Event()
    release = Event()
    loser_pid = None

    def hold_before_repair(conn, cursor, statement, parameters, context, executemany):
        nonlocal loser_pid
        if "WITH vals" in statement and not inserted.is_set():
            loser_pid = conn.connection.driver_connection.get_backend_pid()
            inserted.set()
            assert release.wait(10)

    event.listen(engine, "after_cursor_execute", hold_before_repair)
    try:
        with engine.connect() as winner, winner.begin() as transaction:
            winner_pid = winner.connection.driver_connection.get_backend_pid()
            # A direct SQL writer holds the conflicting key without advisory locks.
            winner.execute(
                backend._table.insert(),
                [
                    {"band_idx": i, "band_hash": h, "cluster_uuid": winner_uuid}
                    for i, h in enumerate([11, 22, 33])
                ],
            )
            with ThreadPoolExecutor(max_workers=2) as pool:
                second = pool.submit(index.index, [44, 22, 66])
                try:
                    wait_for_blocked_writer(engine, winner_pid)
                    transaction.commit()
                    assert inserted.wait(10)
                    third = pool.submit(index.index, [44, 22, 66])
                    wait_for_blocked_writer(engine, loser_pid)
                    # The second writer's new bands remain invisible until repaired.
                    assert backend.query(0, 44) is None
                    assert backend.query(2, 66) is None
                finally:
                    release.set()
                assert second.result(timeout=10) == winner_uuid
                assert third.result(timeout=10) == winner_uuid
        assert backend.query(0, 44) == winner_uuid
        assert backend.query(2, 66) == winner_uuid
    finally:
        release.set()
        event.remove(engine, "after_cursor_execute", hold_before_repair)
