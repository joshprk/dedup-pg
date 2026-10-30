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
        index = DedupIndex(backend, num_perms=2, rows=1)
        metadata.create_all(engine)
        return engine, backend, index

    yield create
    if engines:
        metadata.drop_all(engines[0])
    for engine in engines:
        engine.dispose()


@pytest.mark.parametrize(
    "isolation", ["READ COMMITTED", "REPEATABLE READ", "AUTOCOMMIT"]
)
@pytest.mark.parametrize("second_bands", [[11, 22], [11, 33]])
@pytest.mark.parametrize("rollback", [False, True])
def test_overlapping_inserts_wait_for_committed_owner(
    backend_factory, isolation, second_bands, rollback
):
    engine, backend, index = backend_factory(isolation)
    inserted = Event()
    release = Event()
    first_pid = None

    def hold_first_insert(conn, cursor, statement, parameters, context, executemany):
        nonlocal first_pid
        if "SELECT uuid FROM chosen" in statement and not inserted.is_set():
            first_pid = conn.connection.driver_connection.get_backend_pid()
            inserted.set()
            assert release.wait(10), "Timed out waiting to release first writer"
            if rollback:
                raise RuntimeError("Force rollback after inserting bands")

    event.listen(engine, "after_cursor_execute", hold_first_insert)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(index.index, [11, 22])
            try:
                assert inserted.wait(10), "First writer did not insert"
                # Unrelated inputs must not serialize behind a table-wide lock.
                pool.submit(index.index, [55, 66]).result(timeout=3)
                second = pool.submit(index.index, iter(second_bands))
                deadline = time.monotonic() + 5
                # Observe an actual database lock wait, not a timing-based guess.
                # Autocommit refreshes pg_stat_activity's snapshot on every poll.
                with engine.connect().execution_options(
                    isolation_level="AUTOCOMMIT"
                ) as observer:
                    while time.monotonic() < deadline:
                        waiting = observer.execute(
                            text("""
                            SELECT EXISTS (
                                SELECT 1 FROM pg_stat_activity
                                WHERE :pid = ANY(pg_blocking_pids(pid))
                            )
                        """),
                            {"pid": first_pid},
                        ).scalar_one()
                        if waiting:
                            break
                        time.sleep(0.01)
                assert waiting, "Second writer never waited for first writer"
                assert not second.done()
            finally:
                release.set()

            if rollback:
                with pytest.raises(RuntimeError, match="Force rollback"):
                    first.result(timeout=10)
            else:
                first_uuid = first.result(timeout=10)
            second_uuid = second.result(timeout=10)
            if not rollback:
                assert second_uuid == first_uuid

        assert index.index(second_bands) == second_uuid
        assert all(
            backend.query(i, h) == second_uuid for i, h in enumerate(second_bands)
        )
        if rollback and second_bands[1] != 22:
            assert backend.query(1, 22) is None
    finally:
        release.set()
        event.remove(engine, "after_cursor_execute", hold_first_insert)


def test_bridge_preserves_existing_clusters(backend_factory):
    _, backend, index = backend_factory()
    first = index.index([11, 22])
    second = index.index([33, 44])
    assert first != second
    assert index.index([11, 44]) == first
    assert backend.query(0, 33) == second
    assert backend.query(1, 44) == second


def test_concurrent_identical_queries(backend_factory):
    _, _, index = backend_factory()
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: index.query(["same", "document"]), range(40)))
    assert len(set(results)) == 1
