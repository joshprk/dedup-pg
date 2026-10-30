import textwrap
from collections.abc import Iterable
from uuid import UUID, uuid4

from sqlalchemy import (
    BIGINT,
    Column,
    Engine,
    MetaData,
    SmallInteger,
    Table,
    UniqueConstraint,
    Uuid,
    bindparam,
    select,
    text,
)
from sqlalchemy.orm import DeclarativeBase

from dedup_pg.backend.backend import Backend


class SQLAlchemyBackend(Backend):
    def __init__(
        self,
        *,
        engine: Engine,
        base_or_metadata: type[DeclarativeBase] | MetaData,
        table_name: str,
    ) -> None:
        """
        The SQLAlchemy backend for the deduplication indexing layer.

        Note that this expects a PostgreSQL engine to be provided. You should put this where you
        define your schemas in order to register the table backing this index into your Alembic
        migrations.

        Args:
            base (type[DeclarativeBase] | MetaData): Any SQLAlchemy base, registry, or MetaData
                object. Must expose a `.metadata` attribute or be a MetaData instance.
            table_name (str): Name of the deduplication index table.
        """
        if isinstance(base_or_metadata, MetaData):
            metadata = base_or_metadata
        elif hasattr(base_or_metadata, "metadata"):
            metadata = base_or_metadata.metadata
        else:
            raise TypeError("Expected SQLAlchemy DeclarativeBase, registry, or MetaData object")

        self._engine = engine
        self._metadata = metadata
        self._table = Table(
            table_name,
            self._metadata,
            Column("band_idx", SmallInteger, nullable=False),
            Column("band_hash", BIGINT, nullable=False),
            Column("cluster_uuid", Uuid, nullable=False),
        )

        self._table.append_constraint(
            UniqueConstraint(
                "band_idx",
                "band_hash",
                name=f"{table_name}_band_idx_band_hash_key"
            )
        )

        # This needs to know the num_bands before usage, so we set it to None and throw fatal exceptions if
        # the backend is used standalone.
        self._insert_stmt = None
        self._num_bands = None

    def _init_internal(self, num_bands: int) -> None:
        """
        Initializes backend to be ready for use by an Index. For the SQLAlchemy backend, we use _insert_stmt.
        """
        values_clause = ",\n        ".join(
            f"(:i{i}, :h{i})" for i in range(num_bands)
        )
        self._num_bands = num_bands
        table_name = self._engine.dialect.identifier_preparer.format_table(self._table)

        # Resolve the physical table so qualified and search_path-based names use
        # the same locks. Hash collisions only serialize unrelated inserts.
        self._lock_stmt = text(f"""
            SELECT pg_advisory_xact_lock(lock_key)
            FROM (
                SELECT DISTINCT hashtextextended(
                    CAST(:lock_table AS regclass)::oid::text || ':' || idx || ':' || hash,
                    0
                ) AS lock_key
                FROM (VALUES {values_clause}) AS vals(idx, hash)
            ) AS locks
            ORDER BY lock_key
        """)
        self._lock_table = table_name

        # Precompile PostgreSQL insert stmt
        self._insert_stmt = (
            text(textwrap.dedent(f"""
                SET LOCAL synchronous_commit = OFF;

                WITH vals(idx, hash) AS (
                    VALUES
                        {values_clause}
                ),
                existing AS (
                    SELECT cluster_uuid
                    FROM {table_name} t
                    JOIN vals v ON t.band_idx = v.idx AND t.band_hash = v.hash
                    ORDER BY t.band_idx
                    LIMIT 1
                ),
                chosen AS (
                    SELECT COALESCE((SELECT cluster_uuid FROM existing), :new_uuid) AS uuid
                ),
                ins AS (
                    INSERT INTO {table_name} (band_idx, band_hash, cluster_uuid)
                    SELECT v.idx, v.hash, chosen.uuid
                    FROM vals v CROSS JOIN chosen
                    ON CONFLICT (band_idx, band_hash) DO NOTHING
                )
                SELECT uuid FROM chosen;
            """))
            .bindparams(
                *(bindparam(f"i{k}") for k in range(num_bands)),
                *(bindparam(f"h{k}") for k in range(num_bands)),
                bindparam("new_uuid"),
            )
        )

    def insert(self, bands: Iterable[int]) -> UUID:
        if self._insert_stmt is None:
            raise RuntimeError("SQLAlchemyBackend must be used through an DedupIndex.")

        # Perform parameter computations before starting a session to leave it open as short as
        # possible to avoid connection jamming.
        band_pairs = list(enumerate(bands))
        if len(band_pairs) != self._num_bands:
            raise ValueError(f"Expected {self._num_bands} bands, got {len(band_pairs)}")
        params = {}

        for i, h in band_pairs:
            params[f"i{i}"] = i
            params[f"h{i}"] = h

        new_uuid = uuid4()
        params["new_uuid"] = str(new_uuid)
        params["lock_table"] = self._lock_table

        # A separate statement is essential: the lock statement's snapshot may predate
        # the writer we wait for. READ COMMITTED gives the CTE a fresh snapshot even
        # when the supplied engine defaults to REPEATABLE READ or AUTOCOMMIT.
        with self._engine.connect().execution_options(isolation_level="READ COMMITTED") as conn:
            with conn.begin():
                conn.execute(self._lock_stmt, params).close()
                result = conn.execute(self._insert_stmt, params)
                cluster_uuid = result.scalar()

        assert isinstance(cluster_uuid, UUID)

        return cluster_uuid

    def query(self, index: int, band: int) -> UUID | None:
        stmt = (
            select(self._table.c.cluster_uuid)
            .where(self._table.c.band_idx == index, self._table.c.band_hash == band)
            .limit(1)
        )

        with self._engine.begin() as conn:
            result = conn.execute(stmt).scalars().first()

        return result
