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
        self._num_bands = num_bands
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

    def _init_internal(self, num_bands: int) -> None:
        """
        Initializes backend to be ready for use by an Index. For the SQLAlchemy backend, we use _insert_stmt.
        """
        values_clause = ",\n        ".join(
            f"(:i{i}, :h{i})" for i in range(num_bands)
        )

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
                    FROM {self._table.name} t
                    JOIN vals v ON t.band_idx = v.idx AND t.band_hash = v.hash
                    LIMIT 1
                ),
                chosen AS (
                    SELECT COALESCE((SELECT cluster_uuid FROM existing), :new_uuid) AS uuid
                ),
                ins AS (
                    INSERT INTO {self._table.name} (band_idx, band_hash, cluster_uuid)
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
        params = {}

        for i, h in band_pairs:
            params[f"i{i}"] = i
            params[f"h{i}"] = h

        new_uuid = uuid4()
        params["new_uuid"] = str(new_uuid)

        with self._engine.begin() as conn:
            """
            We don't use this code, but useful for seeing what round-trips that our CTE optimizes.

            > # Commit returns before WAL is flushed to durable storage.
            > # The transaction is visible immediately, but a crash before a WAL flush may
            > # forget some committed transactions.
            > #
            > # This is a good-tradeoff as it saves nearly 5 ms of commit time for a high-volume
            > # transaction in exchange for long-tail errors.
            > _ = conn.execute(text("SET LOCAL synchronous_commit = OFF"))

            > existing_uuid = conn.execute(check_existing_stmt).scalars().first()
            > cluster_uuid = existing_uuid or uuid4() 

            > # NOTE: Only works on Postgres.
            > values = [
            >     {"band_idx": i, "band_hash": h, "cluster_uuid": str(cluster_uuid)}
            >     for i, h in enumerate(bands)
            > ]

            > _ = conn.execute(self._insert_sql, values)
            > conn.commit()
            """
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
