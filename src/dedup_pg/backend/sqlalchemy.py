import textwrap
from collections.abc import Iterable
from uuid import UUID, uuid4

from sqlalchemy import (
    BIGINT,
    Column,
    Connection,
    Engine,
    MetaData,
    SmallInteger,
    Table,
    UniqueConstraint,
    Uuid,
    bindparam,
    select,
    text,
    tuple_,
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

        # Precompile PostgreSQL insert stmt
        self._insert_stmt = (
            text(textwrap.dedent(f"""
                SET LOCAL synchronous_commit = OFF;

                WITH vals(idx, hash) AS (
                    VALUES
                        {values_clause}
                ),
                existing AS (
                    SELECT t.band_idx, t.band_hash, t.cluster_uuid
                    FROM {table_name} t
                    JOIN vals v ON t.band_idx = v.idx AND t.band_hash = v.hash
                ),
                missing AS (
                    SELECT v.idx, v.hash FROM vals v
                    WHERE NOT EXISTS (
                        SELECT 1 FROM existing e
                        WHERE e.band_idx = v.idx AND e.band_hash = v.hash
                    )
                ),
                chosen AS (
                    SELECT COALESCE(
                        (SELECT cluster_uuid FROM existing ORDER BY band_idx LIMIT 1),
                        :new_uuid
                    ) AS uuid
                ),
                ins AS (
                    INSERT INTO {table_name} (band_idx, band_hash, cluster_uuid)
                    SELECT v.idx, v.hash, chosen.uuid
                    FROM missing v CROSS JOIN chosen
                    ORDER BY v.idx
                    ON CONFLICT (band_idx, band_hash) DO NOTHING
                    RETURNING band_idx
                )
                SELECT uuid,
                    ARRAY(SELECT band_idx FROM ins) AS inserted_indices,
                    (SELECT count(*) FROM ins) < (SELECT count(*) FROM missing) AS conflicted
                FROM chosen;
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

        # A fresh statement must see the winner after a unique-key wait. Explicit
        # READ COMMITTED also keeps repairs atomic with engines using AUTOCOMMIT.
        with self._engine.connect().execution_options(isolation_level="READ COMMITTED") as conn:
            with conn.begin():
                cluster_uuid, inserted_indices, conflicted = conn.execute(
                    self._insert_stmt, params
                ).one()
                if conflicted:
                    cluster_uuid = self._resolve_conflicts(conn, band_pairs, inserted_indices)

        assert isinstance(cluster_uuid, UUID)

        return cluster_uuid

    def _resolve_conflicts(
        self,
        conn: Connection,
        band_pairs: list[tuple[int, int]],
        inserted_indices: list[int],
    ) -> UUID:
        """Adopt an existing owner and repair only this transaction's new bands."""
        inserted = set(inserted_indices)
        existing_pairs = [pair for pair in band_pairs if pair[0] not in inserted]
        band_key = tuple_(self._table.c.band_idx, self._table.c.band_hash)

        # Conflicting inserts have finished waiting, so their committed owners are
        # visible in this new snapshot. Exclude our own provisional assignments.
        cluster_uuid = conn.execute(
            select(self._table.c.cluster_uuid)
            .where(band_key.in_(existing_pairs))
            .order_by(self._table.c.band_idx)
            .limit(1)
        ).scalar_one()

        if inserted:
            inserted_pairs = [pair for pair in band_pairs if pair[0] in inserted]
            conn.execute(
                self._table.update()
                .where(band_key.in_(inserted_pairs))
                .values(cluster_uuid=cluster_uuid)
            )

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
