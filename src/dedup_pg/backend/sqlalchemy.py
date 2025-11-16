from collections.abc import Iterable
from uuid import UUID, uuid4

from sqlalchemy import (
    Column,
    Engine,
    MetaData,
    SmallInteger,
    String,
    Table,
    UniqueConstraint,
    Uuid,
    select,
    tuple_
)
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert

from dedup_pg.backend.backend import Backend


class SQLAlchemyBackend(Backend):
    def __init__(
        self,
        *,
        engine: Engine | None = None,
        session_factory: sessionmaker[Session] | None = None,
        base_or_metadata: type[DeclarativeBase] | MetaData,
        table_name: str,
    ) -> None:
        """
        The SQLAlchemy backend for the deduplication indexing layer.

        Note that this expects a PostgreSQL engine or session_factory to be provided.

        Args:
            base (type[DeclarativeBase] | MetaData): Any SQLAlchemy base, registry, or MetaData object. Must expose a
                `.metadata` attribute or be a MetaData instance.
            table_name (str): Name of the deduplication index table.
        """
        if isinstance(base_or_metadata, MetaData):
            metadata = base_or_metadata
        elif hasattr(base_or_metadata, "metadata"):
            metadata = base_or_metadata.metadata
        else:
            raise TypeError("Expected SQLAlchemy DeclarativeBase, registry, or MetaData object")

        if engine is None and session_factory is None:
            raise ValueError("Must provide either engine or session_factory")

        if session_factory is None:
            session_factory = sessionmaker(engine)

        self._session_factory = session_factory
        self._metadata = metadata
        self._table = Table(
            table_name,
            self._metadata,
            Column("band_idx", SmallInteger, nullable=False),
            Column("band_hash", String, nullable=False),
            Column("cluster_uuid", Uuid, nullable=False),
        )

        self._table.append_constraint(
            UniqueConstraint(
                "band_idx",
                "band_hash",
                name=f"{table_name}_band_idx_band_hash_key"
            )
        )

        # Precompile PostgreSQL insert stmt
        self._insert_sql = (
            pg_insert(self._table)
            .on_conflict_do_nothing(index_elements=["band_idx", "band_hash"])
        )

    def insert(self, bands: Iterable[tuple[int, str]]) -> UUID:
        check_existing_stmt = (
            select(self._table.c.cluster_uuid)
            .where(tuple_(self._table.c.band_idx, self._table.c.band_hash).in_(bands))
            .limit(1)
        )

        with self._session_factory() as session:
            existing_uuid = session.execute(check_existing_stmt).scalars().first()
            cluster_uuid = existing_uuid or uuid4()

            # NOTE: Only works on Postgres.
            values = [
                {"band_idx": i, "band_hash": h, "cluster_uuid": str(cluster_uuid)}
                for i, h in bands
            ]

            _ = session.execute(self._insert_sql, values)
            session.commit()

        return cluster_uuid

    def query(self, index: int, band: str) -> UUID | None:
        stmt = select(self._table.c.cluster_uuid).where(
            self._table.c.band_idx == index,
            self._table.c.band_hash == band,
        ).limit(1)

        with self._session_factory() as session:
            result = session.execute(stmt).scalars().first()

        return result
