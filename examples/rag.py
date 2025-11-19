"""
Import dependencies
"""

import textwrap
import time
import random
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from uuid import uuid4

import docker
from sqlalchemy.dialects.postgresql.base import select
import typer
from dedup_pg import DedupIndex
from dedup_pg.backend.sqlalchemy import SQLAlchemyBackend
from dedup_pg.helpers import n_grams
from pgvector.sqlalchemy import HALFVEC
from sqlalchemy import BIGINT, Engine, Index, String, Uuid, create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, mapped_column, sessionmaker
from rich.console import Console
from rich.prompt import Prompt

PGVECTOR_IMAGE_NAME = "pgvector/pgvector:pg18-trixie"

"""
Define Postgres schemas
"""

class Base(DeclarativeBase):
    pass


class Chunk(Base):
    __tablename__: str = "chunk"
    __table_args__: dict[str, Any] = (
        {"postgresql_partition_by": "LIST (dataset_name)"}
    )

    id = mapped_column(BIGINT, autoincrement=True, primary_key=True)
    dataset_name = mapped_column(String, nullable=False, primary_key=True)
    context = mapped_column(String, nullable=False)
    text_embedding_3_small = mapped_column(HALFVEC(1536), nullable=False)
    cluster_uuid = mapped_column(Uuid, nullable=False)


index = Index(
    "chunk_hnsw",
    Chunk.text_embedding_3_small,
    postgresql_using="hnsw",
    postgresql_ops={"text_embedding_3_small": "halfvec_l2_ops"},
)


def _is_ready(database_url: str) -> bool:
    try:
        conn = create_engine(database_url)
        conn.connect().close()
        return True
    except OperationalError as err:
        return False


@contextmanager
def build_demo_engine() -> Generator[Engine]:
    client = docker.from_env()
    _image = client.images.pull(PGVECTOR_IMAGE_NAME)

    database_url = "postgresql+psycopg2://postgres:postgres@localhost:5432/testdb"
    container = client.containers.run(
        PGVECTOR_IMAGE_NAME,
        name="dedup-pg-testdb",
        environment={
            "POSTGRES_PASSWORD": "postgres",
            "POSTGRES_DB": "testdb",
        },
        ports={"5432/tcp": 5432},
        detach=True,
        remove=True,
    )

    while not _is_ready(database_url):
        time.sleep(1.0)

    yield create_engine(database_url)
    container.stop()

"""
Initialize LSH index
"""

engine_ctx = build_demo_engine()
engine = engine_ctx.__enter__()

with engine.begin() as conn:
    conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))

dedup_index = DedupIndex(
    SQLAlchemyBackend(
        engine=engine,
        base_or_metadata=Base,
        table_name="lsh_index",
    )
)

Base.metadata.create_all(engine)

"""
Run the agent loop
"""

app = typer.Typer()
console = Console()

# --------------------------------------------------------------------
# Abstract backend interface (replace implementations with your pgvector code)
# --------------------------------------------------------------------

user_db = {
    1: ["main", "wiki"],
    2: ["main"],
    3: ["wiki"],
}

def _get_embedding(content: str) -> list[float]:
    return [random.random() for _ in range(1536)]

def get_response(query: str, datasets: list[str], top_k: int = 40) -> str:
    query_embedding = _get_embedding(query)

    stmt = text(textwrap.dedent("""
        SET LOCAL hnsw.iterative_scan = 'relaxed_order';

        WITH ranked AS (
            SELECT
                id,
                cluster_uuid,
                context,
                text_embedding_3_small,
                text_embedding_3_small <-> CAST(:embedding AS halfvec(1536)) AS dist
            FROM chunk
            WHERE dataset_name = ANY(:datasets)
            ORDER BY dist
        )
        SELECT DISTINCT ON (cluster_uuid)
            id, cluster_uuid, context, dist
        FROM ranked
        ORDER BY cluster_uuid, dist
        LIMIT :k;
    """))


    with engine.begin() as conn:
        results = conn.execute(stmt, {"embedding": query_embedding, "datasets": datasets, "k": top_k})
        chunks = results.fetchall()

    answer = ""

    for i, chunk in enumerate(chunks):
        chunk_str = textwrap.indent(chunk.context, "  ")
        answer += f"Chunk {i} (cluster: {chunk.cluster_uuid}):\n\n{chunk_str}\n\n"

    return answer

# --------------------------------------------------------------------
# App code
# --------------------------------------------------------------------

def index_first() -> None:
    stmts = [
        "CREATE TABLE IF NOT EXISTS chunk_main PARTITION OF chunk FOR VALUES IN ('main');",
        "CREATE TABLE IF NOT EXISTS chunk_wiki PARTITION OF chunk FOR VALUES IN ('wiki');"
    ]

    with engine.begin() as conn:
        for s in stmts:
            conn.execute(text(s))

    sentences_main = [
        "1, The quick brown fox jumps over the lazy dog.",
        "2, PostgreSQL supports high performance vector search.",
        "3, Main dataset sample sentence number three.",
        "4, Embeddings are numerical representations of text.",
        "5, This is another example sentence for indexing."
    ]

    sentences_main_scrambled = [
        "1, The qui k bnown fox jumps over the lazy dog.",
        "2, PostgreS0L supports high performance vetor serch.",
        "3, Main datase saple sentence number thre.",
        "4, Embeddings re nulerical representtios of text.",
        "5, This i aother xample sentnce for indexing."
    ]
    
    SessionLocal = sessionmaker(engine)

    with SessionLocal() as session:
        for ds_name, sentences in (("main", sentences_main), ("wiki", sentences_main_scrambled)):
            for s in sentences:
                emb = _get_embedding(s)
                row = Chunk(
                    dataset_name=ds_name,
                    context=s,
                    text_embedding_3_small=emb,
                    # Use this for raw:
                    # cluster_uuid=uuid4(),
                    # Use this for deduplication:
                    cluster_uuid=dedup_index.query(n_grams(s)),
                )
                session.add(row)

        session.commit()


def chat() -> None:
    index_first()

    current_user = 1

    while query := Prompt.ask(f"[bold cyan]User {current_user}[/bold cyan] ([bold green]{str(user_db[current_user])}[/bold green])"):
        if query.startswith("/"):
            if query.lower().startswith("/user"):
                args = query.split()
                current_user = int(args[1])
                console.print(f"[bold red]Switch to user {current_user}[/bold red]")
            elif query.lower() == "/exit":
                break

            continue

        response = get_response(query, user_db[current_user])

        console.print(f"[bold yellow]Assistant:[/bold yellow]\n\n{response}\n")


# --------------------------------------------------------------------

if __name__ == "__main__":
    chat()

"""
Perform cleanup
"""

_ = engine_ctx.__exit__(None, None, None)
