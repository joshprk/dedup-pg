from collections.abc import Iterator
import time
import docker

PGVECTOR_IMAGE_NAME = "pgvector/pgvector:pg18-trixie"


@pytest.fixture(scope="session")
def postgres_server() -> Iterator[dict[str, str]]:
    """
    Start a Postgres database with `pgvector` installed
    """
    client = docker.from_env()
    _image = client.images.pull(PGVECTOR_IMAGE_NAME)

    container = client.containers.run(
        PGVECTOR_IMAGE_NAME,
        name="dedup-pg-testdb",
        environment={
            "POSTGRES_PASSWORD": "postgres",
            "POSTGRES_DB": "testdb",
        },
        ports={"5432/tcp": None},
        detach=True,
        remove=True,
        healthcheck={
            "test": ["CMD-SHELL", "pg_isready -U postgres"],
            "interval": 1000000000,
            "timeout": 3000000000,
            "retries": 5,
        },
    )

    yield {
        "host": "localhost",
        "port": str(5432),
        "database": "testdb",
        "user": "postgres",
        "password": "postgres",
    }

    container.stop()
