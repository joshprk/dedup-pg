from collections.abc import Iterator
import docker
import pytest

PGVECTOR_IMAGE_NAME = "pgvector/pgvector:latest"


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
    )

    host_port = container.attrs["NetworkSettings"]["Ports"]["5432/tcp"][0]["HostPort"]

    yield {
        "host": "localhost",
        "port": host_port,
        "database": "testdb",
        "user": "postgres",
        "password": "postgres",
    }

    container.stop()
