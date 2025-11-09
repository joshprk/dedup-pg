from collections.abc import Iterator
import time

import docker
import pytest

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
    )

    host_port = None

    for _ in range(20):
        container.reload()
        ports = container.attrs.get("NetworkSettings", {}).get("Ports", {})
        if ports and ports.get("5432/tcp"):
            host_port = ports["5432/tcp"][0]["HostPort"]
            break

        time.sleep(0.5)

    # TODO: Use proper health checks to prevent early disconnection
    time.sleep(10.0)

    if host_port is None:
        raise RuntimeError("Postgres container timed out")

    yield {
        "host": "localhost",
        "port": host_port,
        "database": "testdb",
        "user": "postgres",
        "password": "postgres",
    }

    container.stop()
