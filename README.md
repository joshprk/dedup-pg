# dedup-pg

A library with functions useful for implementing a MinHash-based deduplication indexing layer in
Postgres, or any relational database.

## Use cases

In cases where you have to search for specific items in a dataset derived from noisy data, it is
likely that there are duplicates which hurt retrieval quality. We can estimate the similarity
between such items by hashing their components in a way to approximate their Jaccard similarity.
This can be useful for deduplication before item ingestion into an online production database.

However, if your system has special constraints, particularly multi-tenancy where you cannot simply
delete items for every user (because some users might not have access to certain duplicates), it
becomes more infeasible to compute Jaccard similarity pair-wise per query. This library helps solve
this by using locality-sensitive hashing to bucket items that are likely to be above a specific
Jaccard similarity.

In short, it makes query-time deduplication possible and efficient for search systems with special
needs such as multi-tenant retrieval-augmented generation (RAG).

## Usage

Below is an example of usage for deduplicating textual chunks.

```py
from collections import defaultdict

from dedup_pg import DedupIndex
from dedup_pg.helpers import n_grams

# A corpus of named items we want to deduplicate
corpus = [
    ("key1", "The quick brown fox jumps over the lazy dog"),
    ("key2", " he quic  bnown f x jump  over the  azy dog"),
    ("key3", "An entirely different sentence!"),
]

# Our deduplication index - this can be Postgres-backed with configuration
lsh = DedupIndex()

# Using n=3 character n-grams is a strong choice for deduplicating textual chunks
n_gram_corpus = [(key, n_grams(text, n=3)) for key, text in corpus]

# Index bands for each key which help us determine duplicates
duplicate_map = defaultdict(list)
for key, n_gram in n_gram_corpus:
    cluster_key = lsh.query(n_gram)
    duplicate_map[cluster_key].append(key)

# `key1` and `key2` are in the same cluster in contrast to `key3`
print(duplicate_map)
```

For ease-of-use, we provide the `dedup_pg.backend.sqlalchemy.SQLAlchemy` backend, which you use by
passing it the the `DedupIndex` initialization.

### PostgreSQL concurrency

Concurrent SQLAlchemy inserts that share a `(band_idx, band_hash)` key acquire
transaction-scoped advisory locks in a consistent order. The lookup then runs in a
separate READ COMMITTED statement, so it sees the preceding writer's commit.
The backend uses READ COMMITTED for its own insert transactions even if the engine
has a different default isolation level. Disjoint band sets can still proceed concurrently;
advisory-lock hash collisions can cause extra waiting but cannot change band matches.
This adds one database round trip per insert.

If an input matches multiple existing clusters, the lowest matching band index wins.
Existing clusters are not merged or reassigned; previously returned UUIDs remain unchanged.
New bands are stored under the selected UUID, as before. The existing
`synchronous_commit = OFF` durability tradeoff is unchanged.

No schema migration is required. For rollout, stop/drain old writers before enabling
new ones: all writers to an index must use this locking protocol. Mixed-version or
direct SQL writers can still race. This fix does not repair UUIDs already returned
by the old race.

### Running tests

```sh
uv sync --all-extras
PYTHONPATH=src uv run pytest tests/readme.py tests/postgres.py tests/test_postgres_concurrency.py
```

By default the PostgreSQL fixture starts a PostgreSQL 18 Docker container. To use an
existing **disposable test database**, set `DEDUP_PG_TEST_DATABASE_URL` to its SQLAlchemy
PostgreSQL URL. Tests create and drop tables; never point them at production.

## Alternatives

This library is the easiest way to implement deduplication in Postgres, and has been successfully
used in production (at the company I'm working at). Most similar libraries are built for local usage
and have non-compact serialization incompatible with Postgres.

However, `datasketch` and `rensa` are good alternatives if you would like something different.
