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

## Alternatives

This library is the easiest way to implement deduplication in Postgres, and has been successfully
used in production (at the company I'm working at). Most similar libraries are built for local usage
and have non-compact serialization incompatible with Postgres.

However, `datasketch` and `rensa` are good alternatives if you would like something different.
