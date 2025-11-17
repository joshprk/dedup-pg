from collections import defaultdict

from dedup_pg import DedupIndex
from dedup_pg.backend import Backend
from dedup_pg.helpers import n_grams


def readme_func(backend: Backend | None = None) -> dict[str, list[str]]:
    # A corpus of named items we want to deduplicate
    corpus = [
        ("key1", "The quick brown fox jumps over the lazy dog"),
        ("key2", " he quic  bnown f x jump  over the  azy dog"),
        ("key3", "An entirely different sentence!"),
    ]

    # Our deduplication index - this can be Postgres-backed with configuration
    lsh = DedupIndex(backend)

    # Using n=3 character n-grams is a strong choice for deduplicating textual chunks
    n_gram_corpus = [(key, n_grams(text, n=3)) for key, text in corpus]

    # Index bands for each key which help us determine duplicates
    duplicate_map = defaultdict(list)
    for key, n_gram in n_gram_corpus:
        bands = lsh.bands(n_gram)
        lsh_items = lsh.items(bands)
        cluster_key = lsh.index(lsh_items)

        duplicate_map[cluster_key].append(key)

    # `key1` and `key2` are in the same cluster in contrast to `key3`
    return duplicate_map


def test_readme():
    result = list(readme_func().values())

    assert "key1" in result[0] and "key2" in result[0]
    assert "key3" in result[1]
