from collections import defaultdict

from src.dedup_pg import DedupIndex
from src.dedup_pg.helpers import n_grams

# A corpus of named items we want to deduplicate
corpus = [
    ("key1", "The quick brown fox jumps over the lazy dog"),
    ("key2", "T e qui k bnown fox jump  over t e  azy  og"),
    ("key3", "An entirely different sentence!"),
]

# Our deduplication index - this can be Postgres-backed with configuration
lsh = DedupIndex()

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
print(duplicate_map)
