import hashlib
import struct
import numpy as np
import xxhash
from collections.abc import Iterable
from uuid import UUID

from .backend import Backend, LocalBackend

MAX64 = np.uint64((1 << 64) - 1) # max uint64 mask
MIX_CONST = np.uint64(0x9e3779b97f4a7c15) # golden ratio


class DedupIndex:
    def __init__(
        self,
        backend: Backend | None = None,
        num_perms: int = 128,
        rows: int = 4,
    ) -> None:
        """
        Indexing layer that allows for query-time deduplication through hashing.

        Args:
            num_perms (int): The number of permutation functions to use to generate item signatures.
            rows (int): The number of rows to use when making signature bands.
        """
        self.num_hashes = num_perms
        self.rows = rows
        self.num_bands = num_perms // rows

        self._backend = LocalBackend() if backend is None else backend
        self._backend._init_internal(self.num_bands) # pyright: ignore[reportPrivateUsage]

    def _token_hash(self, token: str, seeds: np.ndarray) -> np.ndarray:
        """
        Hash computation for all seeds per token.

        Args:
            token (str): The token to hash.
            seeds (list[int]): The array of integer seeds to use for each hash row.

        Returns:
            list[int]: An array of integer has hash values, one per seed.
        """
        base = token.encode()
        hashes = np.empty(len(seeds), dtype=np.uint64)

        for i, seed in enumerate(seeds):
            payload = base + b"-" + str(int(seed)).encode()
            hashes[i] = xxhash.xxh3_64(payload).intdigest()

        return hashes

    def _minhash_signature(self, tokens: Iterable[str]) -> np.ndarray:
        """
        Optimized MinHash calculation using numpy vectorization.

        Args:
            tokens (Iterable[str]): A list of tokens to compute the MinHash signature of.

        Returns:
            list[int]: A MinHash signature consisting of `num_rows` hashes.
        """
        tokens = list(tokens)
        seeds = np.arange(self.num_hashes, dtype=np.uint64)
        perm = seeds * MIX_CONST
        min_hashes = np.full(self.num_hashes, MAX64, dtype=np.uint64)

        for token in tokens:
            h0 = np.uint64(xxhash.xxh3_64(token).intdigest())
            token_hashes = h0 ^ perm
            min_hashes = np.minimum(min_hashes, token_hashes)

        return min_hashes

    def bands(self, tokens: Iterable[str]) -> list[int]:
        """
        Returns LSH bands. Currently, this only supports MinHash, which is the most popular algorithm for deduplicating
        large-scale data.

        Args:
            tokens (Iterable[str]): An iterable of any byte-encodeable objects which represent the content to
                deduplicate.

        Returns:
            list[str]: LSH bands derived from the MinHash signature of the tokens.
        """
        signature = self._minhash_signature(tokens)
        band_hashes: list[int] = []

        for i in range(0, len(signature), self.rows):
            band = signature[i:i + self.rows]
            payload = struct.pack(f"{len(band)}Q", *band)
            band_hash = np.uint64(xxhash.xxh64(payload).intdigest()).view(np.int64)
            band_hashes.append(int(band_hash))

        return band_hashes

    def index(self, items: Iterable[int]) -> UUID:
        """
        Retrieves the cluster UUID4 of a given items list derived from MinHash bands. This may add a new entry to the
        backend if the bands do not exist.

        Args:
            items (Iterable[str]): A list of item tuples. See `DedupIndex.items` for details.

        Returns:
            UUID: The cluster ID of the given MinHash bands.
        """
        return self._backend.insert(items)

    def query(self, tokens: Iterable[str]) -> UUID:
        """
        Retrieves the cluster UUID4 of the given tokens. This may add a new entry to the backend if the bands do not
        exist.

        Args:
            tokens (Iterable[str]): A list of tokens derived from some function such as the n_grams function.

        Returns:
            UUID: The cluster ID of the given tokens.
        """
        bands = self.bands(tokens)
        return self.index(bands)
