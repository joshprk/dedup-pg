import hashlib
import numpy as np
from collections.abc import Iterable
from uuid import UUID

from .backend import LocalBackend


class DedupIndex:
    def __init__(
        self,
        num_perms: int = 128,
        rows: int = 5,
    ) -> None:
        """
        Indexing layer that allows for query-time deduplication through hashing

        Args:
            num_perms (int): The number of permutation functions to use to generate item signatures
            rows (int): The number of rows to use when making signature bands
        """
        self.num_hashes = num_perms
        self.rows = rows
        self.num_bands = num_perms // rows

        self._backend = LocalBackend()

    def _token_hash(self, token: str, seeds: np.ndarray) -> np.ndarray:
        """
        Hash computation for all seeds per token

        Args:
            token (str): The token to hash
            seeds (list[int]): The array of integer seeds to use for each hash row

        Returns:
            list[int]: An array of integer has hash values, one per seed
        """
        return np.array([
            int(hashlib.blake2b(f"{token}-{seed}".encode(), digest_size=8).hexdigest(), 16)
            for seed in seeds
        ])

    def _minhash_signature(self, tokens: Iterable[str]) -> list[int]:
        """
        Optimized MinHash calculation using numpy vectorization

        Args:
            tokens (Iterable[str]): A list of tokens to compute the MinHash signature of

        Returns:
            list[int]: A MinHash signature consisting of `num_rows` hashes
        """
        tokens = list(tokens)
        seeds = np.arange(self.num_hashes)
        min_hashes = np.full(self.num_hashes, np.inf)

        for token in tokens:
            token_hashes = self._token_hash(token, seeds)
            min_hashes = np.minimum(min_hashes, token_hashes)

        return min_hashes.astype(int).tolist()

    def bands(self, tokens: Iterable[str]) -> list[str]:
        """
        Returns LSH bands. Currently, this only supports MinHash, which is the most popular algorithm for deduplicating
        large-scale data.

        Args:
            tokens (Iterable[str]): An iterable of any byte-encodeable objects which represent the content to
                deduplicate

        Returns:
            list[str]: LSH bands derived from the MinHash signature of the tokens
        """
        signature = self._minhash_signature(tokens)
        band_hashes: list[str] = []

        for i in range(0, len(signature), self.rows):
            band = signature[i:i + self.rows]
            band_str = '|'.join(map(str, band))
            band_hash = hashlib.blake2b(band_str.encode(), digest_size=8).hexdigest()

            band_hashes.append(band_hash)

        return band_hashes

    def items(self, bands: Iterable[str]) -> list[tuple[int, str]]:
        """
        A helper function which converts a list of bands to a normalized (index, band) format. Useful for inserting rows
        into a database.

        Args:
            bands (Iterable[str]): An iterable of LSH bands

        Returns:
            list[tuple[int, str]]: A list of tuples containing the index of the band hash, and then the band hash
                itself. Note that the band hash can be represented as a BIGINT
        """
        return [(idx, bh) for idx, bh in enumerate(bands)]

    def index(self, items: Iterable[tuple[int, str]]) -> UUID:
        """
        Retrieves the cluster UUID4 of a given items list derived from MinHash bands

        Args:
            items (Iterable[str]): A list of item tuples. See `DedupIndex.items` for details
        """
        return self._backend.insert(items)
