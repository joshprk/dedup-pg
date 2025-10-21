import hashlib
from collections.abc import Iterable
from uuid import UUID, uuid4

from .backend import LocalBackend


class DedupIndex:
    """
    Indexing layer that allows for query-time deduplication through hashing.

    Args:
        num_perms (int): The number of permutation functions to use to generate item signatures
        rows (int): The number of rows to use when making signature bands
    """
    def __init__(
        self,
        num_perms: int = 128,
        rows: int = 5,
    ) -> None:
        self.num_hashes = num_perms
        self.rows = rows
        self.num_bands = num_perms // rows

        self._backend = LocalBackend()

    def _token_hash(self, token: str, seed: int) -> int:
        return int(hashlib.blake2b(f"{token}-{seed}".encode(), digest_size=8).hexdigest(), 16)

    def _minhash_signature(self, tokens: Iterable[str]) -> list[int]:
        tokens = list(tokens)
        signature = []

        for seed in range(self.num_hashes):
            min_hash = min(self._token_hash(t, seed) for t in tokens)
            signature.append(min_hash)

        return signature

    def bands(self, tokens: Iterable[str]) -> list[str]:
        signature = self._minhash_signature(tokens)
        band_hashes: list[str] = []

        for i in range(0, len(signature), self.rows):
            band = signature[i:i + self.rows]
            band_str = '|'.join(map(str, band))
            band_hash = hashlib.blake2b(band_str.encode(), digest_size=8).hexdigest()

            band_hashes.append(band_hash)

        return band_hashes

    def items(self, band_hashes: list[str]) -> list[tuple[int, str]]:
        return [(idx, bh) for idx, bh in enumerate(band_hashes)]

    def index(self, items: Iterable[tuple[int, str]]) -> UUID:
        return self._backend.insert(items)
