import hashlib
import uuid
from collections.abc import Iterable


class DedupIndex:
    """
    Indexing layer that allows for query-time deduplication through hashing.

    Args:
        threshold (float): The approximate Jaccard similarity cutoff for duplicate chunks
        weights (tuple[float, float]): Relative importance of minimizing false-positives and false-negatives
        num_
    """
    def __init__(
        self,
        threshold: float = 0.9,
        weights: tuple[float, float] = (0.5, 0.5),
        num_perms: int = 128,
        rows: int = 5,
    ) -> None:
        weights = (weights[0] / sum(weights), weights[1] / sum(weights))
        self.num_hashes = num_perms
        self.rows = rows
        self.num_bands = num_perms // rows

        # band -> cluster UUID
        self.band_index: dict[tuple[int, str], str] = {}

    def _token_hash(self, token: str, seed: int) -> int:
        # compact and fast 64-bit hash
        return int(hashlib.blake2b(f"{token}-{seed}".encode(), digest_size=8).hexdigest(), 16)

    def _minhash_signature(self, tokens: Iterable[str]) -> list[int]:
        tokens = list(tokens)  # allow multiple passes
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
            # we just use the string directly, no second sha256 needed
            band_hash = hashlib.blake2b(band_str.encode(), digest_size=8).hexdigest()
            band_hashes.append(band_hash)
        return band_hashes

    def items(self, band_hashes: list[str]) -> list[tuple[int, str]]:
        return [(idx, bh) for idx, bh in enumerate(band_hashes)]

    def index(self, items: Iterable[tuple[int, str]]) -> str:
        """
        Insert or lookup an item. If any band already belongs to a cluster,
        reuse that cluster's UUID. Otherwise, create a new cluster UUID.
        """
        found_uuid = None
        for band in items:
            if band in self.band_index:
                found_uuid = self.band_index[band]
                break

        if found_uuid is None:
            found_uuid = str(uuid.uuid4())

        # Assign all bands of this item to that UUID
        for band in items:
            self.band_index[band] = found_uuid

        return found_uuid
