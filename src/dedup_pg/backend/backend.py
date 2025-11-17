from abc import ABC, abstractmethod
from collections.abc import Iterable
from uuid import UUID, uuid4


class Backend(ABC):
    @abstractmethod
    def insert(self, bands: Iterable[int]) -> UUID:
        ...

    @abstractmethod
    def query(self, index: int, band: int) -> UUID | None:
        ...

    def _init_internal(self, num_bands: int) -> None:
        pass


class LocalBackend(Backend):
    def __init__(self) -> None:
        """
        A local backend as an example of how to implement the Backend class.
        """
        self._index: dict[tuple[int, int], UUID] = {}

    def insert(self, bands: Iterable[int]) -> UUID:
        found_uuid = None

        for index, band in enumerate(bands):
            if (query := self.query(index, band)) is not None:
                found_uuid = query
                break

        if found_uuid is None:
            found_uuid = uuid4()
            for item in enumerate(bands):
                self._index[item] = found_uuid

        return found_uuid

    def query(self, index: int, band: int) -> UUID | None:
        item = (index, band)

        if item in self._index:
            return self._index[item]

        return None
