from abc import ABC, abstractmethod
from uuid import UUID, uuid4


class Backend(ABC):
    @abstractmethod
    def insert(self, index: int, band: str) -> UUID:
        pass

    @abstractmethod
    def query(self, index: int, band: str) -> UUID | None:
        pass


class LocalBackend(Backend):
    def __init__(self) -> None:
        self._index: dict[tuple[int, str], UUID] = {}

    def insert(self, index: int, band: str) -> UUID:
        item = (index, band)
        key = self.query(index, band)

        if key is None:
            self._index[item] = uuid4()
            key = self._index[item]

        return key

    def query(self, index: int, band: str) -> UUID | None:
        item = (index, band)

        if item in self._index:
            return self._index[item]

        return None
