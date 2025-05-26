from abc import ABC, abstractmethod
from typing import Iterable


class Writer(ABC):

    @abstractmethod
    def write(self, data: dict | Iterable[dict]) -> None:
        pass


