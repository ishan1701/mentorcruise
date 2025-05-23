from abc import ABC, abstractmethod
from typing import Iterable


class Writer(ABC):

    @staticmethod
    @abstractmethod
    def write(self, data: dict | Iterable[dict], avro_schema: dict, **kwargs) -> None:
        pass


