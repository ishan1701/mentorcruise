from abc import ABC, abstractmethod
from typing import Iterable, Callable


class Writer(ABC):

    @staticmethod
    @abstractmethod
    def write(self, data: dict | Iterable[dict], schema: dict, **kwargs) -> None:
        pass


# context class
class Context:
    def __init__(self, writer: Writer):
        self._writer = writer

    @property
    def writer(self):
        return self._writer

    @writer.setter
    def writer(self, writer: Writer):
        self._writer = writer

    def write_data(self) -> Callable:
        """Returns the write method of the writer class"""
        return self.writer.write
