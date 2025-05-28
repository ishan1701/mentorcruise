import loguru

from streaming_data_pipeline.data_processing.src.reader.reader import (
    KafkaReader,
    Reader,
)
from typing import Callable
from pyspark.sql import DataFrame


# define enum for factory


class ReaderFactory:
    @staticmethod
    def get_reader(reader_type: str, reader_config) -> Reader:
        for k, v in reader_config.items():
            loguru.logger.info(f"{k} : {v}")
        if reader_type == "kafka":
            return KafkaReader(
                topic=reader_config.get("topic"),
                bootstrap_servers=reader_config.get("bootstrap_servers"),
            )

        raise ValueError(f"Unsupported reader type: {reader_type}")


# context


class ReaderContext:
    def __init__(self, reader: Reader):
        self._reader = reader

    @property
    def reader(self) -> Reader:
        return self._reader

    @reader.setter
    def reader(self, reader: Reader):
        if not isinstance(reader, Reader):
            raise TypeError("Reader must be an instance of Reader class")
        self._reader = reader

    def read(self, spark) -> DataFrame:
        return self.reader.read(spark)
