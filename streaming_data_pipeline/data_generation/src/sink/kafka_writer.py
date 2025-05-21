# TO DO. Add the derived class for Kafka writer
from streaming_data_pipeline.data_generation.src.sink.writer import Writer
from typing import Iterable

class KafkaWriter(Writer):
    @staticmethod
    def write(data: dict | Iterable[dict], schema: dict, **kwargs) -> None:
        pass