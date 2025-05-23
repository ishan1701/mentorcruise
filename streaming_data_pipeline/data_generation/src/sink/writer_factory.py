from streaming_data_pipeline.data_generation.src.sink.file_writer import FileWriter
from streaming_data_pipeline.data_generation.src.sink.kafka_writer import KafkaWriter
from streaming_data_pipeline.data_generation.src.sink.writer import Writer
from typing import Callable


class WriterFactory:
    """
    A factory class to create writers.
    """

    @staticmethod
    def get_writer(writer_type: str) -> Writer:
        if writer_type == "file":

            return FileWriter()
        elif writer_type == "kafka":
            return KafkaWriter()
        else:
            raise ValueError(f"Unknown writer type: {writer_type}")


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