from streaming_data_pipeline.data_generation.src.sink.kafka_writer import KafkaWriterFactory
from streaming_data_pipeline.data_generation.src.sink.file_writer import FileWriterFactory
from typing import Callable
from streaming_data_pipeline.data_generation.src.sink.writer import Writer


class WriterFactory:
    """
    A factory class to create writers.
    """

    @staticmethod
    def get_writer(writer_type: str, **kwargs) -> Writer:
        if writer_type == "file":
            return FileWriterFactory.get_writer(**kwargs)

        elif writer_type == "kafka":
            return KafkaWriterFactory.get_writer(**kwargs)
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