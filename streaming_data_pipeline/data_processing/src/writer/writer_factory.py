from streaming_data_pipeline.data_processing.src.writer.writer import (
    Writer,
    ConsoleWriter,
    IcebergWriter,
    FileWriter
)
from pyspark.sql import DataFrame


class WriterFactory:
    @staticmethod
    def get_writer(writer_type: str) -> Writer:
        if writer_type == "console":
            return ConsoleWriter()
        elif writer_type == "iceberg":
            return IcebergWriter()
        elif writer_type == "file":
            return FileWriter()
        else:
            raise ValueError(f"Unknown writer type: {writer_type}")


# context
class WriterContext:
    def __init__(self, writer: Writer):
        self._writer = writer

    @property
    def writer(self) -> Writer:
        return self._writer

    @writer.setter
    def writer(self, writer: Writer):
        if not isinstance(writer, Writer):
            raise TypeError("Writer must be an instance of Writer class")
        self._writer = writer

    def write(self, df: DataFrame, **kwargs):
        self.writer.write(df=df, **kwargs)
