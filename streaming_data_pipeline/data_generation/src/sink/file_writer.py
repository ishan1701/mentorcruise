from streaming_data_pipeline.data_generation.src.sink.kafka_writer import (
    KafkaWriterFactory,
)
from streaming_data_pipeline.data_generation.src.sink.writer import Writer
import loguru
from typing import Iterable
import json


class FileWriter(Writer):

    # currently, we only support file writer with json serialization format
    writer_type = "file"
    serialization_format = "json"

    def __init__(self, **kwargs):
        self.file_path = kwargs.get("file_path")
        self.encoding = kwargs["encoding"] if kwargs.get("encoding") else "utf-8"

    def write(self, data: dict | Iterable[dict]) -> None:
        with open(self.file_path, mode="a+", encoding=self.encoding) as f:
            f.write(json.dumps(data))
            f.write("\n")


class FileWriterFactory(KafkaWriterFactory):
    @staticmethod
    def get_writer(serialization_format: str = "json", **kwargs) -> FileWriter:
        if serialization_format != "json":
            loguru.logger.warning(
                "Unsupported serialization format for file writer: {serialization_format}. Defaulting to JSON."
            )
        return FileWriter(**kwargs)


# testing
if __name__ == "__main__":
    from streaming_data_pipeline.data_generation.src.models.product_sales import (
        ProductSales,
    )
    from datetime import datetime

    sample_data = ProductSales(
        product_id="sample", quantity=10, price=100.0, timestamp=datetime.now()
    )
    writer = FileWriterFactory.get_writer(serialization_format="avro", file_path="test")
    for _ in range(10):
        writer.write(sample_data.serialize())
