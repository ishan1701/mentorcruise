# TO DO. Add the derived class for Kafka writer
import loguru

from streaming_data_pipeline.schemas.product_sales import product_sales_avro_schema
from streaming_data_pipeline.data_generation.src.sink.writer import Writer
from typing import Iterable
from abc import abstractmethod
from confluent_kafka import Producer
import fastavro
import json
from abc import ABC


class KafkaWriter(Writer, ABC):
    writer_type = "kafka"

    def __init__(self, kafka_topic: str, kafka_bootstrap_servers: str, **kwargs):
        self.kafka_topic = kafka_topic
        self.encoding = kwargs["encoding"] if kwargs.get("encoding") else "utf-8"
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.producer = Producer({"bootstrap.servers": f"{kafka_bootstrap_servers}"})

    def flush(self) -> None:
        self.producer.flush()


class KafkaAvroWriter(KafkaWriter):
    serialization_format = "avro"

    def __init__(self, **kwargs):
        # lets define the pydantic models or better to use assert
        self.schema = kwargs["schema"]
        super().__init__(
            kafka_topic=kwargs["topic"], kafka_bootstrap_servers=kwargs["brokers"]
        )

    def _serialize(self, data: dict) -> bytes:
        import io

        with io.BytesIO() as buffer:
            fastavro.writer(buffer, self.schema, [data])
            return buffer.getvalue()

    def write(self, data: dict | Iterable[dict], **kwargs) -> None:
        print(data)

        if isinstance(data, dict):
            data = [data]
        for record in data:
            serialized_record = self._serialize(record)
            self.producer.produce(self.kafka_topic, value=serialized_record)
            ## think on how to handle flush
        self.producer.flush()


class KafkaJsonWriter(KafkaWriter):
    serialization_format = "json"

    def __init__(self, **kwargs):
        # lets define the pydantic models
        super().__init__(
            kafka_topic=kwargs["topic"], kafka_bootstrap_servers=kwargs["brokers"]
        )

    def write(self, data: dict | Iterable[dict]) -> None:

        if isinstance(data, dict):
            data = [data]
        for record in data:
            self.producer.produce(
                self.kafka_topic, value=json.dumps(record).encode(self.encoding)
            )
            # think on how to handle flush
        self.producer.flush()


class KafkaWriterFactory:
    @staticmethod
    def get_writer(serialization_format, **kwargs) -> KafkaWriter:
        loguru.logger.info(
            f"Creating Kafka writer with serialization format: {serialization_format}"
        )
        if serialization_format == "avro":
            return KafkaAvroWriter(**kwargs)
        elif serialization_format == "json":

            return KafkaJsonWriter(**kwargs)
        else:
            raise ValueError(
                f"Unsupported serialization format: {serialization_format}"
            )


# testing
#
if __name__ == '__main__':
    from streaming_data_pipeline.data_generation.src.models.product_sales import ProductSales
    from datetime import datetime
    sample_data = ProductSales(
        product_id='eede',
        quantity=10,
        price=100.0,
        timestamp= datetime.now()
    )
    writer = KafkaWriterFactory.get_writer(serialization_format='json', topic='mentor_cruise', brokers='localhost:9092', schema=product_sales_avro_schema)
    for _ in range(10):
        writer.write(data=sample_data.serialize())

