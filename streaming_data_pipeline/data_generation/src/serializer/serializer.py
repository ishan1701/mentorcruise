from abc import ABC, abstractmethod
import json
from fastavro import schemaless_writer
import io
from confluent_kafka.schema_registry import SchemaRegistryClient

from confluent_kafka.schema_registry.avro import AvroSerializer


class Serializer(ABC):
    @staticmethod
    @abstractmethod
    def serialize(data: dict, schema: dict | None):
        pass


class MJsonSerializer(Serializer):
    @staticmethod
    def serialize(data: dict, schema: dict | None):
        return json.dumps(data)


class MAvroSerializer(Serializer):

    @staticmethod
    def serialize(data: dict, schema: str):
        schema_registry_conf = {'url': 'http://localhost:8081'}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        if schema is None:
            raise ValueError("Schema must be provided for Avro serialization")

        avro_serializer = AvroSerializer()

        return avro_serializer





if __name__ == '__main__':
    # Example usage
    data = {"name": "John", "age": 30}
    schema = '''{
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"}
        ]
    }'''''
    data = MAvroSerializer.serialize(data, schema)
    print(data)
    print(data.__call__(obj='ddede'))

