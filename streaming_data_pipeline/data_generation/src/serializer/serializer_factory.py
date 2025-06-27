from serializer import Serializer, MJsonSerializer as JsonSerializer, MAvroSerializer
from typing import Callable


class SerializerFactory:
    @staticmethod
    def get_serializer(serializer_type: str) -> Callable:
        if serializer_type == "json":
            return JsonSerializer.serialize
        elif serializer_type == "avro":
            return MAvroSerializer.serialize
        else:
            raise ValueError(f"Unknown serializer type: {serializer_type}")


class Context:
    def __init__(self, serializer: Serializer):
        self._serializer = serializer

    @property
    def serializer(self):
        return self._serializer

    @serializer.setter
    def serializer(self, serializer: Serializer):
        self._serializer = serializer

    def serialize_data(self) -> Callable:
        return self.serializer.serialize
