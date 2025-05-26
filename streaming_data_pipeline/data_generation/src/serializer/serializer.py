# from abc import ABC, abstractmethod
# import json
# from fastavro import schemaless_writer
# import io
#
#
# class Serializer(ABC):
#     @staticmethod
#     @abstractmethod
#     def serialize(data: dict, schema: dict | None):
#         pass
#
#
# class JsonSerializer(Serializer):
#     @staticmethod
#     def serialize(data: dict, schema: dict | None):
#         return json.dumps(data)
#
#
# class AvroSerializer(Serializer):
#     @staticmethod
#     def serialize(data: dict, schema: dict | None):
#         bytes_writer = io.BytesIO()
#         schemaless_writer(bytes_writer, schema, data)
#         avro_bytes = bytes_writer.getvalue()
#         print(avro_bytes)
#         return avro_bytes
#
# #
# # if __name__ == '__main__':
# #     # Example usage
# #     data = {"name": "John", "age": 30}
# #     schema = {
# #         "type": "record",
# #         "name": "User",
# #         "fields": [
# #             {"name": "name", "type": "string"},
# #             {"name": "age", "type": "int"}
# #         ]
# #     }
# #     # data = AvroSerializer.serialize(data, schema)
# #     # print(data)
# #     import json
# #
# #     with open("test.avro", "a+") as f:
# #         json.dump(data, f)
#
#
