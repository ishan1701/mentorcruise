from streaming_data_pipeline.data_generation.src.sink.writer import Writer
import fastavro
from typing import Iterable


class FileWriter(Writer):
    @staticmethod
    def write(data: dict | Iterable[dict], avro_schema: dict, **kwargs) -> None:
        print("Writing data to file...")
        with open(kwargs["file_path"], "a+") as file_out:
            if isinstance(data, dict):
                data = [data]
            else:
                data = list(data)
            fastavro.writer(file_out, schema=avro_schema, records=data)
