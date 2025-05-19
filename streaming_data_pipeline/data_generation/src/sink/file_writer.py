from writer import Writer
import fastavro
from typing import Iterable


class FileWriter(Writer):
    @staticmethod
    def write(data: dict | Iterable[dict], schema: dict, **kwargs) -> None:
        """
        Write data to a file.
        """
        with open(kwargs["file_path"], "ab") as file_out:
            if isinstance(data, dict):
                data = [data]
            else:
                data = list(data)
            fastavro.writer(file_out, schema=schema, records=data)
