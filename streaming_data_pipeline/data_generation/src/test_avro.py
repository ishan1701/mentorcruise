import json
from fastavro import reader


def avro_to_json(avro_file_path, json_file_path):
    with open(avro_file_path, "rb") as avro_file:
        avro_reader = reader(avro_file)
        records = list(avro_reader)  # List of dicts

    with open(json_file_path, "w") as json_file:
        json.dump(records, json_file, indent=2)


# Usage
avro_to_json("data.avro", "data.json")
