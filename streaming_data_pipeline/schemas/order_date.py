from pyiceberg.types import StructType, NestedField, StringType, IntegerType, FloatType
from pyiceberg.schema import Schema

order_date_avro_schema = {
    "type": "record",
    "name": "order_date",
    "fields": [
        {"name": "product_id", "type": "string"},
        {"name": "quantity", "type": "int"},
        {"name": "order_date", "type": "string"},
        {"name": "order_country", "type": "string"}
    ]
}

order_date_schema_iceberg = Schema(
    NestedField(1, "product_id", StringType(), required=True),
    NestedField(2, "quantity", IntegerType(), required=True),
    NestedField(3, "order_date", StringType(), required=True),
    NestedField(4, "order_country", StringType(), required=True)
)
