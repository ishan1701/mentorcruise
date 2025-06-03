import pyiceberg.types as pyiceberg_types
from pyiceberg.schema import Schema
import pyspark.sql.types as pyspark_types

product_sales_avro_schema = {
    "type": "record",
    "name": "product_sales",
    "fields": [
        {"name": "product_id", "type": "string"},
        {"name": "quantity", "type": "int"},
        {"name": "price", "type": "float"},
        {"name": "timestamp", "type": "string"},
    ],
}

product_sales_schema_iceberg = Schema(
    pyiceberg_types.NestedField(
        1, "product_id", pyiceberg_types.StringType(), required=True
    ),
    pyiceberg_types.NestedField(
        2, "quantity", pyiceberg_types.IntegerType(), required=True
    ),
    pyiceberg_types.NestedField(3, "price", pyiceberg_types.FloatType(), required=True),
    pyiceberg_types.NestedField(
        4, "timestamp", pyiceberg_types.StringType(), required=True
    ),
)

product_sales_schema_spark = pyspark_types.StructType(
    [
        pyspark_types.StructField("product_id", pyspark_types.StringType(), True),
        pyspark_types.StructField("quantity", pyspark_types.IntegerType(), True),
        pyspark_types.StructField("price", pyspark_types.DoubleType(), True),
        pyspark_types.StructField("timestamp", pyspark_types.StringType(), True),
    ]
)

product_sales_create_sql_iceberg_table = """
                           CREATE TABLE IF NOT EXISTS product_sales
                           (
                               product_id STRING,
                               quantity INT,
                               price FLOAT,
                               `timestamp` STRING
                           ) USING iceberg \
                           """.strip()
