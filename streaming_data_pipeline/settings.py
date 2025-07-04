from streaming_data_pipeline.data_generation.src.models.product_sales import (
    ProductSales,
)
from schemas.product_sales import (
    product_sales_schema_iceberg,
    product_sales_avro_schema,
    product_sales_schema_spark,
    product_sales_create_sql_iceberg_table,
)

DATA_GENERATION_MODEL = "product_sales"

# data generation settings
DATA_GENERATOR_WRITER = "kafka"
DATA_GENERATOR_TYPE = "late_arriving"
NUM_OF_RECORDS = 1000  # number of records to be generated


# data processing settings
READER_TYPE = "kafka"
READER_SERIALIZATION_FORMAT = "json"
WRITER_TYPE = "iceberg"

MODEL_MAP = {
    "product_sales": {
        "model": ProductSales,
        "avro_schema": product_sales_avro_schema,
        "iceberg_schema": product_sales_schema_iceberg,
        "spark_schema": product_sales_schema_spark,  # Spark schema can be derived from the Iceberg schema if needed
        "description": "Product sales data model and schema",
        "iceberg_table": "product_sales",
        "create_sql": product_sales_create_sql_iceberg_table,
    }
}


CONFLUENT_SCHEMA_REGISTRY_URL = "http://localhost:8081"

