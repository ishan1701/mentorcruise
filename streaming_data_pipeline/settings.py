from streaming_data_pipeline.data_generation.src.models.product_sales import ProductSales
from streaming_data_pipeline.data_generation.schemas.product_sales import product_sales


DATA_GENERATOR_WRITER = "kafka"
DATA_GENERATOR_TYPE = "streaming"
NUM_OF_RECORDS = 1000  # number of records to be generated

READER_TYPE = "kafka"
READER_SERIALIZATION_FORMAT = "json"
WRITER_TYPE = "iceberg"

MODEL_MAP = {
    "product_sales": {
        "model": ProductSales,
        "schema": product_sales,
        "description": "Product sales data model and schema",
        "iceberg_table": "product_sales"
    }
}

DATA_GENERATION_MODEL = "product_sales"
