from streaming_data_pipeline.data_generation.src.models.product_sales import ProductSales
from streaming_data_pipeline.data_generation.schemas.product_sales import product_sales

MODEL_SCHEMA_MAP = {
    "product_sales": {
        "model": ProductSales,
        "schema": product_sales,
    }
}
