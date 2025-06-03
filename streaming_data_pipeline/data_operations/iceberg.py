from loguru import logger


def drop_iceberg_table_with_pyspark(spark, namespace: str, iceberg_table: str):
    """
    Drop an Iceberg table using PySpark.
    """
    logger.info(f"Dropping table {namespace}.{iceberg_table}")
    spark.sql(f"DROP TABLE IF EXISTS {namespace}.{iceberg_table}")
    logger.info(f"Table {namespace}.{iceberg_table} dropped successfully.")
