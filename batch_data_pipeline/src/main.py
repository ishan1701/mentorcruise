from batch_data_pipeline.utils import get_spark_conf, get_spark_session
import os
from loguru import logger

nessie_server_uri = "http://localhost:19120/api/v2"
warehouse_bucket = "/tmp/warehouse"


# daily product sales totals, late record counts.


def get_daily_product_sales_totals(spark, iceberg_table):
    """
    Calculate daily product sales totals from the Iceberg table.
    """
    logger.info(f"loaded df records from {iceberg_table}")

    df = spark.read.format("iceberg").load(iceberg_table)
    df.createOrReplaceTempView("product_sales")

    spark.sql(
        "select count(1), creation_date from product_sales group by creation_date"
    ).show()


def get_late_arriving_records(spark, iceberg_table):
    pass


def main(iceberg_table):
    spark_conf = get_spark_conf(
        nessie_server_uri=nessie_server_uri, warehouse_bucket=warehouse_bucket
    )
    spark = get_spark_session(
        master="local[*]", conf=spark_conf, app_name="batch_data_pipeline_app"
    )

    get_daily_product_sales_totals(spark, iceberg_table)


if __name__ == "__main__":
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    iceberg_table = "nessie.product_sales"
    main(iceberg_table=iceberg_table)
