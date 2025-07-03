from streaming_data_pipeline.utils import get_spark_conf, get_spark_session
import os

nessie_server_uri = "http://localhost:19120/api/v2"
warehouse_bucket = "/tmp/warehouse"
namespace = "nessie"

from iceberg import (
    drop_iceberg_table_with_pyspark,
)


def main():
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    spark = get_spark_session(
        master="local[*]",
        app_name="streaming_data_pipeline_app",
        conf=get_spark_conf(
            nessie_server_uri=nessie_server_uri, warehouse_bucket=warehouse_bucket
        ),
    )
    print(f"Spark version: {spark.version}")

    # Drop the Iceberg table
    drop_iceberg_table_with_pyspark(
        spark, namespace=namespace, iceberg_table="product_sales"
    )


if __name__ == "__main__":
    main()
