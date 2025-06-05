from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_conf(**kwargs) -> SparkConf:
    nessie_server_uri = kwargs["nessie_server_uri"]
    warehouse_bucket = kwargs["warehouse_bucket"]

    print(f"Using Nessie server URI: {nessie_server_uri}")
    print(f"Using warehouse bucket: {warehouse_bucket}")

    if nessie_server_uri is None or warehouse_bucket is None:
        raise ValueError("Nessie server URI and warehouse bucket must be provided.")

    spark_conf = (
        SparkConf()
        .set(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.91.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2",
        )
        # SQL Extensions
        .set(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        # Configuring Catalog
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.nessie.uri", nessie_server_uri)
        .set("spark.sql.catalog.nessie.ref", "main")
        .set("spark.sql.catalog.nessie.authentication.type", "NONE")
        .set(
            "spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        # .set("spark.sql.catalog.nessie.s3.endpoint",MINIO_URI)
        .set("spark.sql.catalog.nessie.warehouse", warehouse_bucket)
        # .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .set("spark.driver.memory", "1g")
        .set("spark.executor.memory", "1g")
        .set("spark.sql.streaming.checkpointLocation", "spark-checkpoint/checkpoint")
        .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.local.type", "hadoop")
        .set("spark.sql.catalog.local.warehouse", "file:///tmp/warehouse")
    )

    return spark_conf


def get_spark_session(master: str, conf, app_name: str) -> SparkSession:
    """
    Create and return the spark session object
    """
    return (
        SparkSession.builder.config(conf=conf)
        .appName(app_name)
        .master(master)
        .getOrCreate()
    )
