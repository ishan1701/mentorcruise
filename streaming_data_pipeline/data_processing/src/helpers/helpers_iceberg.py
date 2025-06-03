from pyiceberg.catalog import load_catalog
from pyiceberg.catalog import Catalog
from loguru import logger
from pyiceberg.schema import Schema


def if_iceberg_table_exists(catalog: Catalog, table_name: str, namespace: str) -> bool:
    identifier = catalog.list_tables(namespace)
    for entry in identifier:
        if entry[1] == table_name:
            return True
    return False


def get_iceberg_catalog(catalog: str) -> Catalog:
    try:
        catalog = load_catalog(catalog)
        return catalog
    except Exception as e:
        raise ValueError(f"Failed to load Iceberg catalog '{catalog}': {e}")


def list_namespaces(catalog: Catalog):
    """List all namespaces in the Iceberg catalog."""
    namespaces = catalog.list_namespaces()
    for ns in namespaces:
        logger.info(f"Namespace: {ns[0]}")
    return namespaces


def create_namespace(catalog: Catalog, namespace: str):
    for ns in catalog.list_namespaces():
        if ns[0] == namespace:
            logger.info(f"Namespace {namespace} already exists")
            return
    catalog.create_namespace_if_not_exists(namespace)


def create_table(catalog: Catalog, namespace: str, table_name: str, schema: Schema):
    """Create an Iceberg table if it does not exist."""

    if if_iceberg_table_exists(catalog, table_name, namespace):
        logger.info(f"Table {table_name} already exists in namespace {namespace}")
        return
    logger.info(f"Creating table {table_name}")

    catalog.create_table(
        identifier=(namespace, table_name),
        schema=schema,
        properties={"namespace": namespace},
    )


def list_tables(catalog: Catalog, namespace: str):
    """List all tables in a given namespace."""

    tables = catalog.list_tables(namespace=namespace)
    for table in tables:
        logger.info(f"Table: {table}")


def create_table_with_pyspark(
    spark, namespace: str, iceberg_table: str, create_sql: str
):

    logger.info(f"executing  {create_sql.format(namespace=namespace)}")
    spark.sql(f"{create_sql.format(namespace=namespace)}")
    spark.sql(f"describe table formatted {namespace}.{iceberg_table}").show()


#
# import pyspark
# from pyspark.sql import SparkSession
# import os
#
# print(pyspark.__version__)
#
# os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
#
# ## DEFINE SENSITIVE VARIABLES
# NESSIE_SERVER_URI = "http://localhost:19120/api/v2"
# # WAREHOUSE_BUCKET = "s3://warehouse"
# # MINIO_URI = "http://172.19.0.2:9000"
#
# WAREHOUSE_BUCKET = "/tmp/warehouse"  # Local path for testing purposes
#
# ## Configurations for Spark Session
# conf = (
#     pyspark.SparkConf()
#         .setAppName('app_name')
#   		#packages
#         .set('spark.jars.packages',
#              'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.91.3')
#         # SQL Extensions
#         .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
#   		#Configuring Catalog
#         .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
#         .set('spark.sql.catalog.nessie.uri', NESSIE_SERVER_URI)
#         .set('spark.sql.catalog.nessie.ref', 'main')
#         .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
#         .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
#         # .set("spark.sql.catalog.nessie.s3.endpoint",MINIO_URI)
#         .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE_BUCKET)
#         # .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
# )
#
# ## Start Spark Session
# spark = SparkSession.builder.config(conf=conf).getOrCreate()
# print("Spark Running")
# print(spark)
#
# ## TEST QUERY TO CHECK IT WORKING
# ### Create TABLE
# # spark.sql("CREATE TABLE nessie.example (name STRING) USING iceberg;").show()
# # ### INSERT INTO TABLE
# spark.sql("INSERT INTO nessie.example VALUES ('Addd');").show()
# ### Query Table
# spark.sql("describe table formatted nessie.example;").show()
#
# spark.sql("SELECT * FROM nessie.example;").show()


# local testing
# if __name__ == '__main__':
#     # catalog = load_catalog(
#     #     "nessie",
#     #     **{
#     #         "uri": "http://localhost:19120/iceberg/v1/",
#     #         "ref": "main",
#     #         "authentication.type": "NONE",
#     #         "warehouse": "file:///tmp/warehouse",  # Local path for testing purposes
#     #     }
#     # )
#     nessie_uri = "http://localhost:19120/iceberg"
#     catalog = load_catalog(
#         "rest",
#         uri=f"{nessie_uri}|local_warehouse",
#         warehouse="/var/nessie_warehouse_data"
#     )
#
#     # Verify connection by listing namespaces
#     namespaces = catalog.list_namespaces()
#     print("Namespaces:", namespaces)

# catalog = get_iceberg_catalog("nessie")
# create_namespace(catalog, "mentor")
# from pyiceberg.types import IntegerType, StringType
# from pyiceberg.schema import Schema, NestedField
#
# schema = Schema(
#     NestedField(1, "id", IntegerType()),
#     NestedField(2, "name", StringType())
# )
# create_table(catalog, "mentor",table_name='sample1',schema= schema)
# print(if_iceberg_table_exists(catalog, "sample1", namespace="mentor"))
