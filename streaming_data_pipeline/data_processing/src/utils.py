# utils functions for main module

import yaml
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.storagelevel import StorageLevel


def load_yaml_file(file_path: Path) -> dict:
    """
    Load yaml file
    """
    with open(file_path, "r") as file:
        return yaml.safe_load(file)


def get_spark_session(master: str, conf, app_name: str) -> SparkSession:
    """
    Create and return the spark session object
    """
    return (
        SparkSession.builder.config(conf=conf)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.spark:spark-avro_2.12:3.5.1",
        )
        .appName(app_name)
        .master(master)
        .getOrCreate()
    )


def repartition_spark_dataframe(
    dataframe: DataFrame, num_partitions: int, partition_columns: list | None = None
) -> DataFrame:
    """
    Repartition spark dataframe with either num_partitions strategy or based on columns list
    """
    if partition_columns is not None:
        df: DataFrame = dataframe.repartition(*partition_columns)
    else:
        df: DataFrame = dataframe.repartition(numPartitions=num_partitions)
    # run the action to trigger the Spark DAG
    df.show(1)
    return df


def coalesce_spark_dataframe(dataframe: DataFrame, num_partitions: int) -> DataFrame:
    """
    Coalesce spark dataframe if required
    """
    df = dataframe.coalesce(num_partitions)
    # run the action to trigger the Spark DAG
    df.show(1)
    return df


def persist_spark_dataframe(
    dataframe: DataFrame, storage_level: StorageLevel
) -> DataFrame:
    """
    Persist spark dataframe with the specified storage level
    """
    df = dataframe.persist(storage_level)
    # run the action to trigger the Spark DAG
    df.show(1)
    return df
