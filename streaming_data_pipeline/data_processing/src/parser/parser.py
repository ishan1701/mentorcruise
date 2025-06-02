# parser strategy for parsing data from a file
from pyspark.sql import DataFrame, SparkSession, DataFrame, Column
from loguru import logger

from abc import ABC, abstractmethod


class Parser(ABC):
    @abstractmethod
    def parse(
        self,
        spark: SparkSession,
        df,
        column: Column,
        parsed_column_name: str,
        **kwargs,
    ):
        pass

    @abstractmethod
    def convert(self, spark: SparkSession, data: DataFrame, **kwargs) -> DataFrame:
        ## future use case for converting to avro df
        pass


class AvroParser(Parser):
    def parse(
        self,
        spark: SparkSession,
        df,
        column: Column,
        parsed_column_name: str,
        **kwargs,
    ):
        # schema = kwargs.get("schema")
        schema = """{
            "type": "record",
            "name": "product_sales",
            "fields": [
                {"name": "product_id", "type": "string"},
                {"name": "quantity", "type": "int"},
                {"name": "price", "type": "double"},
                {"name": "timestamp", "type": "string"}
            ]
        }"""

        if not schema:
            logger.error("Schema is required for Avro parsing")
            raise Exception("Schema is required for Avro parsing")
        logger.info(f"Parsing data using AvroParser with schema: {schema}")
        from pyspark.sql.avro.functions import from_avro

        parsed_df = df.withColumn(
            parsed_column_name, from_avro("value", schema)
        ).select(f"{parsed_column_name}.*")
        return parsed_df

    def convert(self, spark: SparkSession, data: DataFrame, **kwargs) -> DataFrame:
        pass


class JsonParser(Parser):
    def parse(
        self,
        spark: SparkSession,
        df,
        column: Column,
        parsed_column_name: str,
        **kwargs,
    ):
        from pyspark.sql.functions import from_json
        df.printSchema()
        logger.info(f"Parsing data using JsonParser with column: {column}")

        spark_schema = kwargs.get("spark_schema")
        if not spark_schema:
            logger.error("Schema is required for JSON parsing")
            raise Exception("Schema is required for JSON parsing")

        logger.info(f"Parsing data using JsonParser with spark_schema: {spark_schema}")
        json_df = df.selectExpr(f"CAST({column} AS STRING) as value")

        json_df.printSchema()

        parsed_df_with_column = json_df.withColumn(
            parsed_column_name, from_json('value', spark_schema)
        ).select(f"{parsed_column_name}.*")

        parsed_df_with_column.printSchema()

        return parsed_df_with_column

    def convert(self, spark: SparkSession, data: DataFrame, **kwargs) -> DataFrame:
        pass
