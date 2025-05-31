from abc import abstractmethod, ABC
from pyspark.sql import SparkSession, DataFrame
import loguru


class Writer(ABC):
    @abstractmethod
    def write(self, spark: SparkSession, df: DataFrame, **kwargs):
        pass


class ConsoleWriter(Writer):
    def write(self, df: DataFrame, **kwargs):
        """
        Write DataFrame to console.
        """
        loguru.logger.info(f"the type is {type(df)}")

        loguru.logger.info("Writing DataFrame to console")
        query = df.writeStream.format("console").outputMode("append").start()
        query.awaitTermination()


class IcebergWriter(Writer):

    # def create_table(self, spark: SparkSession, table_name: str, catalog: str, db_name: str):
    #     """
    #     Create Iceberg table if it does not exist.
    #     """
    #     loguru.logger.info(f"Creating Iceberg table {table_name} in catalog {catalog} and database {db_name}")
    #     spark.sql(f"""
    #         CREATE TABLE IF NOT EXISTS {catalog}.{db_name}.{table_name} (
    #             id INT,
    #             name STRING,
    #             value DOUBLE
    #         )
    #         USING iceberg
    #     """)
    #     loguru.logger.info(f"Iceberg table {table_name} created successfully")

    def write(self, spark: SparkSession, df: DataFrame, **kwargs):
        """
        Write DataFrame to Iceberg table.
        """
        catalog = kwargs.get("catalog")
        db_name = kwargs.get("db")
        table_name = kwargs.get("table_name")

        if not table_name or not catalog or not db_name:
            loguru.logger.error(
                f"Missing required parameters: table_name, catalog, or db"
            )
            raise ValueError("Missing required parameters: table_name, catalog, or db")

        loguru.logger.info(f"Writing DataFrame to Iceberg table: {table_name}")
        df.writeTo(f"{catalog}.{db_name}.{table_name}").append()
