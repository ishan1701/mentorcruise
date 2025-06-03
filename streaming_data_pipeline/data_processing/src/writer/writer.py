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

    def write(self, df: DataFrame, **kwargs):
        """
        Write DataFrame to Iceberg table.
        """
        # catalog = kwargs.get("catalog")
        # namespace = kwargs.get("namespace")
        # table_name = kwargs.get("iceberg_table")
        # print(df.schema)
        #
        # if not table_name or not catalog or not namespace:
        #     loguru.logger.error(
        #         f"Missing required parameters: table_name, catalog, or db"
        #     )
        #     raise ValueError("Missing required parameters: table_name, catalog, or db")

        # loguru.logger.info(f"Writing DataFrame to Iceberg table: {table_name}")
        namespace= kwargs['namespace']
        iceberg_table = kwargs['iceberg_table']
        processing_time = kwargs.get('processing_time', '10 seconds')


        query = df.writeStream.format('iceberg').outputMode('append').option("path", f"{namespace}.{iceberg_table}").trigger(processingTime=f"{processing_time}").start()
        query.awaitTermination()

class FileWriter(Writer):
    def write(self, df: DataFrame, **kwargs):
        """
        Write DataFrame to a file.
        """
        format = kwargs.get("format", "parquet")
        dir_path = kwargs.get("path")
        mode = kwargs.get("mode", "append")
        partition_col = kwargs.get("partition_by")

        query = df.writeStream.format(format).partitionBy(partition_col).outputMode(mode).option("path", dir_path).start()
        query.awaitTermination()