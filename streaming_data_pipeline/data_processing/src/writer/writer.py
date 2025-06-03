from abc import abstractmethod, ABC
from pyspark.sql import SparkSession, DataFrame
import loguru


class Writer(ABC):
    @abstractmethod
    def write(self, spark: SparkSession, df: DataFrame, **kwargs):
        pass

    def print_df_schema(self, df: DataFrame):
        """
        Print the schema of the DataFrame.
        """
        loguru.logger.info("DataFrame Schema:")
        df.printSchema()


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
        namespace = kwargs["namespace"]
        iceberg_table = kwargs["iceberg_table"]
        processing_time = kwargs.get("processing_time", "10 seconds")

        self.print_df_schema(df)

        loguru.logger.info(
            f"Iceberg table is {namespace}.{namespace}.{iceberg_table} with processing time {processing_time}"
        )

        query = (
            df.writeStream.format("iceberg")
            .outputMode("append")
            .trigger(processingTime=f"{processing_time}")
            .toTable(f"{namespace}.{iceberg_table}")
        )
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

        query = (
            df.writeStream.format(format)
            .partitionBy(partition_col)
            .outputMode(mode)
            .option("path", dir_path)
            .start()
        )
        query.awaitTermination()
