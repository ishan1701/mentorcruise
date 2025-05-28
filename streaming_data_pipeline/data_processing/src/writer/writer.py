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
