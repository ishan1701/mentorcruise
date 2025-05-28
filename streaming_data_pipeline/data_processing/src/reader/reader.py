from abc import ABC, abstractmethod

import loguru
from pyspark.sql import SparkSession, DataFrame


class Reader(ABC):
    @abstractmethod
    def read(self, spark: SparkSession) -> DataFrame:
        pass


class KafkaReader(Reader, ABC):

    def __init__(
        self, topic: str, bootstrap_servers: str, starting_offsets: str = "latest"
    ):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.starting_offsets = starting_offsets

    def read(self, spark: SparkSession) -> DataFrame:
        """
        Read data from Kafka: topic using Spark Structured Streaming.
        Currently, it reads the data as a DataFrame and returns it.
        """
        loguru.logger.info("Reading data from Kafka")
        loguru.logger.info(
            f"Topic: {self.topic}, Bootstrap Servers: {self.bootstrap_servers}, Starting Offsets: {self.starting_offsets}"
        )
        df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", self.starting_offsets)
            .load()
        )
        return df
