# from abc import ABC, abstractmethod
#
# class MCSparkSession:
#     def __init__(self):
#         self.spark = None
#
#     @abstractmethod
#     def create_spark_session(self):
#         """
#         Abstract method to create a Spark session.
#         This method should be implemented by subclasses.
#         """
#         pass
#
# class AvroSparkSession(MCSparkSession):
#     def create_spark_session(self):
#         """
#         Create a Spark session with Avro support.
#         """
#         from pyspark.sql import SparkSession
#         self.spark = SparkSession.builder \
#             .appName("AvroSparkSession") \
#             .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.1.2") \
#             .getOrCreate()
#         return self.spark
