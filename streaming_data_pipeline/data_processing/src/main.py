# create a context for parser
# create a context for writer
from parser.parser_factory import ParserFactory, ParserContext
from reader.reader_factory import ReaderFactory, ReaderContext
from writer.writer_factory import WriterFactory, WriterContext
from setting import READER_TYPE, READER_SERIALIZATION_FORMAT, WRITER_TYPE
from utils import load_yaml_file
from pathlib import Path
from utils import get_spark_session
from pyspark import SparkConf
from process import process_data
import os
from loguru import logger


def main():

    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    config = load_yaml_file(Path(__file__).parent / "config.yaml")
    reader_config = config.get("reader")
    writer_config = config.get("writer")
    logger.info(f"Reader config: {reader_config}")
    logger.info(f"Writer config: {writer_config}")

    # reader context
    reader = ReaderFactory.get_reader(
        reader_type=READER_TYPE, reader_config=reader_config[READER_TYPE]
    )
    reader_context = ReaderContext(reader=reader)

    # parser context
    parser = ParserFactory.get_parser(parser_type=READER_SERIALIZATION_FORMAT)
    parser_context = ParserContext(parser=parser)

    # #writer context
    writer = WriterFactory.get_writer(writer_type=WRITER_TYPE)
    writer_context = WriterContext(writer=writer)

    # master: str, conf, app_name: str
    spark_conf = (
        SparkConf()
        .set("spark.driver.memory", "1g")
        .set("spark.executor.memory", "1g")
        # .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .set("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint")
    )
    # .set("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.1"))

    spark = get_spark_session(
        master="local[*]", app_name="mentor_cruise_app", conf=spark_conf
    )
    process_data(
        reader=reader_context, parser=parser_context, writer=writer_context, spark=spark
    )


if __name__ == "__main__":
    main()
