# create a context for parser
# create a context for writer
from streaming_data_pipeline.data_processing.src.parser.parser_factory import (
    ParserFactory,
    ParserContext,
)
from streaming_data_pipeline.data_processing.src.reader.reader_factory import (
    ReaderFactory,
    ReaderContext,
)
from streaming_data_pipeline.data_processing.src.writer.writer_factory import (
    WriterFactory,
    WriterContext,
)
from streaming_data_pipeline.settings import (
    READER_TYPE,
    READER_SERIALIZATION_FORMAT,
    WRITER_TYPE,
)
from streaming_data_pipeline.data_processing.src.utils import load_yaml_file
from pathlib import Path
from streaming_data_pipeline.utils import get_spark_session
from streaming_data_pipeline.data_processing.src.process import process_data
import os
from loguru import logger
from streaming_data_pipeline.data_processing.src.helpers.helpers_iceberg import (
    create_table_with_pyspark,
)
from streaming_data_pipeline.utils import get_spark_conf


def main(model):
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    config = load_yaml_file(Path(__file__).parent / "config.yaml")
    reader_config = config.get("reader")
    writer_config = config.get("writer")
    logger.info(f"Reader config: {reader_config}")
    logger.info(f"Writer config: {writer_config}")
    # get the model configuration. This is imoetant for integration
    # model_config = MODEL_MAP

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
    nessie_server_uri = writer_config[WRITER_TYPE]["nessie_server_uri"]
    warehouse_bucket = writer_config[WRITER_TYPE]["warehouse_bucket"]

    spark = get_spark_session(
        master="local[*]",
        app_name="mentor_cruise_app",
        conf=get_spark_conf(
            nessie_server_uri=nessie_server_uri, warehouse_bucket=warehouse_bucket
        ),
    )

    writer_kwargs = dict()

    if WRITER_TYPE == "iceberg":
        writer_kwargs["namespace"] = writer_config[WRITER_TYPE]["namespace"]
        writer_kwargs["iceberg_table"] = model["iceberg_table"]
        writer_kwargs["create_sql"] = model["create_sql"]
        create_table_with_pyspark(spark=spark, **writer_kwargs)

        # from streaming_data_pipeline.data_processing.src.helpers.helpers_iceberg import create_table_with_pyspark
        #
        # catalog = writer_config[WRITER_TYPE]['catalog']
        # namespace = writer_config[WRITER_TYPE]['namespace']
        #
        # from helpers.helpers_iceberg import get_iceberg_catalog, create_namespace, create_table
        #
        # catalog = get_iceberg_catalog(catalog=catalog)
        # create_namespace(catalog=catalog, namespace=namespace)
        # create_table(catalog=catalog,
        #              namespace=namespace,
        #              table_name=model['iceberg_table'],
        #              schema=model['iceberg_schema'])
        # writer_kwargs['catalog'] = catalog
        # writer_kwargs['namespace'] = namespace
        # writer_kwargs['iceberg_table'] = model['iceberg_table']

    elif WRITER_TYPE == "file":
        format = writer_config[WRITER_TYPE]["format"]
        path = writer_config[WRITER_TYPE]["path"]
        partition_by = writer_config[WRITER_TYPE]["partition_by"]

        writer_kwargs["format"] = format
        writer_kwargs["path"] = path
        writer_kwargs["partition_by"] = partition_by

    process_data(
        reader=reader_context,
        parser=parser_context,
        writer=writer_context,
        spark=spark,
        spark_schema=model["spark_schema"],
        **writer_kwargs,
    )


if __name__ == "__main__":
    logger.info("Starting the streaming data pipeline...")

    # hardcoded for now, but can be extended to take command line arguments via click
    from streaming_data_pipeline.settings import DATA_GENERATION_MODEL, MODEL_MAP

    model_det = MODEL_MAP.get(DATA_GENERATION_MODEL)
    if not model_det:
        raise ValueError(f"Model {DATA_GENERATION_MODEL} not found in MODEL_MAP")

    main(model=model_det)
