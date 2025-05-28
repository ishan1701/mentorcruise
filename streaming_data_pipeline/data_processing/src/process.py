from types import NoneType

import loguru
from pyspark.sql.column import Column
from pyspark.sql import DataFrame
from streaming_data_pipeline.data_processing.schemas.product_sales import product_sales


def tesr(df: DataFrame) -> DataFrame:
    return df


def process_data(reader, parser, writer, spark):
    # start reading data

    df = reader.read(spark=spark)

    parsed_df = parser.parse(
        df=df,
        spark=spark,
        column="value",
        parsed_column_name="parsed_value",
        schema=product_sales,
    )

    ## apply ant more transformations if needed

    writer.write(df=parsed_df)
