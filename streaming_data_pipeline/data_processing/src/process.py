from pyspark.sql.functions import col, to_date
from loguru import logger


def process_data(reader, parser, writer, spark, **kwargs):
    # start reading data

    df = reader.read(spark=spark)

    parsed_df = parser.parse(
        df=df,
        spark=spark,
        column="value",
        parsed_column_name="parsed_value",
        spark_schema=kwargs["spark_schema"],
    )

    ## apply ant more transformations if needed
    transformed_df = parsed_df.withColumn("creation_date", to_date(col("timestamp")))

    transformed_df.printSchema()

    writer.write(df=transformed_df, **kwargs)
