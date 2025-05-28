from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType

product_sales = StructType([
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("timestamp", StringType(), True)
])