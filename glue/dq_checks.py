from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read.parquet("s3://zomato-silver/orders/")

assert df.filter(col("order_id").isNull()).count() == 0
assert df.filter(col("event_time").isNull()).count() == 0
assert df.filter(col("net_amount") < 0).count() == 0

spark.stop()
