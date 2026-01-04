import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import input_file_name, current_timestamp

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ["SOURCE_PATH", "TARGET_PATH"])

df = spark.read.option("multiLine", "true").json(args["SOURCE_PATH"])

df = (
    df.withColumn("source_file", input_file_name())
      .withColumn("ingested_at", current_timestamp())
)

df.write.mode("append").parquet(args["TARGET_PATH"])

spark.stop()
