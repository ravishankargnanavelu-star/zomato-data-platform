import json
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.types import StructType
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, row_number, when
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ---------- Read schema ----------
with open("schemas/orders_schema.json") as f:
    schema = StructType.fromJson(json.load(f))

# ---------- Read Bronze ----------
df = spark.read.schema(schema).parquet("s3://zomato-bronze/orders/")

# ---------- Deduplicate (latest event) ----------
w = Window.partitionBy("order_id").orderBy(col("event_time").desc())

df = (
    df.withColumn("rn", row_number().over(w))
      .filter(col("rn") == 1)
      .drop("rn")
)

# ---------- Event-time validation ----------
df = df.filter(col("event_time") <= col("ingested_at"))

# ---------- Valid statuses ----------
valid_status = [
    "PLACED", "CONFIRMED", "PREPARED",
    "PICKED", "DELIVERED", "CANCELLED"
]

df = df.filter(col("order_status").isin(valid_status))

# ---------- Business flags ----------
df = (
    df.withColumn("is_delivered", when(col("order_status") == "DELIVERED", 1).otherwise(0))
      .withColumn("is_cancelled", when(col("order_status") == "CANCELLED", 1).otherwise(0))
)

# ---------- Net amount ----------
df = df.withColumn(
    "net_amount",
    col("order_amount") - when(col("discount").isNull(), 0).otherwise(col("discount"))
)

# ---------- Quarantine bad records ----------
bad_df = df.filter(
    (col("order_id").isNull()) |
    (col("net_amount") < 0) |
    (col("order_amount") <= 0)
)

bad_df.write.mode("append").parquet("s3://zomato-quarantine/orders/")

df = df.subtract(bad_df)

# ---------- Write Silver ----------
df.write.mode("overwrite").parquet("s3://zomato-silver/orders/")

spark.stop()
