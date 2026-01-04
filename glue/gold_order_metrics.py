from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import (
    to_date,
    count,
    sum,
    avg,
    when
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ---------------------------------------------------
# READ SILVER DATA
# ---------------------------------------------------
df = spark.read.parquet("s3://zomato-silver/orders/")

# ---------------------------------------------------
# DERIVE DATE
# ---------------------------------------------------
df = df.withColumn("order_date", to_date("event_time"))

# ---------------------------------------------------
# GOLD: CITY-LEVEL BUSINESS METRICS
# ---------------------------------------------------
gold_df = (
    df.groupBy("order_date", "city")
      .agg(
          # Volume metrics
          count("*").alias("total_orders"),
          sum("is_delivered").alias("delivered_orders"),
          sum("is_cancelled").alias("cancelled_orders"),

          # Revenue metrics (business rule: only delivered orders)
          sum(
              when(df.is_delivered == 1, df.net_amount).otherwise(0)
          ).alias("delivered_revenue"),

          # Gross revenue (before cancellations)
          sum("net_amount").alias("gross_revenue"),

          # Average order value (AOV)
          avg(
              when(df.is_delivered == 1, df.net_amount)
          ).alias("avg_order_value")
      )
)

# ---------------------------------------------------
# DERIVED BUSINESS KPIs
# ---------------------------------------------------
gold_df = (
    gold_df
    .withColumn(
        "cancel_rate",
        when(gold_df.total_orders > 0,
             gold_df.cancelled_orders / gold_df.total_orders
        ).otherwise(0)
    )
    .withColumn(
        "delivery_success_rate",
        when(gold_df.total_orders > 0,
             gold_df.delivered_orders / gold_df.total_orders
        ).otherwise(0)
    )
    .withColumn(
        "revenue_per_order",
        when(gold_df.delivered_orders > 0,
             gold_df.delivered_revenue / gold_df.delivered_orders
        ).otherwise(0)
    )
)

# ---------------------------------------------------
# WRITE GOLD (BI READY)
# ---------------------------------------------------
gold_df.write \
    .mode("overwrite") \
    .partitionBy("order_date") \
    .parquet("s3://zomato-gold/daily_city_metrics/")

spark.stop()
