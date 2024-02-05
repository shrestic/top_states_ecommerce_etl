import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    DateType,
)


def perform_sql(spark: SparkSession, bucket_name: str) -> None:
    order_schema = StructType(
        [
            StructField("order_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("status", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("created_at", DateType(), True),
            StructField("returned_at", DateType(), True),
            StructField("shipped_at", DateType(), True),
            StructField("delivered_at", DateType(), True),
            StructField("num_of_item", IntegerType(), True),
        ]
    )
    user_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
            StructField("state", StringType(), True),
            StructField("street_address", StringType(), True),
            StructField("postal_code", IntegerType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True),
            StructField("traffic_source", StringType(), True),
            StructField("created_at", DateType(), True),
        ]
    )

    order_df = spark.read.schema(order_schema).csv("/raw-data-stage/order", header=True)
    user_df = spark.read.schema(user_schema).csv("/raw-data-stage/user", header=True)

    user_df.createOrReplaceTempView("users")
    order_df.createOrReplaceTempView("orders")

    states_user_order_activity = spark.sql(
        """SELECT u.state,
       COUNT(DISTINCT u.id) AS user_count,
       COUNT(*) AS order_count,
       COUNT(DISTINCT u.id) + COUNT(*) AS combined_count
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE u.country  = 'United States'
GROUP BY u.state
ORDER BY combined_count DESC;"""
    )

    states_user_order_activity = (
        states_user_order_activity.withColumn("state", col("state").cast(StringType()))
        .withColumn("user_count", col("user_count").cast(IntegerType()))
        .withColumn("order_count", col("order_count").cast(IntegerType()))
        .withColumn("combined_count", col("combined_count").cast(IntegerType()))
    )
    states_user_order_activity.repartition(1).write.parquet(
        "s3://" + bucket_name + "/transformed-data/", mode="overwrite"
    )

    spark.catalog.dropTempView("users")

    spark.catalog.dropTempView("orders")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket_name")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Spark Transform Data").getOrCreate()

    perform_sql(spark=spark, bucket_name=args.bucket_name)
