import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType


def perform_sql(spark: SparkSession, bucket_name: str) -> None:
    spark.catalog.setCurrentDatabase("e_commerce_database")

    user_info_with_order_df = spark.sql(
        "select * from order join user on order.user_id = user.id"
    )

    user_info_with_order_df.createOrReplaceTempView("user_info_with_order")

    user_behavior_df = spark.sql(
        "select age, count(*) AS order_count from user_info_with_order group by age order by order_count desc"
    )

    user_behavior_df = user_behavior_df.withColumn("age", col("age").cast(
        IntegerType())).withColumn("order_count", col("order_count").cast(IntegerType()))

    user_behavior_df.repartition(1).write.parquet(
        "s3://" + bucket_name + "/clean_data/", mode="overwrite"
    )

    spark.catalog.dropTempView("user_info_with_order")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket_name')
    args = parser.parse_args()

    spark: SparkSession = (
        SparkSession.builder.appName("Spark Transform Data")
        .config(
            "hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        )
        .enableHiveSupport()
        .getOrCreate()
    )
    perform_sql(spark=spark, bucket_name=args.bucket_name)
