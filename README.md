# Project Overview
The project builds an ETL pipeline to collect e-commerce data from different data sources and joins them together to identify the top states in US with the most users and orders.
## Airflow Graph Design
 * This pipeline is scheduled to run on a monthly basis.
![Screenshot 2024-02-05 at 18 23 12](https://github.com/shrestic/top_states_ecommerce_etl/assets/60643737/0e82bc93-fbba-4d1f-916f-c6524e93b746)
## ETL Design
![design](https://github.com/shrestic/top_states_ecommerce_etl/assets/60643737/2a75615c-8acb-4b86-8168-e170a52801b2)
### Extract 
* The `extract.py` file
```python
def extract_users_data_pg():
    export_postgres_to_csv(sql=sql_user_query, file=open("plugins/data/users.csv", "w"))
    local_to_s3(
        bucket_name=BUCKET_NAME,
        key="raw-data/user/users.csv",
        file_name="plugins/data/users.csv",
        remove_local=True,
    )


def extract_orders_data_pg(date: str):
    export_postgres_to_csv(
        sql=sql.SQL(sql_order_query).format(start_date=sql.Literal(date)),
        file=open("plugins/data/orders.csv", "w"),
    ),
    date_converted = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d")
    local_to_s3(
        bucket_name=BUCKET_NAME,
        key="raw-data/order/" + str(date_converted) + ".csv",
        file_name="plugins/data/orders.csv",
        remove_local=True,
    )


def extract_users(dag):
    return PythonOperator(
        dag=dag, task_id="extract_users", python_callable=extract_users_data_pg
    )


def extract_orders(dag):
    return PythonOperator(
        dag=dag,
        task_id="extract_orders",
        python_callable=extract_orders_data_pg,
        op_kwargs={"date": "{{ data_interval_start }}"},
    )


```
### Transform 
* The script
`process_transform_script.py`
```python
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

```
* The `transform.py` file
```python
def spark_script_to_s3():
    local_to_s3(
        bucket_name=BUCKET_NAME,
        key="scripts/process_transform_script.py",
        file_name="plugins/scripts/spark/process_transform_script.py",
        remove_local=False,
    )


def upload_process_script(dag):
    return PythonOperator(
        dag=dag,
        task_id="upload_process_script",
        python_callable=spark_script_to_s3,
    )


def trigger_transform_data(dag):
    return EmrAddStepsOperator(
        dag=dag,
        task_id="trigger_transform_data",
        job_flow_id=EMR_ID,
        aws_conn_id="aws-conn-id",
        steps=EMR_STEPS,
        params={"BUCKET_NAME": BUCKET_NAME},
        depends_on_past=True,
    )


def wait_for_transformation_data(dag):
    last_step = len(EMR_STEPS)-1
    return EmrStepSensor(
        dag=dag,
        aws_conn_id="aws-conn-id",
        task_id="wait_for_transformation_data",
        job_flow_id=EMR_ID,
        step_id='{{ task_instance.xcom_pull("transform.trigger_transform_data", key="return_value")['
        + str(last_step)
        + "] }}",
        depends_on_past=True,
    )

```
### Load 
* The `setup_states_user_order.sql` file is used to create the 'states_user_order' table, which stores information about user orders in the Redshift data warehouse.
```sql
CREATE EXTERNAL SCHEMA spectrum
FROM DATA CATALOG DATABASE 'states_user_order_db' iam_role 'arn:aws:iam::362262895301:role/Custom-RedShift-Role'
CREATE EXTERNAL DATABASE IF NOT EXISTS;
DROP TABLE IF EXISTS spectrum.states_user_order;
CREATE EXTERNAL TABLE spectrum.states_user_order (
    state VARCHAR,
    user_count INT,
    order_count INT,
    combined_count INT
) STORED AS PARQUET LOCATION 's3://BUCKET_NAME/transformed-data/' TABLE PROPERTIES ('skip.header.line.count' = '1');
```
* The `load.py` file. It is used for testing in this case
```python
def get_states_user_order_data(dag):
    return SQLExecuteQueryOperator(
        database='dev',
        dag=dag,
        task_id='get_states_user_order_data',
        sql='get_states_user_order_data.sql',
        conn_id='redshift-conn-id'
    )
```
## Visualize
![image](https://github.com/shrestic/top_states_ecommerce_etl/assets/60643737/064510a7-6099-46cc-b400-458faecf8780)



## Authors
- [Phong Nguyen](https://github.com/shrestic)

