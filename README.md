# Project Overview
For our project we will assume we work for a user behavior analytics company that collects user data from different data sources and joins them together to get a broader understanding of the customer. For this project we will consider 2 sources,
   1. Purchase data from an OLTP database
   2. Movie review data from a 3rd party data vendor (we simulate this by using a file and assuming its from a data vendor)

<img width="741" alt="image" src="https://github.com/nhatphongcgp/batch-processing-project/assets/60643737/6022ae6a-7a4d-4ddc-b0ea-682919408d53">


The goal is to provide a joined dataset of the 2 datasets above, in our analytics (OLAP) database every day, to be used by analysts, dashboard software, etc.

<img width="718" alt="image" src="https://github.com/nhatphongcgp/batch-processing-project/assets/60643737/9ec555a6-79c9-4721-9c8b-a9a285c5a926">


## ETL Design

*  ### Airflow Theory:
Airflow runs data pipelines using DAG's. Each DAG is made up of one or more tasks which can be stringed together to get the required data flow. Airflow also enables templating, where text surrounded by {{ }} can be replaced by variables when the DAG is run. These variables can either be passed in by the user as params, or we can use inbuilt macros for commonly used variables.

Check document: https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html

## Code And Explanation

For ease of implementation and testing, we will build our data pipeline in stages. There are 3 stages and these 3 stages shown below

<img width="724" alt="image" src="https://github.com/nhatphongcgp/batch-processing-project/assets/60643737/08408841-0028-4ef6-a733-0b490fff1a46">


* ### Stage 1. Postgres -> File -> S3 Bucket
Since we are not dealing with a lot of data we can use or  Airflow metadata database as our "fake" datastore as well.

Let's set up our fake datastore.

```sql
CREATE SCHEMA retail;

CREATE TABLE retail.user_purchase (
    invoice_number varchar(10),
    stock_code varchar(20),
    detail varchar(1000),
    quantity int,
    invoice_date timestamp,
    unit_price Numeric(8,3),
    customer_id int,
    country varchar(20)
);

COPY retail.user_purchase(invoice_number,
stock_code,detail,quantity,
invoice_date,unit_price,customer_id,country) 
FROM '/data/OnlineRetail.csv' 
DELIMITER ','  CSV HEADER;

```

Now let's build up on what we already have and unload data from pg into a local file. We have to unload data for the specified execution day and only if the quantity is greater than 2. Create a script called `filter_unload_user_purchase.sql` in the `/dags/scripts/sql/` directory 

```sql
COPY (
    select invoice_number,
           stock_code,
           detail,
           quantity,
           invoice_date,
           unit_price,
           customer_id,
           country
      from retail.user_purchase
     where quantity > 2
       and cast(invoice_date as date)='{{ ds }}')
TO '{{ params.user_purchase }}' WITH (FORMAT CSV, HEADER);

```

In this templated SQL script we use `{{ ds }}` which is one of airflow's inbuilt macros to get the execution date. `{{ params.user_purchase }}` is a parameter we have to set at the DAG. In the DAG we will use a PostgresOperator to execute the `filter_unload_user_purchase.sql` sql script. Add the following snippet to your DAG at `user_behaviour.py`.

```python
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.empty import EmptyOperator

unload_user_purchase ='./scripts/sql/filter_unload_user_purchase.sql'

empty_task = EmptyOperator(task_id= 'empty_task')

extract_user_purchase_data = SQLExecuteQueryOperator(
    dag=dag,
    task_id='extract_user_purchase_data',
    sql=unload_user_purchase,
    conn_id='postgres_default',
    params={'user_purchase': "/temp/user_purchase.csv"},
    depends_on_past=True,
    wait_for_downstream=True
)

```

It’s not always a good pattern to store the entire dataset in our local filesystem. Since we could get an out-of-memory error if our dataset is too large. 
Ideally you will have your configs in a different file or set them as docker env variables, but due to this being a simple example we keep them with the DAG script.


You will see that the {{ }} template in your SQL script will have been replaced by parameters set in the DAG at user_behaviour.py.
For the next task lets upload this file to our S3 bucket. But before upload, we gonna create file call utils.py and define a helper function to help we push file from local to s3 bucket

```python
import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def local_to_s3(
    bucket_name: str, 
    key: str, 
    file_name: str, 
    remove_local: bool = False,
) -> None:
    s3 = S3Hook(aws_conn_id='s3_conn')
    s3.load_file(
        filename=file_name, 
        bucket_name=bucket_name, 
        replace=True, 
        key=key
    )
    if remove_local:
        if os.path.isfile(file_name):
            os.remove(file_name)
```
And add the following snippet to your DAG 

```python
from airflow.hooks.S3_hook import S3Hook
from airflow.operators import PythonOperator
from dags.scripts.utils.utils import local_to_s3

BUCKET_NAME = '<your-bucket-name>'

user_purchase_to_stage_data_lake = PythonOperator(
    dag=dag,
    task_id='user_purchase_to_stage_data_lake',
    python_callable=local_to_s3,
    op_kwargs={
        "file_name": "temp/user_purchase.csv",
        "key": "user_purchase/stage/{{ ds }}/user_purchase.csv",
        "bucket_name": BUCKET_NAME,
        "remove_local": True,
    },
)

```
In the above snippet we have introduced 2 new concepts: the S3Hook and PythonOperator. The hook is a mechanism used by airflow to establish connections to other systems(S3 in our case), we wrap the creation of an S3Hook and moving a file from our local filesystem to S3 using a python function called `local_to_s3` and call it using the PythonOperator.

* ### Stage 2. File -> S3 -> EMR -> S3
In this stage we assume we are getting a movie review data feed from a data vendor. Usually the data vendor drops data in S3 or some SFTP server, but in our example let's assume the data is available at setup `data/movie_review.csv`

Moving the movie_review.csv file to S3 is similar to the tasks we did in stage 1

```python
movie_review_to_raw_data_lake = PythonOperator(
    dag=dag,
    task_id="movie_review_to_raw_data_lake",
    python_callable=local_to_s3,
    op_kwargs={
        "file_name": "data/movie_review.csv",
        "key": "raw/movie_review/{{ ds }}/movie.csv",
        "bucket_name": BUCKET_NAME,
    },
)
```
It's similar to the previous task, but not directly dependent on any other task.

In EMR we have a feature called steps which can be used to run commands on the EMR cluster one at at time, we will use these steps to

    1.Pull data from  S3 location to EMR clusters HDFS location.

    2.Perform text data cleaning and naive text classification using a pyspark script and write the output to HDFS in the EMR cluster.

    3.Moves classified data from HDFS to S3.
    
We can define the EMR steps as a json file (https://docs.aws.amazon.com/cli/latest/reference/emr/add-steps.html), create a file /dags/scripts/emr/clean_movie_review.json. Its content should be as follows

```json
[
    {
      "Name": "Move raw data from S3 to HDFS",
      "ActionOnFailure": "CANCEL_AND_WAIT",
      "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": [
          "s3-dist-cp",
          "--src=s3://{{ params.BUCKET_NAME }}/{{ params.raw_movie_review }}/{{ ds }}/",
          "--dest=/movie/{{ ds }}"
        ]
      }
    },
    {
      "Name": "Classify movie reviews",
      "ActionOnFailure": "CANCEL_AND_WAIT",
      "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": [
          "spark-submit",
          "s3://{{ params.BUCKET_NAME }}/scripts/random_text_classification.py",
          "--input=/movie/{{ ds }}",
          "--run-id={{ ds }}"
        ]
      }
    },
    {
      "Name": "Move classified data from HDFS to S3",
      "ActionOnFailure": "CANCEL_AND_WAIT",
      "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": [
          "s3-dist-cp",
          "--src=/output",
          "--dest=s3://{{ params.BUCKET_NAME }}/{{ params.stage_movie_review }}/{{ ds }}"
        ]
      }
    }
  ]
```

The first step uses s3-dist-cp (https://docs.aws.amazon.com/emr/latest/ReleaseGuide/UsingEMR_s3distcp.html) is a distributed copy tool to copy data from S3 to EMR's HDFS. The second step runs a pyspark script called random_text_classification.py we will see what it is and how it gets moved to that S3 location and finally we move the output to a stage location. The templated values will be filled in by the values provided to the DAG at run time.

Create a python file at beginner_de_project/dags/scripts/spark/random_text_classification.py with the following content

P/s: It's a simple spark script to clean text data (tokenize and remove stop words) and use a naive classification heuristic to classify if a review is positive or not. I'm not very familiar with ML so I consulted some documents and links:

https://www.youtube.com/watch?v=eAhZsAMWWsA

https://www.youtube.com/watch?v=rfKg6ciG8c4

```python
# pyspark
import argparse

from pyspark.ml.feature import StopWordsRemover, Tokenizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, lit


def random_text_classifier(
    input_loc: str, output_loc: str, run_id: str
) -> None:
    """
    This is a dummy function to show how to use spark, It is supposed to mock
    the following steps
        1. clean input data
        2. use a pre-trained model to make prediction
        3. write predictions to a HDFS output

    We are naively going to mark reviews having the text "good" as
    positive and the rest as negative
    """

    # read input
    df_raw = spark.read.option("header", True).csv(input_loc)
    # perform text cleaning

    # Tokenize text
    tokenizer = Tokenizer(inputCol="review_str", outputCol="review_token")
    df_tokens = tokenizer.transform(df_raw).select("cid", "review_token")

    # Remove stop words
    remover = StopWordsRemover(
        inputCol="review_token", outputCol="review_clean"
    )
    df_clean = remover.transform(df_tokens).select("cid", "review_clean")

    # function to check presence of good
    df_out = df_clean.select(
        "cid",
        array_contains(df_clean.review_clean, "good").alias("positive_review"),
    )
    df_fin = df_out.withColumn("insert_date", lit(run_id))
    # parquet is a popular column storage format, we use it here
    df_fin.write.mode("overwrite").parquet(output_loc)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input", type=str, help="HDFS input", default="/movie"
    )
    parser.add_argument(
        "--output", type=str, help="HDFS output", default="/output"
    )
    parser.add_argument("--run-id", type=str, help="run id")
    args = parser.parse_args()
    spark = SparkSession.builder.appName(
        "Random Text Classifier"
    ).getOrCreate()
    random_text_classifier(
        input_loc=args.input, output_loc=args.output, run_id=args.run_id
    )
```

Note that in the second EMR step we are reading the script from your S3 bucket so we also have to move the pyspark script to a S3 location.
Add the following content to your DAG at `user_behaviour.py`.

```python
movie_review_to_raw_data_lake = PythonOperator(
    dag=dag,
    task_id="movie_review_to_raw_data_lake",
    python_callable=_local_to_s3,
    op_kwargs={
        "file_name": "/data/movie_review.csv",
        "key": "raw/movie_review/{{ ds }}/movie.csv",
        "bucket_name": BUCKET_NAME,
    },
)

spark_script_to_s3 = PythonOperator(
    dag=dag,
    task_id="spark_script_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={
        "file_name": "./dags/scripts/spark/random_text_classification.py",
        "key": "scripts/random_text_classification.py",
        "bucket_name": BUCKET_NAME,
    },
)

start_emr_movie_classification_script = EmrAddStepsOperator(
    dag=dag,
    task_id="start_emr_movie_classification_script",
    job_flow_id=EMR_ID,
    aws_conn_id="aws_default",
    steps=EMR_STEPS,
    params={
        "BUCKET_NAME": BUCKET_NAME,
        "raw_movie_review": "raw/movie_review",
        "text_classifier_script": "scripts/random_text_classifier.py",
        "stage_movie_review": "stage/movie_review",
    },
    depends_on_past=True,
)

last_step = len(EMR_STEPS) - 1

wait_for_movie_classification_transformation = EmrStepSensor(
    dag=dag,
    task_id="wait_for_movie_classification_transformation",
    job_flow_id=EMR_ID,
    step_id='{{ task_instance.xcom_pull("start_emr_movie_classification_script", key="return_value")['
    + str(last_step)
    + "] }}",
    depends_on_past=True,
)
```
* ### Stage 3. movie_review_stage, user_purchase_stage -> Redshift Table -> Quality Check Data
This stage involves doing joins in your Redshift Cluster. You should have your redshift host, database, username and password from when you set up Redshift. In your Redshift cluster you need to set up the staging tables and our final table, you can do this using the sql script at `/setup/redshift/create_external_schema.sql` in your repo, replacing the iam-ARN and s3-bucket with your specific ARN and bucket name. 

Check this document: https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-external-schemas.html

```sql

CREATE EXTERNAL SCHEMA spectrum
FROM DATA CATALOG DATABASE 'spectrumdb' iam_role 'arn:aws:iam::"$AWS_ID":role/"$IAM_ROLE_NAME' CREATE EXTERNAL DATABASE IF NOT EXISTS;
DROP TABLE IF EXISTS spectrum.user_purchase_staging;
CREATE EXTERNAL TABLE spectrum.user_purchase_staging (
    InvoiceNo VARCHAR(10),
    StockCode VARCHAR(20),
    detail VARCHAR(1000),
    Quantity INTEGER,
    InvoiceDate TIMESTAMP,
    UnitPrice DECIMAL(8, 3),
    customerid INTEGER,
    Country VARCHAR(20)
) PARTITIONED BY (insert_date DATE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS textfile LOCATION 's3://"<your-bucket>"/stage/user_purchase/' TABLE PROPERTIES ('skip.header.line.count' = '1');
DROP TABLE IF EXISTS spectrum.classified_movie_review;
CREATE EXTERNAL TABLE spectrum.classified_movie_review (
    cid VARCHAR(100),
    positive_review boolean,
    insert_date VARCHAR(12)
) STORED AS PARQUET LOCATION 's3://"<your-bucket>"/stage/movie_review/';
DROP TABLE IF EXISTS public.user_behavior_metric;
CREATE TABLE public.user_behavior_metric (
    customerid INTEGER,
    amount_spent DECIMAL(18, 5),
    review_score INTEGER,
    review_count INTEGER,
    insert_date DATE
);

```
In the above script there are 4 main steps

    1.Create your spectrum external schema, if you are unfamiliar with the external part, it is basically a mechanism where the data is stored outside of the database(in our case in S3) and the data schema details are stored in something called a data catalog(in our case AWS glue). When the query is run, the database executor talks to the data catalog to get information about the location and schema of the queried table and processes the data. The advantage here is separation of storage(cheaper than storing directly in database) and processing(we can scale as required) of data. This is called Spectrum within Redshift, we have to create an external database to enable this functionality.

    2.Creating an external user_purchase_staging table, note here we are partitioning by insert_date, this means the data is stored at s3://<your-s3-bucket>/stage/user_purchase/yyyy-mm-dd, partitioning is a technique to reduce the data that needs to be scanned by the query engine to get the requested data. The partition column(s) should depend on the query pattern that the data is going to get. But rule of thumb, date is generally a good partition column, especially our Airflow works off date ranges. Note here that once we add a partition we need to alter the user_purchase_staging to be made aware of that.

    3.Creating an external movie_review_clean_stage table to store the data which was cleaned by EMR. Note here we use a term STORED AS PARQUET this means that data is stored in parquet format. Parquet is a column storage format for efficient compression. We wrote out the data as parquet in our spark script. Note here that we can just drop the correct data at s3://<your-s3-bucket>/stage/movie_review/ and it will automatically be ready for queries.

    4.Create a table user_behavior_metric which is our final goal.
    
We have the movie review and user purchase data cleaned and ready in the staging S3 location. We need to enable airflow to connect to our redshift database.

Once we have the connection established, we need update `utils.py` file then let the `user_purchase_staging` table know that a new partition has been added. We can do that on our DAG as shown below.


```python
user_purchase_to_rs_stage = PythonOperator(
    dag=dag,
    task_id='user_purchase_to_rs_stage',
    python_callable=run_redshift_external_query,
    op_kwargs={
        'qry': "alter table spectrum.user_purchase_staging add partition(insert_date='{{ ds }}') \
            location 's3://"
        + BUCKET_NAME
        + "/stage/user_purchase/{{ ds }}'",
    },
)
```

The final task is to load data into our `user_behavior_metric` table. Let's write a templated query to do this at `/dags/scripts/sql/get_user_behavior_metrics.sql`

```sql
INSERT INTO public.user_behavior_metric 
(customerid, 
amount_spent, 
review_score, 
review_count, 
insert_date) 

SELECT ups.customerid, 
cast(sum( ups.Quantity * ups.UnitPrice) as decimal(18, 5)) as amount_spent, 
sum(mrcs.positive_review) as review_score, count(mrcs.cid) as review_count,
'{{ ds }}' 
FROM spectrum.user_purchase_staging ups  
JOIN (select cid, case when positive_review is True then 1 else 0 end as positive_review from spectrum.movie_review_clean_stage) mrcs  
ON ups.customerid = mrcs.cid 
WHERE ups.insert_date = '{{ ds }}' 
GROUP BY ups.customerid;

```

We are getting customer level metrics and loading them into a user_behavior_metric table. Add this as a task to our DAG as shown below

```python
get_user_behaviour = PostgresOperator(
    dag=dag,
    task_id='get_user_behaviour',
    sql=get_user_behaviour,
    postgres_conn_id='redshift'
)
```
 Verify that it completes successfully from the Airflow UI and Redshift Editor
 
<img width="500" alt="image" src="https://github.com/nhatphongcgp/batch-processing-project/assets/60643737/477e82ae-8437-409b-b2dc-fb5976611353">

<img width="500" alt="image" src="https://github.com/nhatphongcgp/batch-processing-project/assets/60643737/b77da8a0-092e-41c8-9d83-c0676c97f387">



 


