from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from utils.utils import local_to_s3, run_redshift_external_query
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
import json

# Config local path of script sql file
unload_user_purchase = './scripts/sql/filter_unload_user_purchase.sql'

# Config
BUCKET_NAME = Variable.get("BUCKET")
EMR_ID = Variable.get("EMR_ID")
EMR_STEPS = {}
with open("./dags/scripts/emr/clean_movie_review.json") as json_file:
    EMR_STEPS = json.load(json_file)

default_args = {
    # The owner of the DAG. This is the user who is responsible for the DAG.
    "owner": "airflow",
    # Whether or not the task depends on the previous task in the DAG. If this is set to True, the task will not be executed if the previous task fails.
    "depends_on_past": True,
    # Whether or not the task will wait for all downstream tasks to finish before it can finish. If this is set to True, the task will not be marked as finished until all downstream tasks are finished.
    'wait_for_downstream': True,
    "retries": 1,  # The number of times the task will be retried if it fails.
    # The delay between retries, in minutes.
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    "user_behaviour",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    start_date=datetime(2010, 12, 1),
    max_active_runs=1
)

empty_task = EmptyOperator(task_id='empty_task')

"============= Stage 1. Postgres -> File -> S3 ================"

extract_user_purchase_data = SQLExecuteQueryOperator(
    dag=dag,
    task_id='extract_user_purchase_data',
    sql=unload_user_purchase,
    conn_id='postgres_default',
    params={'user_purchase': "/temp/user_purchase.csv"},
    depends_on_past=True,
    wait_for_downstream=True
)

user_purchase_to_stage_data_lake = PythonOperator(
    dag=dag,
    task_id='user_purchase_to_stage_data_lake',
    python_callable=local_to_s3,
    op_kwargs={
        "file_name": "temp/user_purchase.csv",
        "key": "stage/user_purchase/{{ ds }}/user_purchase.csv",
        "bucket_name": BUCKET_NAME,
        "remove_local": True,
    },
)
"============= Stage 2. File -> S3 -> EMR -> S3 ================"

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

spark_script_to_s3 = PythonOperator(
    dag=dag,
    task_id="spark_script_to_s3",
    python_callable=local_to_s3,
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
    aws_conn_id="aws-conn-id",
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
'''
In this case, the step_id is the ID of the EMR step that was submitted by the task start_emr_movie_classification_script.
The last_step variable is a placeholder that will be replaced with the index of the last step in the EMR job flow.
The sensor will first check if the EMR step has already completed.
If it has, the sensor will return immediately.
Otherwise, the sensor will sleep for a specified amount of time and then check the EMR step again.
This process will continue until the EMR step has completed.
The wait_for_movie_classification_transformation sensor is used to ensure that the EMR step that performs the movie classification transformation has completed before the next task in the DAG is triggered.
This helps to prevent the DAG from being triggered before the transformation has completed, which could lead to errors.

The xcom_pull() method takes two arguments: the task_id of the task that pushed the XCom, and the key of the XCom.
In this case, the task_id is the ID of the start_emr_movie_classification_script task, and the key is "return_value".

The start_emr_movie_classification_script task is responsible for submitting an EMR step that performs the movie classification transformation.
The return_value XCom of this task contains the ID of the EMR step.
'''
wait_for_movie_classification_transformation = EmrStepSensor(
    dag=dag,
    aws_conn_id="aws-conn-id",
    task_id="wait_for_movie_classification_transformation",
    job_flow_id=EMR_ID,
    step_id='{{ task_instance.xcom_pull("start_emr_movie_classification_script", key="return_value")['
    + str(last_step)
    + "] }}",
    depends_on_past=True,
)

"============= Stage 3. movie_review_stage, user_purchase_stage -> Redshift Table -> Quality Check Data ================"

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

get_user_behaviour = PostgresOperator(
    dag=dag,
    task_id='get_user_behaviour',
    sql='scripts/sql/get_user_behavior_metrics.sql',
    postgres_conn_id='redshift-conn-id'
)


(extract_user_purchase_data >> user_purchase_to_stage_data_lake >> user_purchase_to_rs_stage)
([movie_review_to_raw_data_lake, spark_script_to_s3] >> start_emr_movie_classification_script >> wait_for_movie_classification_transformation)
([user_purchase_to_rs_stage, wait_for_movie_classification_transformation] >> get_user_behaviour)

