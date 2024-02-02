from airflow import DAG
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from utils import local_to_s3, run_redshift_external_query
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

# Config
# BUCKET_NAME = Variable.get("BUCKET")
# EMR_ID = Variable.get("EMR_ID")
# EMR_STEPS = {}
# with open("path") as json_file:
#     EMR_STEPS = json.load(json_file)

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    'wait_for_downstream': True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    "user_behavior",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    start_date=datetime(2010, 12, 1),
    max_active_runs=1
)

empty_task = EmptyOperator(task_id='empty_task')

