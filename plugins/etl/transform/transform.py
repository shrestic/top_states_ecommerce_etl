import json
from airflow.models import Variable
from utilities.utils import create_crawler, local_to_s3, start_crawler
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor


BUCKET_NAME = Variable.get("BUCKET_NAME")
EMR_ID = Variable.get("EMR_ID")
EMR_STEPS = {}
with open("./plugins/scripts/emr/step_emr.json") as json_file:
    EMR_STEPS = json.load(json_file)


def crawl_raw_data():
    create_crawler(
        name="crawler_raw_data",
        role="arn:aws:iam::362262895301:role/Custom-Glue-S3-Role",
        database_name="e_commerce_database",
        path_s3="s3://" + BUCKET_NAME + "/raw_data/",
    )
    start_crawler(name="crawler_raw_data")


def upload_process_job_script():
    local_to_s3(
        bucket_name=BUCKET_NAME,
        key="scripts/process_transform_script.py",
        file_name="plugins/scripts/spark/process_transform_script.py",
        remove_local=False,
    )


def crawler_data(dag):
    return PythonOperator(
        dag=dag,
        task_id="crawler_data",
        python_callable=crawl_raw_data,
    )


def crawler_sensor(dag):
    return GlueCrawlerSensor(
        dag=dag,
        task_id="crawler_sensor",
        aws_conn_id="aws-conn-id",
        crawler_name="crawler_raw_data",
    )


def upload_process_script(dag):
    return PythonOperator(
        dag=dag,
        task_id="upload_process_script",
        python_callable=upload_process_job_script,
    )


def trigger_transform_data(dag):
    return EmrAddStepsOperator(
        dag=dag,
        task_id="trigger_transform_data",
        job_flow_id=EMR_ID,
        aws_conn_id="aws-conn-id",
        steps=EMR_STEPS,
        params={
            "BUCKET_NAME": BUCKET_NAME,
            "FILE_NAME": "user_behavior.csv",
        },
        depends_on_past=True,
    )


def wait_for_transformation_data(dag):
    return EmrStepSensor(
        dag=dag,
        aws_conn_id="aws-conn-id",
        task_id="wait_for_transformation_data",
        job_flow_id=EMR_ID,
        step_id="{{ task_instance.xcom_pull('transform.trigger_transform_data', key='return_value')[0] }}",
        depends_on_past=True,
    )
