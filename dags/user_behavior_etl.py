from datetime import datetime, timedelta
from airflow import DAG
from etl.extract.extract import extract_orders, extract_users
from airflow.decorators import task_group
from etl.transform.transform import (
    trigger_transform_data,
    upload_process_script,
    crawler_data,
    crawler_sensor,
    wait_for_transformation_data,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="user_behavior_etl",
    schedule_interval="@daily",
    start_date=datetime(2019, 1, 6),
    max_active_runs=1,
    default_args=default_args,
    catchup=True,
) as dag:

    def user_behavior_etl():
        @task_group(group_id="extract", dag=dag)
        def extract():
            extract_users(dag)
            extract_orders(dag)

        @task_group(group_id="transform", dag=dag)
        def transform():
            upload_process_script(dag)
            (
                crawler_data(dag)
                >> crawler_sensor(dag)
                >> trigger_transform_data(dag)
                >> wait_for_transformation_data(dag)
            )

        extract() >> transform()


user_behavior_etl()
