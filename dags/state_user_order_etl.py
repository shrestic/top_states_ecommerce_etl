from datetime import datetime, timedelta
from airflow import DAG
from etl.extract.extract import extract_orders, extract_users
from airflow.decorators import task_group
from etl.load.load import get_states_user_order_data
from etl.transform.transform import (
    trigger_transform_data,
    upload_process_script,
    wait_for_transformation_data
)

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="state_user_order_etl",
    schedule_interval="@monthly",
    start_date=datetime(2019, 1, 6),
    max_active_runs=1,
    default_args=default_args,
    template_searchpath="plugins/scripts/sql",
    catchup=True,
) as dag:

    def state_user_order_etl():
        @task_group(group_id="extract", dag=dag)
        def extract():
            extract_users(dag)
            extract_orders(dag)

        @task_group(group_id="transform", dag=dag)
        def transform():
            (
                upload_process_script(dag)
                >> trigger_transform_data(dag)
                >> wait_for_transformation_data(dag)
            )

        @task_group(group_id="load", dag=dag)
        def load():
            get_states_user_order_data(dag)

        extract() >> transform() >> load()


state_user_order_etl()
