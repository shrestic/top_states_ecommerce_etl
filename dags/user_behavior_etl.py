from datetime import datetime, timedelta
from etl.extract.extract import extract_users_pg, extract_orders_pg
from airflow.decorators import dag, task_group
from airflow.operators.python import PythonOperator
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


@dag(dag_id='user_behavior_etl',
     schedule_interval="@daily",
     start_date=datetime(2019, 1, 5),
     max_active_runs=1, default_args=default_args,
     catchup=True)
def user_behavior_etl():
    @task_group(group_id='extract')
    def extract():

        extract_users = PythonOperator(
            task_id='extract_users',
            python_callable=extract_users_pg
        )

        extract_orders = PythonOperator(
            task_id='extract_orders',
            python_callable=extract_orders_pg,
            op_kwargs={"date": "{{ data_interval_start }}"},
        )

    extract()


dag = user_behavior_etl()
