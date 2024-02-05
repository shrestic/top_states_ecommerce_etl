from datetime import datetime
from utilities.utils import export_postgres_to_csv, local_to_s3
from psycopg2 import sql
from airflow.models import Variable
from airflow.operators.python import PythonOperator

sql_user_query = """COPY (
    SELECT id,
        first_name,
        last_name,
        email,
        age,
        gender,
        state,
        street_address,
        postal_code,
        city,
        country,
        latitude,
        longitude,
        traffic_source,
        to_date(cast(created_at as TEXT),'YYYY-MM-DD') as created_at
    FROM users
) TO STDOUT WITH (FORMAT CSV, HEADER);"""

sql_order_query = """COPY (
    SELECT order_id,
        user_id,
        status,
        gender,
        created_at,
        returned_at,
        shipped_at,
        delivered_at,
        num_of_item
    FROM public.orders
    WHERE DATE_TRUNC('month', created_at) = DATE_TRUNC('month', TIMESTAMP {start_date}) 
    ORDER BY created_at
) TO STDOUT WITH (FORMAT CSV, HEADER);"""

BUCKET_NAME = Variable.get("BUCKET_NAME")


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
