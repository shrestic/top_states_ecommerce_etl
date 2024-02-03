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
    SELECT id,
        order_id,
        user_id,
        product_id,
        inventory_item_id,
        status,
        to_date(cast(created_at as TEXT),'YYYY-MM-DD') as created_at,
        to_date(cast(shipped_at as TEXT),'YYYY-MM-DD') as shipped_at,
        to_date(cast(delivered_at as TEXT),'YYYY-MM-DD') as delivered_at,
        to_date(cast(returned_at as TEXT),'YYYY-MM-DD') as returned_at,
        sale_price
    FROM public.orders
    WHERE to_date(cast(created_at as TEXT),'YYYY-MM-DD') = {start_date}
    ORDER BY created_at
) TO STDOUT WITH (FORMAT CSV, HEADER);"""

BUCKET_NAME = Variable.get('BUCKET_NAME')


def extract_users_data_pg():
    export_postgres_to_csv(sql=sql_user_query, file=open(
        'plugins/data/users.csv', 'w'))
    local_to_s3(
        bucket_name=BUCKET_NAME,
        key="raw_data/user/users.csv",
        file_name="plugins/data/users.csv",
        remove_local=True
    )


def extract_orders_data_pg(date: str):
    export_postgres_to_csv(sql=sql.SQL(sql_order_query).format(start_date=sql.Literal(date)),
                           file=open('plugins/data/orders.csv', 'w')),
    date_obj = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S%z")
    local_to_s3(
        bucket_name=BUCKET_NAME,
        key="raw_data/order/date=" +
            str(date_obj.strftime("%Y-%m-%d")) + "/orders.csv",
        file_name="plugins/data/orders.csv",
        remove_local=True
    )


def extract_users(dag):
    return PythonOperator(
        dag=dag,
        task_id="extract_users", python_callable=extract_users_data_pg
    )


def extract_orders(dag):
    return PythonOperator(
        dag=dag,
        task_id="extract_orders",
        python_callable=extract_orders_data_pg,
        op_kwargs={"date": "{{ data_interval_start }}"},
    )
