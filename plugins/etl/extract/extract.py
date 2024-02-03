from utilities.utils import export_postgres_to_csv, local_to_s3
from psycopg2 import sql
from airflow.models import Variable

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
        created_at
    FROM users
) TO STDOUT WITH (FORMAT CSV, HEADER);"""

sql_order_query = """COPY (
    SELECT id,
        order_id,
        user_id,
        product_id,
        inventory_item_id,
        status,
        created_at,
        shipped_at,
        delivered_at,
        returned_at,
        sale_price
    FROM public.orders
    WHERE to_date(cast(created_at as TEXT),'YYYY-MM-DD') = {start_date}
    ORDER BY created_at
) TO STDOUT WITH (FORMAT CSV, HEADER);"""

bucket_name = Variable.get('BUCKET_NAME')


def extract_users_pg():
    export_postgres_to_csv(sql=sql_user_query, file=open(
        'plugins/data/users.csv', 'w'))
    local_to_s3(
        bucket_name=bucket_name,
        key="stage/users/users.csv",
        file_name="plugins/data/users.csv",
        remove_local=True
    )

def extract_orders_pg(date: str):
    export_postgres_to_csv(sql=sql.SQL(sql_order_query).format(start_date=sql.Literal(date)),
                           file=open('plugins/data/orders.csv', 'w')),
    local_to_s3(
        bucket_name=bucket_name,
        key="stage/orders/date=" + str(date) + "/orders.csv",
        file_name="plugins/data/orders.csv",
        remove_local=True
    )
