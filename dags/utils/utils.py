import os
import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def local_to_s3(
    bucket_name: str, 
    key: str, 
    file_name: str, 
    remove_local: bool = False,
) -> None:
    s3 = S3Hook(aws_conn_id='aws-conn-id')
    s3.load_file(
        filename=file_name, 
        bucket_name=bucket_name, 
        replace=True, 
        key=key
    )
    if remove_local:
        if os.path.isfile(file_name):
            os.remove(file_name)
            

def run_redshift_external_query(qry: str) -> None:
    rs_hook = PostgresHook(postgres_conn_id="redshift-conn-id",database="dev")
    rs_conn = rs_hook.get_conn()
    rs_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    rs_cursor = rs_conn.cursor()
    rs_cursor.execute(qry)
    rs_cursor.close()
    rs_conn.commit()
    
'''
The get_conn() method of the PostgresHook object is used to get a connection to the Redshift database.

The set_isolation_level() method of the connection object is used to set the isolation level to AUTOCOMMIT. This means that each SQL statement will be committed automatically after it is executed.

The cursor() method of the connection object is used to create a cursor object. The cursor object is used to execute SQL statements.

The execute() method of the cursor object is used to execute the SQL query.

The close() method of the cursor object is used to close the cursor object.

The commit() method of the connection object is used to commit the changes made to the database.
'''