import os
import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerHook


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
    rs_hook = PostgresHook(postgres_conn_id="redshift-conn-id", database="dev")
    rs_conn = rs_hook.get_conn()
    rs_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    rs_cursor = rs_conn.cursor()
    rs_cursor.execute(qry)
    rs_cursor.close()
    rs_conn.commit()


def export_postgres_to_csv(sql, file) -> None:
    pg_hook = PostgresHook(
        postgres_conn_id="postgres_conn_id", database="airflow")
    pg_conn = pg_hook.get_conn()
    pg_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    pg_cursor = pg_conn.cursor()
    pg_cursor.copy_expert(
        sql=sql,
        file=file
    )
    pg_cursor.close()
    pg_conn.commit()


def create_crawler(name, role, database_name, path_s3):
    crawler_hook = GlueCrawlerHook(aws_conn_id="aws-conn-id")
    if not crawler_hook.has_crawler(name):
        crawler_hook.create_crawler(
            Name=name,
            Role=role,
            DatabaseName=database_name,
            Targets={
                'S3Targets': [
                    {
                        'Path': path_s3,
                    },
                ]
            },
        )


def start_crawler(name):
    crawler_hook = GlueCrawlerHook(aws_conn_id="aws-conn-id")
    crawler_hook.start_crawler(name)
