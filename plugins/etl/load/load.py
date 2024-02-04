from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


def get_user_behavior_data(dag):
    return SQLExecuteQueryOperator(
        database='dev',
        dag=dag,
        task_id='get_user_behavior_data',
        sql='get_user_behavior_data.sql',
        conn_id='redshift-conn-id'
    )
