from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


def get_states_user_order_data(dag):
    return SQLExecuteQueryOperator(
        database='dev',
        dag=dag,
        task_id='get_states_user_order_data',
        sql='get_states_user_order_data.sql',
        conn_id='redshift-conn-id'
    )
