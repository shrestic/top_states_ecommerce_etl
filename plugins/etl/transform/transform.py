import json
from airflow.models import Variable
from utilities.utils import local_to_s3
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor


BUCKET_NAME = Variable.get("BUCKET_NAME")
EMR_ID = Variable.get("EMR_ID")
EMR_STEPS = {}
with open("./plugins/scripts/emr/step_emr.json") as json_file:
    EMR_STEPS = json.load(json_file)


def spark_script_to_s3():
    local_to_s3(
        bucket_name=BUCKET_NAME,
        key="scripts/process_transform_script.py",
        file_name="plugins/scripts/spark/process_transform_script.py",
        remove_local=False,
    )


def upload_process_script(dag):
    return PythonOperator(
        dag=dag,
        task_id="upload_process_script",
        python_callable=spark_script_to_s3,
    )


def trigger_transform_data(dag):
    return EmrAddStepsOperator(
        dag=dag,
        task_id="trigger_transform_data",
        job_flow_id=EMR_ID,
        aws_conn_id="aws-conn-id",
        steps=EMR_STEPS,
        params={"BUCKET_NAME": BUCKET_NAME},
        depends_on_past=True,
    )


def wait_for_transformation_data(dag):
    last_step = len(EMR_STEPS)-1
    return EmrStepSensor(
        dag=dag,
        aws_conn_id="aws-conn-id",
        task_id="wait_for_transformation_data",
        job_flow_id=EMR_ID,
        step_id='{{ task_instance.xcom_pull("transform.trigger_transform_data", key="return_value")['
        + str(last_step)
        + "] }}",
        depends_on_past=True,
    )
