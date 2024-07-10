# downstream_dag.py

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'downstream_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    wait_for_upstream = ExternalTaskSensor(
        task_id='wait_for_upstream',
        external_dag_id='upstream_dag',
        external_task_id='end',  # The task_id of the task in upstream_dag you want to wait for
        timeout=600,  # Timeout in seconds
        poke_interval=60,  # Poke interval in seconds
        mode='poke'  # 'poke' or 'reschedule'
    )

    start_downstream = DummyOperator(
        task_id='start_downstream',
    )

    wait_for_upstream >> start_downstream
