# upstream_dag.py
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}
def update_variable():
    # Get the current value of the variable
    dag_conf = Variable.get("dag_conf", deserialize_json=True)
    
    # Update the DAG configuration where name is "DAG_10"
    for dag in dag_conf:
        dag['enabled'] = True
    
    # Set the updated value back to the variable
    Variable.set("dag_conf", dag_conf,serialize_json=True)

with DAG(
    'upstream_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    update = PythonOperator(
        task_id = 'update_variable',
        python_callable=update_variable,
        dag=dag
    )

    update
