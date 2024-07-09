from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello, World! Its the 1st DAG")

# Define the default arguments dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='DAG_1',
    default_args=default_args,
    description='A simple DAG with dummy and Python operators',
    schedule_interval=None,  # Set to None to disable scheduling; adjust as needed
    catchup=False,
) as dag:

    # Define the start dummy operator
    start = DummyOperator(
        task_id='start'
    )

    # Define the Python operator
    print_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )

    # Define the end dummy operator
    end = DummyOperator(
        task_id='end'
    )

    # Set up the task dependencies
    start >> print_task >> end
