from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['example@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG, its parameters, and schedule
dag = DAG(
    'git_dag1',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 1),
    catchup=False
)

# Define tasks/functions to be executed
def task1():
    print("Running task 1")

def task2():
    print("Running task 2")

def task3():
    print("Running task 3")

# Create Airflow tasks that run the functions
task1 = PythonOperator(
    task_id='task1',
    python_callable=task1,
    dag=dag,
)

task2 = PythonOperator(
    task_id='task2',
    python_callable=task2,
    dag=dag,
)

task3 = PythonOperator(
    task_id='task3',
    python_callable=task3,
    dag=dag,
)

# Set task dependencies to define the execution order
task1 >> task2 >> task3
