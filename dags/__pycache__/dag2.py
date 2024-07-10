from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Define a function to be executed by the PythonOperator
def print_hello():
    print("Hello, Airflow! 2nd DAG")

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'DAG_2',
    default_args=default_args,
    description='A simple hello world DAG',
)
start = DummyOperator(task_id='start')
end = DummyOperator(task_id = 'end')
# Define the task
hello_task = BashOperator(
    task_id='print_hello',
    bash_command="sleep 59",
    dag=dag,
)

# Set the task in the DAG
start >> hello_task >> end
