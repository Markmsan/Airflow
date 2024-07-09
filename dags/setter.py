from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime

# Function to set an Airflow variable
def set_airflow_variable(**kwargs):
    Variable.set("dag_conf", {"param1": "value1", "param2": "value2"})
    print("DAG Configuration set in Airflow UI Variables")

# Create the DAG
dag = DAG(
    'example_dag_conf',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 7, 9),
    },
    schedule_interval='@daily',
)

# Create a task that calls the set_airflow_variable function
set_variable_task = PythonOperator(
    task_id='set_variable_task',
    python_callable=set_airflow_variable,
    provide_context=True,
    dag=dag,
)

# Define the task dependencies
set_variable_task
