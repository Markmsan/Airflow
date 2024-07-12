from airflow import DAG
import json
from airflow.models import Variable, DagRun
from airflow.utils.state import State
from airflow.utils.db import provide_session
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.utils.db import provide_session
from datetime import datetime
from airflow.utils.timezone import make_aware
from airflow.operators.python import BranchPythonOperator



# Get the number of max DAG count to run
max_dag_count = int(Variable.get("max_dag_run"))
dags_config = json.loads(Variable.get("dag_conf"))

@provide_session
def get_task_trigger_count( dag_id, session=None):
    now = make_aware(datetime.now())
    start_of_day = make_aware(datetime(now.year, now.month, now.day, 0, 0, 0))
    end_of_day = make_aware(datetime(now.year, now.month, now.day, 23, 59, 59))

    task_count = session.query(DagRun).filter(
        DagRun.dag_id == dag_id,
        DagRun.execution_date >= start_of_day,
        DagRun.execution_date < end_of_day,
    ).count()

    print(f"The DAG {dag_id} was triggered {task_count} times today.")
    return task_count

@provide_session
def get_runnning_dag_count(session=None):
    return session.query(DagRun).filter(DagRun.state == State.RUNNING).count()

def schedule_dags():

    to_trigger = []
    to_skip = []
    
    runnin_dag_count = get_runnning_dag_count()
    dags_to_trigger = max_dag_count-runnin_dag_count
    to_skip += [dag['name'] for dag in dags_config if not dag['enabled']]

    enabled_dags = [dag for dag in dags_config if dag['enabled']]
    sorted_dag = sorted(enabled_dags, key=lambda d: d['priority'],reverse=True)

    for dag in sorted_dag:
        if len(to_trigger) == dags_to_trigger or get_task_trigger_count(dag["name"]) == 1:
            to_skip.append(dag['name'])
        else:
            to_trigger.append(dag["name"])

    return to_trigger, to_skip

def branch_operator(dag_id, condition):
    if get_task_trigger_count(dag_id) == 1:
        return [f'already_triggered_{dag_id}']
    else:
        if condition == False:
            return [f'skip_{dag_id}']
        else:
            return [f'trigger_{dag_id}']

def print_id(a,b):
    runnin_dag_count = get_runnning_dag_count()
    dags_to_trigger = max_dag_count-runnin_dag_count
    print(f'Number of Dags that will be tirggered:{dags_to_trigger}\n')
    print(f'Dags to trigger{a}\n')
    print(f'Dags no to trigger{b}\n')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='DAG_master_branch',
    default_args=default_args,
    schedule_interval=None,  # Set to None to disable scheduling; adjust as needed
    catchup=False,
) as dag:

    trigger_dag_id, no_trigger_dag_id = schedule_dags()
    # Define the start Python operator
    start = PythonOperator(
        task_id='start',
        python_callable=print_id,
        op_args=[trigger_dag_id,no_trigger_dag_id]
    )
     # Define the end dummy operator
    end = DummyOperator(
        task_id='end'
    )

  
    for dag_id in trigger_dag_id:
        # Define the Python operator
        branching  = BranchPythonOperator(
            task_id=f'{dag_id}_branch',
            python_callable=branch_operator,
            op_args=[dag_id, True]
        )
        # Define the Trigger operator
        trigger = TriggerDagRunOperator(
            task_id=f'trigger_{dag_id}',
            trigger_dag_id=dag_id,
            wait_for_completion=False,
        )

        # Define the Dummy operators
        already_triggered = DummyOperator(
            task_id=f'already_triggered_{dag_id}'
        )
        skip=DummyOperator(
            task_id=f'skip_{dag_id}'
        )
        finish = DummyOperator(
            task_id = f'Conditional_branching_finished_{dag_id}',
            trigger_rule = 'one_success'
        )

        # Set up the task dependencies
        start >> branching >> [already_triggered, trigger, skip] >> finish >> end

    for dag_id in no_trigger_dag_id:
        # Define the Python operator
        branching  = BranchPythonOperator(
            task_id=f'{dag_id}_branch',
            python_callable=branch_operator,
            op_args=[dag_id, False]
        )
        # Define the Trigger operator
        trigger = TriggerDagRunOperator(
            task_id=f'trigger_{dag_id}',
            trigger_dag_id=dag_id,
            wait_for_completion=False,
        )

        # Define the Dummy operators
        already_triggered = DummyOperator(
            task_id=f'already_triggered_{dag_id}'
        )
        skip=DummyOperator(
            task_id=f'skip_{dag_id}'
        )
        finish = DummyOperator(
            task_id = f'Conditional_branching_finished_{dag_id}',
            trigger_rule = 'one_success'
        )

        # Set up the task dependencies
        start >> branching >> [already_triggered, trigger, skip] >> finish >> end
   
   