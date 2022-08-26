from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
import logging 
import time
from pprint import pprint

log = logging.getLogger(__name__)

# H:\airflow-docker>curl -X GET --user "airflow:airflow" "http://localhost:8080/api/v1/dags/~/dagRuns/~/taskInstances?state=success"
def print_context(ds=None, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

def task_1_fn(**kwargs):
    print("task_1_fn started")
    time.sleep(1)
    print("task_1_fn finished")
    return "task_1 executed successfully"

def task_2_fn(**kwargs):
    print("task_2_fn started")
    time.sleep(1)
    print("task_2_fn finished")
    return "task_2 executed successfully"


def task_3_fn(**kwargs):
    print("task_3_fn started")
    time.sleep(2)
    print("task_3_fn finished")
    return "task_3 executed successfully"

def task_4_fn(**kwargs):
    print("task_4_fn started")
    time.sleep(2)
    print("task_4_fn finished")
    return "task_4 executed successfully"

def task_5_fn(**kwargs):
    print("task_5_fn started")
    time.sleep(3)
    print("task_5_fn finished")
    return "task_5 executed successfully"

def task_6_fn(**kwargs):
    print("task_6_fn started")
    time.sleep(2)
    print("task_6_fn finished")
    return "task_6 executed successfully"


def task_7_fn(**kwargs):
    print("task_7_fn started")
    time.sleep(1)
    print("task_7_fn finished")
    return "task_7 executed successfully"

default_args={
    'owner':'airflow',
    'start_date':datetime(2022,8,12),
    'depends_on_past':False,
    'retries':0
}

dag = DAG(
    dag_id='councurrent_tasks_2',
    default_args=default_args,
    catchup=False,
    schedule_interval=None
    )

start = DummyOperator(
    task_id='start',
    dag=dag
    )

task_1 = PythonOperator(
        task_id="task_1",
        dag=dag,
        python_callable=task_1_fn,
        provide_context=True     
    )

task_2 = PythonOperator(
        task_id="task_2",
        dag=dag,
        python_callable=task_2_fn,
        provide_context=True     
    )


task_3 = PythonOperator(
        task_id="task_3",
        dag=dag,
        python_callable=task_3_fn,
        provide_context=True     
    )

task_4 = PythonOperator(
        task_id="task_4",
        dag=dag,
        python_callable=task_4_fn,
        provide_context=True     
    )



task_5 = PythonOperator(
        task_id="task_5",
        dag=dag,
        python_callable=task_5_fn,
        provide_context=True     
    )

task_6 = PythonOperator(
        task_id="task_6",
        dag=dag,
        python_callable=task_6_fn,
        provide_context=True     
    )


task_7 = PythonOperator(
        task_id="task_7",
        dag=dag,
        python_callable=task_7_fn,
        provide_context=True     
    )


end = DummyOperator(
    task_id='end',
    dag=dag
    )



python_task = PythonOperator(task_id='python_task', python_callable=print_context, dag=dag)
start >> [task_1, task_2] 
task_1 >> task_3
task_3 >> [task_4, task_5]
task_4 >> task_7
task_5 >> task_6 >> task_7 >>end
