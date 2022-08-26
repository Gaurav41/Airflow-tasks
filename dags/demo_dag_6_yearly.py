from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
import logging 
from pprint import pprint

log = logging.getLogger(__name__)

# H:\airflow-docker>curl -X GET --user "airflow:airflow" "http://localhost:8080/api/v1/dags/~/dagRuns/~/taskInstances?state=success"
def print_context(ds=None, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'



default_args={
    'owner':'airflow',
    'start_date':datetime(2022,8,12),
    'depends_on_past':False,
    'retries':0
}

dag = DAG(
    dag_id='demo_dag_6_yearly',
    default_args=default_args,
    catchup=False,
    schedule_interval='@yearly')

start = DummyOperator(
    task_id='start',
    dag=dag
    )

end = DummyOperator(
    task_id='end',
    dag=dag
    )



python_task = PythonOperator(task_id='python_task', python_callable=print_context, dag=dag)
start >> python_task >> end