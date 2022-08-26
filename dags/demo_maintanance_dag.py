"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.
 
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
from airflow import DAG, settings
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.jobs.base_job import BaseJob
from airflow.models import DAG, DagModel, DagRun, ImportError, Log, SlaMiss, RenderedTaskInstanceFields, TaskFail, TaskInstance, TaskReschedule, Variable, XCom
from datetime import datetime, timedelta
import os
from sqlalchemy import and_ 
from airflow.models.param import Param

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['gauravpingale4@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}
DAG_PATTERNS = (Variable.get("DAG_PATTERNS",default_var="demo_maintanance_dag")).split(",")
MAX_AGE_IN_DAYS = int(Variable.get("MAX_METADATA_STOREAGE_DAYS", default_var=60))

# DAG_PATTERN ="dag_1_once"
 
OBJECTS_TO_CLEAN = [[BaseJob,BaseJob.latest_heartbeat], 
    [DagModel,DagModel.last_parsed_time], 
    [DagRun,DagRun.execution_date], 
    # [ImportError,ImportError.timestamp],
    [Log,Log.dttm], 
    [SlaMiss,SlaMiss.execution_date], 
    [RenderedTaskInstanceFields,RenderedTaskInstanceFields.execution_date], 
    [TaskFail,TaskFail.end_date], 
    [TaskInstance, TaskInstance.execution_date],
    [TaskReschedule, TaskReschedule.execution_date],
    [XCom,XCom.execution_date],     
]
 
# def cleanup_db_fn(**kwargs):
#     session = settings.Session()
#     print("session: ",str(session))
 
#     # oldest_date = days_ago(int(Variable.get("max_metadb_storage_days", default_var=DEFAULT_MAX_AGE_IN_DAYS)))
#     oldest_date = days_ago(0)
    
#     print("oldest_date: ",oldest_date)
#     print("DAG_PATTERN: ",DAG_PATTERN)

 
#     for x in OBJECTS_TO_CLEAN:
#         # query = session.query(x[0]).filter(x[1] <= oldest_date)
#         # print(str(x[0]),": ",(query.all()))
#         # print("*"*200)
#         try:
#             result = session.query(x[0]).filter(and_(x[0].dag_id.like(f"%{DAG_PATTERN}%"),x[1] <= oldest_date))
#             # result = session.query(x[0]).filter((x[0].dag_id.like(f"%{DAG_PATTERN}%")))

#             # result = session.query(x[0]).filter(x[1] <= oldest_date)
#             # print(result.label("abc"))

#             print(str(x[0]),":",result.all())
#             result.delete(synchronize_session=False)
#         except AttributeError:
#             print(f"{x[0]} doest not have dag_id ")
#             result = session.query(x[0]).filter(x[1] <= oldest_date)
#             print(str(x[0]),":",result.all())
#             result.delete(synchronize_session=False)
#         print("*"*100)
        
 
#     session.commit()
 
#     return "OK"
 
def cleanup_db_fn(**kwargs):
    session = settings.Session()
    print("session: ",str(session))
 
    oldest_date = days_ago(MAX_AGE_IN_DAYS)
    print("oldest_date: ",oldest_date)
    print("DAG_PATTERN: ",DAG_PATTERNS)
    
    for dag_pattern in DAG_PATTERNS:
        print("dag_pattern: ",dag_pattern)
        for x in OBJECTS_TO_CLEAN:
            
            query = session.query(x[0]).filter(and_(x[0].dag_id.like(f"%{dag_pattern}%"),x[1] <= oldest_date))
            count = query.count()
            print(str(x[0]),": ",str(query.all()))
            print(f"Cleaning {x[0]}, {count} rows...")
            # query.delete(synchronize_session=False)

           
    x= [ImportError,ImportError.timestamp]         
    query = session.query(x[0]).filter(x[1] <= oldest_date)
    # query = session.query(x[0])
    print(str(x[0]),": ",str(query.all()))
    # query.delete(synchronize_session=False)



        # query = session.query(x[0]).filter(x[1] <= oldest_date)
        # print(str(x[0]),": ",str(query.all()))
        # query.delete(synchronize_session=False)
 
    session.commit()
 
    return "OK"

# def cleanup_db_fn(**kwargs):
#     session = settings.Session()
#     print("session: ",str(session))
 
#     # oldest_date = days_ago(int(Variable.get("max_metadb_storage_days", default_var=DEFAULT_MAX_AGE_IN_DAYS)))
#     oldest_date = days_ago(0)
    
#     print("oldest_date: ",oldest_date)
 
#     query = session.query(DagRun).filter(DagRun.dag_id == 'demo_maintanance_dag')
#     print((query.all()))
#     print("*"*200)

#     result = session.query(DagRun).all()
#     print(result)
#     print("*"*200)
    
#     # query.delete(synchronize_session=False)
 
#     session.commit()
 
#     return "OK"
with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    # start_date=days_ago(1),
    start_date = datetime(2022,8,12),
    schedule_interval=None,
    tags=['db'],
    params={"dag": Param('_full_data_run', type="string",minLength=2)},
) as dag:
 
    cleanup_db = PythonOperator(
        task_id="cleanup_db",
        python_callable=cleanup_db_fn,
        provide_context=True     
    )
