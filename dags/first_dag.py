from datetime import timedelta 
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
# from airflow.operators.bash_operator import BaseOperator

# def first_function_execute(**kwargs):
#     variable = kwargs.get("name","didn't find the name")
#     print("Hello World {}".format(variable))
#     return "hello world " + variable

def first_function_execute(**context):
    print("first function execution")
    context['ti'].xcom_push(key='mykey',value='first_function-execute say hello')

def second_function_execute(**context):
    instance = context.get('ti').xcom_pull(key='mykey')
    print("I am in second function execution got value : {} from function 1".format(instance))

with DAG(
dag_id= "first_dag",
schedule_interval="@daily",
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay":timedelta(minutes=5),
    "start_date": datetime(2023,1,28)
    },
catchup=False
) as f:
    # first_function_execute =PythonOperator(
    # task_id='first_function_execute',
    # python_callable=first_function_execute,
    # op_kwargs ={"name":"Arun"}
    # )
    
    first_function_execute =PythonOperator(
    task_id='first_function_execute',
    python_callable=first_function_execute,
    provide_context=True,
    op_kwargs ={"name":"Arun"}
    )

    second_function_execute =PythonOperator(
    task_id='second_function_execute',
    python_callable= second_function_execute,
    provide_context=True
    )

first_function_execute >> second_function_execute