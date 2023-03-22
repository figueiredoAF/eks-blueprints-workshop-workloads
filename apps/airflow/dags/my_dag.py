from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime, timedelta

MY_NAME = "figueiredoaf1"
MY_NUMBER = 19

def multiply_by_23(number):
    '''Multiplies a number by 23 and prints the result to Airflow logs'''
    result = number * 23
    print(f'{number:.2f} * 23 = {result:.2f}')

with DAG(
    dag_id='my_first_dag', # The name of the DAG that appears in the Airflow UI
    start_date=datetime(2023,3,22), # The date and time when the DAG is scheduled to start running
    schedule_interval=timedelta(minutes=15), # The frequency the DAG runs. You can define this as a timedelta object, a CRON expression, or as a macro such as "@daily".
    catchup=False, # Defines whether the DAG reruns all DAG runs that were scheduled before today's date.
    tags=['getting started', 'multiply'],
    default_args={ # A list of configurations for the DAG's behavior. 
        "owner":MY_NAME,
        "retries":2,
        "retry_delay":timedelta(minutes=5)
    }
    
) as dag:
     # Start mark operator
    start = EmptyOperator(
        task_id='start'
    )

    # Task#1
    t1 = BashOperator(
        task_id = 'say_my_name',
        bash_command=f'echo {MY_NAME}'
    )
    
    # Task#2
    t2 = PythonOperator(
        task_id = 'multiply_my_number_by_23',
        python_callable=multiply_by_23,
        op_kwargs={"number":MY_NUMBER}
    )

    # End mark operator
    end = EmptyOperator(
        task_id='end'
    )
    
    # Dependencies / Orchestrator
    start >> t1 >> t2 >> end