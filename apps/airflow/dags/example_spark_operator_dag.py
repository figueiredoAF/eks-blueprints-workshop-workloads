from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.models import Variable
from kubernetes.client import models as k8s
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

def pipe_init():
    print(f'INICIALIZANDO PIPELINE')

def pipe_end():
    print(f'FINALIZANDO PIPELINE')

default_args={
   'depends_on_past': False,
   'email': ['abcd@gmail.com'],
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=5)
}
with DAG(
   'example_spark_dag',
   default_args=default_args,
   description='simple dag',
   schedule_interval='@once',
   start_date=datetime(2023,4,13),
   catchup=False,
   tags=['getting started', 'spark-operator']
) as dag:
   t_init = PythonOperator(
        task_id = 'pipe_init',
        python_callable=pipe_init,
        op_kwargs={}
    )

   t_process = SparkKubernetesOperator(
       task_id='spark_process',
       trigger_rule="all_success",
       depends_on_past=False,
       retries=3,
       application_file="spark_py_pi_application.yaml",
       namespace="spark-operator",
       kubernetes_conn_id="k8s_data_eng_local",
       api_group="sparkoperator.k8s.io",
       api_version="v1beta2",
       do_xcom_push=True,
       dag=dag
   )

   t_end = PythonOperator(
        task_id = 'pipe_end',
        python_callable=pipe_end,
        op_kwargs={}
    )

   # Dependencies / Orchestrator
   t_init >> t_process >> t_end
