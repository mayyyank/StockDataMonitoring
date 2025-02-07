import sys
import threading

from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator

dag_arg={
    'owner':'Mayank',
    'retries':'3',
    'retry_delay':timedelta(minutes=2)
}

dag1=DAG(
    dag_id='StockDataAnalysis',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 29),
    catchup=True,
    default_args=dag_arg
)
sys.path.insert(0,'/home/sunbeam/Desktop/BigData_Project')

from Producer import kafkaProducer
from Consumer import kafkaConsumer
from MergeFiles import merge_files
from SparkToHive import sparkHive

def start_consumer():
    consumer_thread=threading.Thread(target=kafkaConsumer)
    consumer_thread.start()


def start_producer():
    kafkaProducer()

def start_mergeFiles():
    merge_files()

def start_spartToHive():
    sparkHive()


task1=PythonOperator(
    task_id='Consumer',
    python_callable=start_consumer,
    dag=dag1
)


task2=PythonOperator(
    task_id='Producer',
    python_callable=start_producer,
    dag=dag1
)

task3=PythonOperator(
    task_id='MergeFiles',
    python_callable=start_mergeFiles,
    dag=dag1
)

task4=PythonOperator(
    task_id='SavetoHive',
    python_callable=start_spartToHive,
    dag=dag1
)

task1 >> task2 >> task3 >> task4