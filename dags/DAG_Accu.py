from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow.models import Variable
from AccuWeather_ETL import extract_data, tables_redshift, insert_data
from Alert_Msg import check_weather, send_alert
import os
import pandas as pd
import json
import smtplib


dag_path = os.getcwd()
default_args = {
    'owner': 'Cicler',
    'start_date': datetime(2024, 3, 31),
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

weather_dag = DAG(
    dag_id='weather_dag',
    default_args=default_args,
    description='Agrega datos de 150 ciudades',
    schedule_interval=timedelta(days=1),
    catchup=False
)

connection_task = PythonOperator(
    task_id='connection',
    python_callable=tables_redshift,
    dag=weather_dag,
)

extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=get_data,
    provide_context=True,
    dag=weather_dag,
)

check_weather_task = PythonOperator(
    task_id='check_weather',
    python_callable=check_weather,
    provide_context=True,
    dag=weather_dag,
)

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=branch_func,
    provide_context=True,
    dag=weather_dag,
)

send_alert_task = PythonOperator(
    task_id='send_alert',
    python_callable=send_alert,
    provide_context=True,
    dag=weather_dag,
)

no_alert_needed_task = DummyOperator(
    task_id='no_alert_needed',
    dag=weather_dag,
)


insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    provide_context=True,
    dag=weather_dag,
)

extract_data_task >> connection_task >> check_weather_task >> branch_task
branch_task >> send_alert_task >> insert_data_task
branch_task >> no_alert_needed_task >> insert_data_task
