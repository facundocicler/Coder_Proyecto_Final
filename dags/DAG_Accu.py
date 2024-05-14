from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow.models import Variable
from AccuWeather_ETL import get_data, tables_redshift, insert_data
from Alert_Msg import check_and_send_alert
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

def check_and_send_alert(**context):
    csv_filename = context['ti'].xcom_pull(task_ids='extract_data')
    df = pd.read_csv(csv_filename)

    with open('dags/config.json', 'r') as json_config:
        try:
            config_data = json.load(json_config)
            min_temp = config_data['celsius_temperature_range']['min_temp']
            max_temp = config_data['celsius_temperature_range']['max_temp']
        except Exception as e:
            print(f'Error al cargar el archivo JSON: {str(e)}')

    city = df['city']
    country = df['country']
    temperature = df['temperature_metric_value']
    alert_messages = []

    for city_name, country_name, temp in zip(city, country, temperature):
        if temp < min_temp or temp > max_temp:
            alert_message = f"{city_name}, {country_name} - {temp} °C"
            alert_messages.append(alert_message)

    if alert_messages:
        alert_body = "\n".join(alert_messages)
        smtp_server = 'smtp.gmail.com'
        smtp_port = 587
        sender_email = Variable.get("EMAIL")
        password = Variable.get("EMAIL_PASSWORD")

        subject = f'Alerta de Temperatura de Ciudades Fuera del Rango de {min_temp} °C - {max_temp} °C'
        message = alert_body

        try:
            body_text = f"\n{message}"
            msg = MIMEMultipart()
            msg['To'] = sender_email
            msg['From'] = sender_email
            msg['Subject'] = subject
            msg.attach(MIMEText(body_text, 'plain'))

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(sender_email, password)
                server.send_message(msg)
            print('El email fue enviado con éxito')

        except Exception as e:
            print(f'Error al enviar el correo electrónico: {str(e)}')

    else:
        print("No se requiere alerta de temperatura.")

check_and_send_alert_task = PythonOperator(
    task_id='check_and_send_alert',
    python_callable=check_and_send_alert,
    provide_context=True,
    dag=weather_dag,
)

extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=get_data,
    provide_context=True,
    dag=weather_dag,
)

connection_task = PythonOperator(
    task_id='connection',
    python_callable=tables_redshift,
    dag=weather_dag,
)

insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    provide_context=True,
    dag=weather_dag,
)

extract_data_task >> connection_task >> check_and_send_alert_task >> insert_data_task
