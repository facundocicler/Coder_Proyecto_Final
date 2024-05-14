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

def send_alert(**context):

csv_filename = context['ti'].xcom_pull(task_ids='extract_data')
    df = pd.read_csv(csv_filename)
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