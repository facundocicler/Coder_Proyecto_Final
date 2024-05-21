import requests
import pandas as pd
from psycopg2.extras import execute_values
from airflow.models import Variable
import datetime
import psycopg2

def connection_redshift():
    try:
        # Obtener valores de las variables de Airflow
        redshift_host = Variable.get("REDSHIFT_HOST")
        redshift_db = Variable.get("REDSHIFT_DB")
        redshift_user = Variable.get("REDSHIFT_USER")
        redshift_password = Variable.get("REDSHIFT_PASSWORD")
        redshift_port = Variable.get("REDSHIFT_PORT")

        # Conectar a Redshift usando los valores obtenidos
        conn = psycopg2.connect(
            host=redshift_host,
            dbname=redshift_db,
            user=redshift_user,
            password=redshift_password,
            port=redshift_port
        )
        print("Conectado a Redshift")
        return conn

    except Exception as e:
        print("La conexión ha fallado")
        print(e)
        return None

def extract_data(**context):
    columns = [
        'key', 'city', 'country_id', 'country', 'timezone_code', 'name', 'gmt_offset', 
        'is_daylight_saving', 'next_offset_change', 'localobservation_datetime', 'epochtime', 
        'latitude', 'longitude', 'elevation_metric_value', 'elevation_metric_unit', 
        'elevation_imperial_value', 'elevation_imperial_unit', 'weather_text', 'weather_icon', 
        'has_precipitation', 'precipitation_type', 'is_day_time', 'temperature_metric_value', 
        'temperature_metric_unit', 'temperature_imperial_value', 'temperature_imperial_unit'
    ]
    df_final = []

    api_url = Variable.get("API_URL")
    api_key = Variable.get("API_KEY")
    url = f'{api_url}?apikey={api_key}&language=en-us'

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud: {e}")
        return

    for entry in data:
        row_data = {
            'key': entry['Key'],
            'city': entry['LocalizedName'],
            'country_id': entry['Country']['ID'],
            'country': entry['Country']['LocalizedName'],
            'timezone_code': entry['TimeZone']['Code'],
            'name': entry['TimeZone']['Name'],
            'gmt_offset': entry['TimeZone']['GmtOffset'],
            'is_daylight_saving': entry['TimeZone']['IsDaylightSaving'],
            'next_offset_change': entry['TimeZone']['NextOffsetChange'],
            'localobservation_datetime': entry['LocalObservationDateTime'],
            'epochtime': entry['EpochTime'],
            'latitude': entry['GeoPosition'].get('Latitude', None),
            'longitude': entry['GeoPosition'].get('Longitude', None),
            'elevation_metric_value': entry['Temperature']['Metric'].get('Value', None),
            'elevation_metric_unit': entry['Temperature']['Metric'].get('Unit', None),
            'elevation_imperial_value': entry['Temperature']['Imperial'].get('Value', None),
            'elevation_imperial_unit': entry['Temperature']['Imperial'].get('Unit', None),
            'weather_text': entry['WeatherText'],
            'weather_icon': entry['WeatherIcon'],
            'has_precipitation': entry['HasPrecipitation'],
            'precipitation_type': entry['PrecipitationType'],
            'is_day_time': entry['IsDayTime'],
            'temperature_metric_value': entry['Temperature']['Metric'].get('Value', None),
            'temperature_metric_unit': entry['Temperature']['Metric'].get('Unit', None),
            'temperature_imperial_value': entry['Temperature']['Imperial'].get('Value', None),
            'temperature_imperial_unit': entry['Temperature']['Imperial'].get('Unit', None)
        }
        df_final.append(row_data)

    df_final = pd.DataFrame(df_final, columns=columns)

    csv_filename = f"{context['ds']}_weather_data.csv"
    df_final.to_csv(csv_filename, index=False)

    context['ti'].xcom_push(key='csv_filename', value=csv_filename)
    return csv_filename


def tables_redshift():

    conn = connection_redshift()

    # Lista de TABLAS
    sql_queries = [
        """
        CREATE TABLE IF NOT EXISTS cicler_facundo_coderhouse.City (
            key INT,
            city VARCHAR(255),
            country_id VARCHAR(2),
            timezone_code VARCHAR(5),
            ingestion_datetime DATETIME,
            PRIMARY KEY (key, country_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS cicler_facundo_coderhouse.Country (
            country_id VARCHAR(2) PRIMARY KEY,
            country VARCHAR(255),
            ingestion_datetime DATETIME
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS cicler_facundo_coderhouse.TimeZone (
            timezone_code VARCHAR(5) PRIMARY KEY,
            name VARCHAR(255),
            gmt_offset INT,
            is_daylight_saving BOOLEAN,
            next_offset_change TIMESTAMP,
            ingestion_datetime DATETIME
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS cicler_facundo_coderhouse.Local_Observation_DateTime (
            key INT PRIMARY KEY,
            local_observation_datetime TIMESTAMP WITH TIME ZONE,
            epochtime INT,
            ingestion_datetime DATETIME
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS cicler_facundo_coderhouse.GeoPosition (
            key INT PRIMARY KEY,
            latitude FLOAT,
            longitude FLOAT,
            elevation_metric_value INT,
            elevation_metric_unit VARCHAR(10),
            elevation_imperial_value INT,
            elevation_imperial_unit VARCHAR(10),
            ingestion_datetime DATETIME
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS cicler_facundo_coderhouse.Weather (
            key INT PRIMARY KEY,
            weather_text VARCHAR(255),
            weather_icon INT,
            has_precipitation BOOLEAN,
            precipitation_type VARCHAR(255),
            is_day_time BOOLEAN,
            temperature_metric_value FLOAT,
            temperature_metric_unit VARCHAR(5),
            temperature_imperial_value FLOAT,
            temperature_imperial_unit VARCHAR(5),
            ingestion_datetime DATETIME
        );
        """
    ]

    # Lista de FOREIGN KEYS
    foreign_keys = [
        """
        ALTER TABLE cicler_facundo_coderhouse.City
        ADD CONSTRAINT fk_country_id FOREIGN KEY (country_id)
        REFERENCES cicler_facundo_coderhouse.Country(country_id);
        """,
        """
        ALTER TABLE cicler_facundo_coderhouse.City
        ADD CONSTRAINT fk_timezone_code FOREIGN KEY (timezone_code)
        REFERENCES cicler_facundo_coderhouse.TimeZone(timezone_code);
        """,
        """
        ALTER TABLE cicler_facundo_coderhouse.City
        ADD CONSTRAINT fk_local_observation_datetime_key FOREIGN KEY (key)
        REFERENCES cicler_facundo_coderhouse.Local_Observation_DateTime(key);
        """,
        """
        ALTER TABLE cicler_facundo_coderhouse.City
        ADD CONSTRAINT fk_geoposition_key FOREIGN KEY (key)
        REFERENCES cicler_facundo_coderhouse.GeoPosition(key);
        """,
        """
        ALTER TABLE cicler_facundo_coderhouse.City
        ADD CONSTRAINT fk_weather_key FOREIGN KEY (key)
        REFERENCES cicler_facundo_coderhouse.Weather(key);
        """
    ]

    try:
        with conn.cursor() as cur:
            for query in sql_queries:
                cur.execute(query)
            conn.commit()
        with conn.cursor() as cur:
            for fk_query in foreign_keys:
                # Extrae el nombre de la FK de la consulta ALTER TABLE
                constraint_name = fk_query.split('CONSTRAINT')[1].split('FOREIGN KEY')[0].strip()
                # Verificar si la FK ya existe antes de ejecutar la consulta
                cur.execute(
                    "SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_type = 'FOREIGN KEY' AND table_name = 'city' AND constraint_name = %s",
                    (constraint_name,))
                result = cur.fetchone()
                if result[0] == 0:
                    cur.execute(fk_query)
            conn.commit()
        conn.close()
    except Exception as e:
        conn.rollback()
        print(f"Error durante la ejecucion: {str(e)}")
        conn.close()


def insert_data(**context):

    conn = connection_redshift()
    
    csv_filename = context['ti'].xcom_pull(task_ids='extract_data')
    city_df = pd.read_csv(csv_filename)

    current_datetime = datetime.datetime.now()

    city_df['ingestion_datetime'] = current_datetime
    # Elimina duplicados en la tabla Country
    unique_country_df = city_df[['Country_ID', 'Country', 'ingestion_datetime']].drop_duplicates()

    with conn.cursor() as cur:
        # Tabla City
        city_values = city_df[['Key', 'City', 'Country_ID', 'TimeZone_Code', 'ingestion_datetime']].values.tolist()
        execute_values(
            cur,
            '''
            INSERT INTO cicler_facundo_coderhouse.City ("key", "city", "country_id", "timezone_code", "ingestion_datetime")
            VALUES %s
            ''',
            city_values
        )
        
        # Tabla Country
        country_values = unique_country_df[['Country_ID', 'Country', 'ingestion_datetime']].values.tolist()
        execute_values(
            cur,
            '''
            INSERT INTO cicler_facundo_coderhouse.Country ("country_id", "country", "ingestion_datetime")
            VALUES %s
            ''',
            country_values
        )
        
        # Tabla TimeZone
        timezone_values = city_df[['TimeZone_Code', 'Name', 'Gmt_Offset', 'Is_Daylight_Saving', 'Next_Offset_Change', 'ingestion_datetime']].values.tolist()
        execute_values(
            cur,
            '''
            INSERT INTO cicler_facundo_coderhouse.TimeZone ("timezone_code", "name", "gmt_offset", "is_daylight_saving", "next_offset_change", "ingestion_datetime")
            VALUES %s
            ''',
            timezone_values
        )
        
        # Tabla Local_Observation_DateTime
        local_observation_values = city_df[['Key', 'LocalObservation_DateTime', 'EpochTime', 'ingestion_datetime']].values.tolist()
        execute_values(
            cur,
            '''
            INSERT INTO cicler_facundo_coderhouse.Local_Observation_DateTime ("key", "local_observation_datetime", "epochtime", "ingestion_datetime")
            VALUES %s
            ''',
            local_observation_values
        )
        
        # Tabla GeoPosition
        geoposition_values = city_df[['Key', 'Latitude', 'Longitude', 'Elevation_Metric_Value', 'Elevation_Metric_Unit', 'Elevation_Imperial_Value', 'Elevation_Imperial_Unit', 'ingestion_datetime']].values.tolist()
        execute_values(
            cur,
            '''
            INSERT INTO cicler_facundo_coderhouse.GeoPosition ("key", "latitude", "longitude", "elevation_metric_value", "elevation_metric_unit", 
            "elevation_imperial_value", "elevation_imperial_unit", "ingestion_datetime")
            VALUES %s
            ''',
            geoposition_values
        )
        
        # Tabla Weather
        weather_values = city_df[['Key', 'Weather_Text', 'Weather_Icon', 'Has_Precipitation', 'Precipitation_Type', 'Is_Day_Time', 'Temperature_Metric_Value', 'Temperature_Metric_Unit', 'Temperature_Imperial_Value', 'Temperature_Imperial_Unit', 'ingestion_datetime']].values.tolist()
        execute_values(
            cur,
            '''
            INSERT INTO cicler_facundo_coderhouse.Weather ("key", "weather_text", "weather_icon", "has_precipitation", "precipitation_type", "is_day_time",
            "temperature_metric_value", "temperature_metric_unit", "temperature_imperial_value", "temperature_imperial_unit", "ingestion_datetime")
            VALUES %s
            ''',
            weather_values
        )
        
        conn.commit()
    conn.close()     
