from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Import the producer function we defined earlier
from produce_weather_to_kafka import fetch_and_produce

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='open_meteo_weather_pipeline',
    default_args=default_args,
    description='Orchestrate Open-Meteo API ingestion via Kafka, PySpark, and PostgreSQL',
    schedule_interval='@hourly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Fetch weather data from Open-Meteo API and send to Kafka
    ingest_to_kafka = PythonOperator(
        task_id='fetch_api_to_kafka',
        python_callable=fetch_and_produce,
    )

    # Task 2: Submit PySpark job to clean Kafka messages and load into PostgreSQL staging table
    run_spark_processing = SparkSubmitOperator(
        task_id='pyspark_clean_and_stage',
        application='/opt/airflow/scripts/clean_and_load.py',
        conn_id='spark_default',
        verbose=True,
    )

    # Task 3: Perform atomic Upsert from PostgreSQL Staging Table to Production Table
    upsert_postgres = PostgresOperator(
        task_id='postgres_upsert',
        postgres_conn_id='postgres_default',
        sql="""
            INSERT INTO production_weather (city, latitude, longitude, time, temperature_c, relative_humidity, wind_speed_kmh, weather_code, processed_at)
            SELECT city, latitude, longitude, time, temperature_c, relative_humidity, wind_speed_kmh, weather_code, processed_at
            FROM staging_weather
            ON CONFLICT (city, time) 
            DO UPDATE SET 
                temperature_c = EXCLUDED.temperature_c,
                relative_humidity = EXCLUDED.relative_humidity,
                wind_speed_kmh = EXCLUDED.wind_speed_kmh,
                weather_code = EXCLUDED.weather_code,
                processed_at = EXCLUDED.processed_at;
        """,
    )

    # Define the execution order
    ingest_to_kafka >> run_spark_processing >> upsert_postgres