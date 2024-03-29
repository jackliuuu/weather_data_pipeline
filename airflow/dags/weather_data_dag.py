from airflow import DAG
from airflow.decorators import task
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
import sys
load_dotenv('/home/jack/weather_data_pipeline/.env')
from weather_etl import WeatherPipeline


locations = ["London", "Tokyo", "Sydney", "Paris", "Berlin", "Moscow", "Madrid", "Rome", "Cairo"]
weather = WeatherPipeline(locations)

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = 'load_weather_data',
    default_args=default_args, 
    start_date = datetime(2023,8,12), 
    schedule_interval='@daily', 
    catchup=False
) as dag:

    # Task #1 - Extract data
    @task
    def extract_weather_data():
        weather.extract_weather_data()

    # Task #2 - load_to_cloudStorage
    @task
    def load_to_cloudStorage():
        weather.getOrCreate_S3bucket()
        weather.load_to_S3bucket()
    # Task #3 - load_to_athena
    @task
    def load_to_athena(dataset_name, table_name):
        df=weather.processData()
        weather.createAthenaTable(df,CURRENT_DATE)


    # Dependencies
    extract_weather_data() >> load_to_cloudStorage() >> load_to_athena("weather", "weather")
