import requests
from dotenv import load_dotenv
import boto3
import json
import pandas as pd
import os
import datetime
load_dotenv(".env")
CURRENT_DATE=datetime.datetime.now().strftime("%Y-%m-%d").replace("-","_")
aws_key=os.getenv("AWS_ACCESS_KEY_ID")
aws_secret=os.getenv("AWS_SECRET_ACCESS_KEY")
weather_api_key=os.getenv("API_KEY")
S3=boto3.client('s3',
                aws_access_key_id=aws_key,
                aws_secret_access_key=aws_secret)
class WeatherPipeline:
    def __init__(self,locations=None) -> None:
        self.locations=locations
    def extract_weather_data(self):
        '''
        extract weather data from api
        '''
        if not os.path.exists(f"/home/jack/weather_data_pipeline/airflow/data/{CURRENT_DATE}"):
            os.makedirs(f"/home/jack/weather_data_pipeline/airflow/data/{CURRENT_DATE}")
        for location in self.locations:
            url = f"http://api.weatherapi.com/v1/current.json?key={weather_api_key}&q={location}&aqi=no"
            response=requests.get(url)
            with open(f"/home/jack/weather_data_pipeline/airflow/data/{CURRENT_DATE}/{location}.txt", "w") as writer:
                writer.write(json.dumps(response.json()))
                writer.close()
    def getOrCreate_S3bucket(self,bucket_name = f'weather_bucket_{CURRENT_DATE}'):
        try:
            S3.create_bucket(Bucket=bucket_name)
            print(f"create bucket {bucket_name} success")
        except:
            response= S3.list_buckets()
            bucket_names=[bucket['Name'] for bucket in response['Buckets']]
            if bucket_name in bucket_names:
                print(f"bucket {bucket_name} already exists.")
    def load_to_S3bucket(self,bucket_name=f'weather_bucket_{CURRENT_DATE}',overwrite=False):
        if overwrite:
            S3.delete_bucket(Bucket=bucket_name)
        os.chdir(f"/home/jack/weather_data_pipeline/airflow/data/{CURRENT_DATE}")
        for file in os.listdir():
            object_key = f"{file}_{CURRENT_DATE}_{datetime.datetime.now().strftime('%H:%M:%S')}"
            S3.upload_file(file, bucket_name, object_key)

if __name__=="__main__":
    locations = ["London", "Tokyo", "Sydney", "Paris", "Berlin", "Moscow", "Madrid", "Rome", "Cairo"]
    weather=WeatherPipeline(locations)
    # weather.extract_weather_data()
            

