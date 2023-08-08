import requests
from dotenv import load_dotenv
import boto3
import json
import pandas as pd
import os
import datetime
load_dotenv
CURRENT_DATE=datetime.datetime.now().strftime("%Y-%m-%d").replace("-","_")
weather_api_key=os.getenv("API_KEY")
S3=boto3.client('s3')
class WeatherPipeline:
    def __init__(self,locations=None) -> None:
        self.locations=locations
    def extract_weather_data(self):
        '''
        extract weather data from api
        '''
        if not os.path.exists(f"./airflow/data/{CURRENT_DATE}"):
            os.mkdir(f"./airflow/data/{CURRENT_DATE}")
        for location in self.locations:
            url = f"http://api.weatherapi.com/v1/current.json?key={weather_api_key}&q={location}&aqi=no"
            response=requests.get(url)
            with open(f"./airflow/data/{CURRENT_DATE}/{location}.txt", "w") as writer:
                writer.write(json.dumps(response.json()))
                writer.close()
    def getOrCreate_S3bucket(self,bucket_name = f'weather_bucket_{CURRENT_DATE}'):
        try:
            s3.create_bucket(Bucket=bucket_name)
            print(f"create bucket {bucket_name} success")
        except:
            response= s3.list_buckets()
            bucket_names=[bucket['Name'] for bucket in response['Buckets']]
            if bucket_name in bucket_names:
                print(f"bucket {bucket_name} already exists.")
    def load_to_S3bucket(self):
       
        pass
    
            

