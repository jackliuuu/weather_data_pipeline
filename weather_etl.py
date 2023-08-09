import requests
from dotenv import load_dotenv
import boto3
import json
import pandas as pd
import os
import datetime
load_dotenv(".env")
CURRENT_DATE=datetime.datetime.now().strftime("%Y-%m-%d")
aws_key=os.getenv("AWS_ACCESS_KEY_ID")
aws_secret=os.getenv("AWS_SECRET_ACCESS_KEY")
weather_api_key=os.getenv("API_KEY")

class WeatherPipeline:
    def __init__(self,locations=None) -> None:
        self.locations=locations
        self.s3=boto3.client('s3',
                        aws_access_key_id=aws_key,
                        aws_secret_access_key=aws_secret,
                        region_name="ap-northeast-1")
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
    def getOrCreate_S3bucket(self,bucket_name = f'weather-bucket-jack'):
        try:
            self.s3.create_bucket(Bucket=bucket_name, 
                                CreateBucketConfiguration={'LocationConstraint': 'ap-northeast-1'  
            }
        )
            print(f"create bucket {bucket_name} success")
        except Exception as e:
            print("An error occurred:", e)
    def load_to_S3bucket(self,bucket_name=f'weather-bucket-jack',overwrite=False):
        if overwrite:
            self.s3.delete_bucket(Bucket=bucket_name,CreateBucketConfiguration={'LocationConstraint': 'ap-northeast-1'  
            })
        os.chdir(f"/home/jack/weather_data_pipeline/airflow/data/{CURRENT_DATE.replace('-','_')}")
        try:
            for file in os.listdir():
                object_key = f"{CURRENT_DATE}/{file}"
                self.s3.upload_file(file, bucket_name, object_key)
                print(f"upload {file} to bucket {bucket_name} / folder:{CURRENT_DATE} success")
        except Exception as e:
            print("An error occurred:", e)
if __name__=="__main__":
    locations = ["London", "Tokyo", "Sydney", "Paris", "Berlin", "Moscow", "Madrid", "Rome", "Cairo"]
    weather=WeatherPipeline(locations)
    # weather.extract_weather_data()
    weather.getOrCreate_S3bucket()        
    weather.load_to_S3bucket()
