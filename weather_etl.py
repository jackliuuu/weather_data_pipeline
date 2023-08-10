import requests
from dotenv import load_dotenv
import boto3
import json
import pandas as pd
import os
import datetime
from botocore.exceptions import ClientError
import awswrangler as wr
load_dotenv(".env")
CURRENT_DATE=datetime.datetime.now().strftime("%Y-%m-%d")
aws_key=os.getenv("AWS_ACCESS_KEY_ID")
aws_secret=os.getenv("AWS_SECRET_ACCESS_KEY")
weather_api_key=os.getenv("API_KEY")
region_name="ap-northeast-1"
boto3.setup_default_session(region_name=region_name)
class WeatherPipeline:
    def __init__(self,locations=None) -> None:
        self.locations=locations
        self.s3=boto3.client('s3',
                        aws_access_key_id=aws_key,
                        aws_secret_access_key=aws_secret
                        )
        self.glue_client = boto3.client('glue')
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
        os.chdir(f"/home/jack/weather_data_pipeline/airflow/data/{CURRENT_DATE}")
        try:
            for file in os.listdir():
                object_key = f"{CURRENT_DATE}/{file}"
                self.s3.upload_file(file, bucket_name, object_key)
                print(f"upload {file} to bucket {bucket_name} / folder:{CURRENT_DATE} success")
        except Exception as e:
            print("An error occurred:", e)
    def processData(self):
        os.chdir(f"/home/jack/weather_data_pipeline/airflow/data/{CURRENT_DATE}")
        files = os.listdir()
        df = pd.DataFrame()
        current_index = 0
        for file in files:
            with open(file, 'r') as read_file:
                data = json.loads(read_file.read())

                # Extract data
                location_data = data.get("location")  
                current_data = data.get("current")

                # Create DataFrames
                location_df = pd.DataFrame(location_data, index=[current_index])
                current_df = pd.DataFrame(current_data, index=[current_index])
                current_index += 1
                current_df['condition'] = current_data.get('condition').get('text')

                # Concatenate DataFrames and append to main DataFrame
                temp_df = pd.concat([location_df, current_df],axis=1)
                df = pd.concat([df, temp_df])

                read_file.close()

        # Return main DataFrame
        df = df.rename(columns={'name':'city'})
        df['localtime'] = pd.to_datetime(df['localtime'])
        return df
    def createAthenaTable(self,df,date):
        try:
            self.glue_client.get_database(Name='weather')
            print(f"Database 'weather' already exists. Skipping creation.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                self.glue_client.create_database(DatabaseInput={'Name': 'weather'})
                print(f"Database 'weather' created.")
        try:
            response=wr.s3.to_parquet(
            df=df,
            path=f"s3://weather-bucket-jack/{date}/weather_{date}.parquet",
            dataset=True,
            database="weather",
            table=f"weather.{CURRENT_DATE}"
            )
            print(response)
        except Exception as e:
            print("An error occurred:",e)
        
if __name__=="__main__":
    locations = ["London", "Tokyo", "Sydney", "Paris", "Berlin", "Moscow", "Madrid", "Rome", "Cairo"]
    weather=WeatherPipeline(locations)
    # weather.extract_weather_data()
    # weather.getOrCreate_S3bucket()        
    # weather.load_to_S3bucket()
    df=weather.processData()
    print(df)
    weather.createAthenaTable(df,CURRENT_DATE)
