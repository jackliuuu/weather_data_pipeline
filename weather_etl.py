import requests
from dotenv import load_dotenv
import boto3
import json
import pandas as pd
import os
import datetime
from botocore.exceptions import ClientError
import awswrangler as wr
from xgboost import XGBRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import train_test_split
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
                                aws_secret_access_key=aws_secret)
        self.glue_client = boto3.client('glue',
                                        aws_access_key_id=aws_key,
                                        aws_secret_access_key=aws_secret)
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
            table=f"weather_{CURRENT_DATE}"
            )
            print(response)
        except Exception as e:
            print("An error occurred:",e)
    def train_model(self,df):
        df=df.drop(columns=['region','country','tz_id', 'localtime','last_updated_epoch', 'last_updated', 'wind_dir', 'condition'])
        city_map = {
                'London':0,
                'Moscow':1,
                'Berlin':2,
                'Paris':3,
                'Rome':4,
                'Madrid':5,
                'Cairo':6,
                'Tokyo':7,
                'Sydney':8}
        df['city']=df['city'].map(city_map)
        x = df.drop(columns = ['temp_c'])
        y = df['temp_c']
        x_train,x_test,y_train,y_test=train_test_split(x,y,train_size=0.9,random_state=365)
        model=XGBRegressor()
        model.fit(x_train,y_train)
        predictions = model.predict(x_test)
        model_score = model.score(x_test, y_test)
        cities=[]
        for city_number in x_test.city.tolist():
            for city,num in city_map.items():
                if city_number==num:
                    cities.append(city)
        predictions_df=pd.DataFrame([*zip(cities,y_test,predictions,abs(y_test-predictions), 
                                    [model_score]*len(cities))], 
                                    columns=['city', 'actual_temp(Celcius)', 'predicted_temp(Celcius)', 'diff(Celcius)','score'])
        print(f"Test Data Predictions:\n {predictions_df}")
        return model
    def predict_next_day_weather(self, model):
        cities = ['London', 'Moscow' ,'Berlin' ,'Paris' ,'Rome' ,'Madrid', 'Cairo' ,'Tokyo', 'Sydney']
        next_day = datetime.datetime.now() + datetime.timedelta(days=1)
        next_day = next_day.strftime("%Y-%m-%d")
        table=f'weather_{CURRENT_DATE}'.replace('-','_')
        sql=f'WITH RankedWeather AS (SELECT*,ROW_NUMBER() OVER (PARTITION BY city ORDER BY localtime DESC) AS rn FROM {table}) SELECT * FROM  RankedWeather WHERE rn=1;'
        df=wr.athena.read_sql_query(sql=sql,database='weather')
        df = df.drop(columns = ['temp_c','rn', 'region', 'country', 'tz_id', 'localtime','last_updated_epoch', 'last_updated', 'wind_dir', 'condition'])
        city_map = {
                'London':0,
                'Moscow':1,
                'Berlin':2,
                'Paris':3,
                'Rome':4,
                'Madrid':5,
                'Cairo':6,
                'Tokyo':7,
                'Sydney':8}
        df['city'] = df['city'].map(city_map)
        df['localtime_epoch'] = df['localtime_epoch'] + 86400

        # Predict next day weather
        predictions = model.predict(df)

        # Generate and print a predictions dataframe
        predictions_df = pd.DataFrame([*zip(cities, predictions)], columns=['city', 'predicted_temp(Celcius)'])
        predictions_df['at_date(UTC+8)'] = df['localtime_epoch']

        # translate epoch to datetime
        predictions_df['at_date(UTC+8)'] = pd.to_datetime(predictions_df['at_date(UTC+8)'], unit='s')
        print(f"Next Day Predictions:\n {predictions_df}")

        return predictions_df
if __name__=="__main__":
    locations = ["London", "Tokyo", "Sydney", "Paris", "Berlin", "Moscow", "Madrid", "Rome", "Cairo"]
    weather=WeatherPipeline(locations)
    # weather.extract_weather_data()
    # weather.getOrCreate_S3bucket()        
    # weather.load_to_S3bucket()
    df=weather.processData()
    print(df)
    # weather.createAthenaTable(df,CURRENT_DATE)
    model=weather.train_model(df)
    weather.predict_next_day_weather(model=model)