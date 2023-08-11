from flask import Flask
import requests
from bs4 import BeautifulSoup
import json
import os
import sys
from dotenv import load_dotenv
import pandas as pd
sys.path.append('/home/jack/weather_data_pipeline/airflow')
from dags.weather_etl import WeatherPipeline

load_dotenv('/home/jack/weather_data_pipeline/airflow/dags/.env')
app = Flask(__name__)

locations = ["London", "Tokyo", "Sydney", "Paris", "Berlin", "Moscow", "Madrid", "Rome", "Cairo"]
weather = WeatherPipeline(locations)
@app.route("/city=<city>")
def get_data(city):
    df=weather.processData()
    model=weather.train_model(df)
    predictions_df = weather.predict_next_day_weather(model)
    predictions_df['at_date(UTC+8)'] = predictions_df['at_date(UTC+8)'].astype(str)

    # Return all predictions or predictions for a specific city
    if city == 'All':
        return predictions_df.to_json(orient='records')
    else:
        return predictions_df[predictions_df['city'] == city].to_json(orient='records')

if __name__=="__main__":
    app.run(debug=True)