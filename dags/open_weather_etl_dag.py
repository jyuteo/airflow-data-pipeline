import os
import requests
import pandas as pd

from datetime import timedelta, datetime
from io import StringIO
from typing import Dict

from airflow.decorators import dag, task
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["example@domain.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def get_open_weather_api_key():
    return os.environ["OPEN_WEATHER_API_KEY"]


def kelvin_to_celcius(temp: float) -> float:
    return temp - 273.15


@dag(
    default_args=default_args,
    dag_id="open_weather_etl",
    schedule_interval="@hourly",
    start_date=datetime(2023, 8, 1),
    catchup=False,
)
def open_weather_etl():
    api_key = get_open_weather_api_key()
    api_enpoint = "/data/2.5/weather?q=London&APPID={}".format(api_key)

    s3_hook = S3Hook(aws_conn_id="s3_airflow_user")
    s3_bucket = "open-weather-airflow-data-pipeline"
    s3_file_name = "open_weather_data.csv"

    @task.sensor(
        poke_interval=5,
        timeout=15,
        mode="reschedule",
    )
    def check_api_ready():
        return HttpSensor(
            task_id="check_api_ready",
            http_conn_id="open_weather_api",
            endpoint=api_enpoint,
        )

    @task()
    def extract() -> Dict:
        req_url = "https://api.openweathermap.org" + api_enpoint
        resp = requests.get(req_url)
        return {"weather_data": resp.json()}

    @task()
    def transform(data: Dict) -> Dict:
        weather_data = data["weather_data"]
        city = weather_data["name"]
        weather_description = weather_data["weather"][0]["description"]
        temp_celcius = kelvin_to_celcius(weather_data["main"]["temp"])
        feels_like_celcius = kelvin_to_celcius(weather_data["main"]["feels_like"])
        min_temp_celcius = kelvin_to_celcius(weather_data["main"]["temp_min"])
        max_temp_celcius = kelvin_to_celcius(weather_data["main"]["temp_max"])
        pressure = weather_data["main"]["pressure"]
        humidity = weather_data["main"]["humidity"]
        wind_speed = weather_data["wind"]["speed"]
        time_of_record = datetime.utcfromtimestamp(weather_data["dt"] + weather_data["timezone"])
        sunrise_time = datetime.utcfromtimestamp(weather_data["sys"]["sunrise"] + weather_data["timezone"])
        sunset_time = datetime.utcfromtimestamp(weather_data["sys"]["sunset"] + weather_data["timezone"])

        transformed_data = {
            "time": time_of_record,
            "city": city,
            "description": weather_description,
            "temperature_celcius": temp_celcius,
            "feels_like_celcius": feels_like_celcius,
            "minimum_temp_celcius": min_temp_celcius,
            "maximum_temp_celcius": max_temp_celcius,
            "pressure": pressure,
            "humidty": humidity,
            "wind_speed": wind_speed,
            "sunrise_local_time": sunrise_time,
            "sunset_local_time)": sunset_time,
        }

        return {"transformed_data": transformed_data}

    @task()
    def load(data):
        transformed_data = data["transformed_data"]
        print(transformed_data)

        historical_data = pd.DataFrame()
        if s3_hook.check_for_key(s3_file_name, s3_bucket):
            data = StringIO(s3_hook.read_key(s3_file_name, s3_bucket))
            historical_data = pd.read_csv(data, sep=",")

        current_data = pd.DataFrame([transformed_data])
        if not historical_data.empty:
            current_data = pd.concat([historical_data, current_data])

        s3_hook.load_string(
            current_data.to_csv(index=False),
            s3_file_name,
            bucket_name=s3_bucket,
            replace=True,
        )

    data = extract()
    check_api_ready() >> data
    transformed_data = transform(data)
    load(transformed_data)


etl = open_weather_etl()
