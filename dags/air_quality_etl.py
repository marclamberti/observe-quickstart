"""DAG that an ETL pipeline for air quality data once per minute."""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.models.param import Param

from include.utils import get_data_from_aq_sensor


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="* * * * *",
    max_active_runs=1,
    catchup=False,
    doc_md=__doc__,
    params={
        "sensor_id": Param(
            1, type="integer", description="The id of the sensor to be accessed"
        ),
        "simulate_api_delay": Param(
            False,
            type="boolean",
            description="Whether to fail the API call to the weather sensor",
        ),
    },
)
def aq_etl():

    @task
    def get_aq_data(**context):
        """
        Extracts the raw current air quality data from an air quality sensor.
        Args:
            context (dict): The context object passed by Airflow.
        Returns:
            dict: The raw air quality data.
        """
        sensor_id = context["params"]["sensor_id"]
        simulate_api_delay = context["params"]["simulate_api_delay"]

        # ts is the timestamp that marks the start of this DAG run's data interval
        # (when the previous DAG run happened)
        # the DAG runs once every minute, so we need to add another minute
        # to get the timestamp for the current minute
        timestamp = context["ts"]
        timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%z") + timedelta(
            minutes=1
        )
        timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S%z")

        aq_data = get_data_from_aq_sensor(
            sensor_id=sensor_id,
            timestamp=timestamp,
            simulate_api_delay=simulate_api_delay,
        )

        return aq_data

    @task
    def transform_aq_data(aq_data: dict):
        """
        Transforms the raw air quality data into a format that can be loaded into a database.
        Args:
            aq_data (dict): The raw air quality data.
        Returns:
            dict: The transformed air quality data.
        """
        pm2_5 = aq_data["aq"]["pm2_5"]
        pm10 = aq_data["aq"]["pm10"]
        timestamp = aq_data["timestamp"]
        sensor_id = aq_data["sensor_id"]

        return {
            "sensor_id": sensor_id,
            "pm2_5": pm2_5,
            "pm10": pm10,
            "timestamp": timestamp,
        }

    @task(outlets=[Dataset("aq_data")])
    def load_aq_data(aq_data: dict):
        """
        Loads the transformed air quality data into a local CSV file.
        In a production environment, this function would load the data into a database.
        Args:
            aq_data (dict): The transformed air quality data.
        """
        import os

        if not os.path.exists("include/aq_data.csv"):
            with open("include/aq_data.csv", "a") as f:
                f.write("sensor_id,pm2_5,pm10,timestamp\n")

        with open("include/aq_data.csv", "a") as f:
            f.write(
                f"{aq_data['sensor_id']},{aq_data['pm2_5']},{aq_data['pm10']},{aq_data['timestamp']}\n"
            )

    load_aq_data(transform_aq_data(get_aq_data()))


aq_etl()
