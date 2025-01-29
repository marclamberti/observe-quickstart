from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models.param import Param
import logging
from airflow.models.dataset import Dataset

# get the airflow.task logger
t_log = logging.getLogger("airflow.task")


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",  # runs every hour
    max_active_runs=1,
    catchup=False,
    params={
        "delay_air_quality_fetch": Param(
            False,
            type="boolean",
            description="Whether to fail the API call to the weather sensor",
        ),
    },
)
def create_air_quality_report():

    @task(inlets=[Dataset("air_quality_data")])
    def get_air_quality_data_last_day(**context):
        """
        Fetches the latest air quality data from the csv file.
        Returns:
            dict: The latest air quality data.
        """
        import csv
        import time

        delay_air_quality_fetch = context["params"]["delay_air_quality_fetch"]
        if delay_air_quality_fetch:
            time.sleep(60 * 61)

        # ts is the timestamp that marks the start of this DAG run's data interval
        # (when the previous DAG run happened),
        # the DAG runs once every hour, so we need to subtract another 23
        # to get the timstamp for 24 hours before the actual DAG run
        timestamp = context["ts"]
        cutoff_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%z") - timedelta(
            hours=23
        )

        with open("include/air_quality_data.csv", "r") as file:
            reader = csv.DictReader(file)
            data = list(reader)

        # filter data for the last week
        last_day_aq_data = [
            row
            for row in data
            if datetime.strptime(row["timestamp"], "%Y-%m-%dT%H:%M:%S%z") >= cutoff_time
        ]

        return last_day_aq_data

    @task
    def caluclate_avg_air_quality_per_hour(last_day_aq_data: dict, **context) -> dict:
        """
        Calculates the average air quality values per hour in the last day.
        """

        timestamp = context["ts"]
        current_hour = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%z").hour
        last_24_hours = [(current_hour - i) % 24 for i in range(24)]

        avg_air_quality_per_hour = {}

        for hour in last_24_hours:
            data_for_hour = [
                row
                for row in last_day_aq_data
                if datetime.strptime(row["timestamp"], "%Y-%m-%dT%H:%M:%S%z").hour
                == hour
            ]

            pm2_5_values = [float(row["pm2_5"]) for row in data_for_hour]
            pm10_values = [float(row["pm10"]) for row in data_for_hour]

            if len(pm2_5_values) != 0:
                avg_pm2_5 = sum(pm2_5_values) / len(pm2_5_values)
            else:
                avg_pm2_5 = "NO DATA"

            if len(pm10_values) != 0:
                avg_pm10 = sum(pm10_values) / len(pm10_values)
            else:
                avg_pm10 = "NO DATA"

            avg_air_quality_per_hour[str(hour)] = {
                "avg_pm2_5": avg_pm2_5,
                "avg_pm10": avg_pm10,
            }

        return avg_air_quality_per_hour

    @task(
        outlets=[Dataset("air_quality_report")],
    )
    def send_air_quality_report(avg_air_quality_per_hour: dict, **context):
        """
        Mocks sending the air quality report to the team.
        """
        day = context["ts"].split("T")[0]

        for entry in avg_air_quality_per_hour:
            t_log.info(
                f"Day {day} Hour {entry}: PM2.5: {avg_air_quality_per_hour[entry]['avg_pm2_5']}, PM10: {avg_air_quality_per_hour[entry]['avg_pm10']}"
            )

    _get_air_quality_data_last_week = get_air_quality_data_last_day()
    _caluclate_avg_air_quality_per_hour = caluclate_avg_air_quality_per_hour(
        _get_air_quality_data_last_week
    )
    send_air_quality_report(_caluclate_avg_air_quality_per_hour)


create_air_quality_report()
