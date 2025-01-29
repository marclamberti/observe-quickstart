"""DAG that runs every 10minutes to evaluate the air quality data for the last 
20 minutes and send an alert if the PM2.5 or PM10 values are above the threshold."""

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.models.dataset import Dataset
from airflow.models.param import Param

# get the airflow.task logger
t_log = logging.getLogger("airflow.task")


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="0/10 * * * *",  # runs every 10 minutes
    max_active_runs=1,
    catchup=False,
    doc_md=__doc__,
    params={
        "delay_aq_fetch": Param(
            False,
            type="boolean",
            description="Whether to fail the API call to the weather sensor",
        ),
    },
)
def send_aq_alerts():

    @task(
        inlets=[Dataset("aq_data")],
    )
    def get_aq_data_last_20min(**context):
        """
        Fetches the latest air quality data from the csv file.
        Returns:
            dict: The latest air quality data.
        """
        import csv
        import time

        delay_aq_fetch = context["params"]["delay_aq_fetch"]
        if delay_aq_fetch:
            time.sleep(60 * 61)

        # ts is the timestamp that marks the start of this DAG run's data interval
        # (when the previous DAG run happened),
        # the DAG runs once every 10 minutes, so we need to subtract another 10 minutes
        # to get the timstamp for 20 minutes before the actual DAG run
        timestamp = context["ts"]
        cutoff_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%z") - timedelta(
            minutes=10
        )

        with open("include/aq_data.csv", "r") as file:
            reader = csv.DictReader(file)
            data = list(reader)

        # filter data for the last 20 minutes
        last_20min_aq_data = [
            row
            for row in data
            if datetime.strptime(row["timestamp"], "%Y-%m-%dT%H:%M:%S%z") >= cutoff_time
        ]

        return last_20min_aq_data

    @task
    def calculate_aq_averages_last_20min(last_20min_data: dict):
        """
        Calculates the average PM2.5 and PM10 values for the last 20 minutes.
        Args:
            last_20min_data (dict): The air quality data for the last 20 minutes.
        Returns:
            dict: The average PM2.5 and PM10 values in the last 20 minutes.
        """
        pm2_5_values = [float(row["pm2_5"]) for row in last_20min_data]
        pm10_values = [float(row["pm10"]) for row in last_20min_data]

        avg_pm2_5 = sum(pm2_5_values) / len(pm2_5_values)
        avg_pm10 = sum(pm10_values) / len(pm10_values)

        return {
            "avg_pm2_5": avg_pm2_5,
            "avg_pm10": avg_pm10,
        }

    @task.branch
    def decide_if_alert(last_20min_avg_aq_data: dict):
        avg_pm2_5 = last_20min_avg_aq_data["avg_pm2_5"]
        avg_pm10 = last_20min_avg_aq_data["avg_pm10"]

        if avg_pm2_5 > 50 or avg_pm10 > 100:
            return "send_alert"
        else:
            return "no_alert"

    @task(
        outlets=[Dataset("aq_alert")],
    )
    def send_alert(avg_aq_data: dict):
        """
        Mocks sending an alert if the average PM2.5 or PM10 values are above the threshold.
        Args:
            avg_aq_data (dict): The average PM2.5 and PM10 values in the last 20 minutes.
        """
        avg_pm2_5 = avg_aq_data["avg_pm2_5"]
        avg_pm10 = avg_aq_data["avg_pm10"]

        t_log.warning(
            f"Air quality alert! In the last 20min: {avg_pm2_5} µg/m³ PM2.5 and {avg_pm10} µg/m³ PM10."
        )

    @task
    def no_alert(avg_aq_data):
        """
        Logs that no alert is needed.
        Args:
            avg_aq_data (dict): The average PM2.5 and PM10 values in the last 20min.
        """

        avg_pm2_5 = avg_aq_data["avg_pm2_5"]
        avg_pm10 = avg_aq_data["avg_pm10"]

        t_log.info(
            f"No air quality alert needed. In the last 20min: {avg_pm2_5} µg/m³ PM2.5 and {avg_pm10} µg/m³ PM10."
        )

    _get_aq_data_last_20min = get_aq_data_last_20min()
    _calculate_aq_averages_last_20min = calculate_aq_averages_last_20min(
        _get_aq_data_last_20min
    )
    _decide_if_alert = decide_if_alert(_calculate_aq_averages_last_20min)
    _send_alert = send_alert(_calculate_aq_averages_last_20min)
    _no_alert = no_alert(_calculate_aq_averages_last_20min)

    chain(_decide_if_alert, [_send_alert, _no_alert])


send_aq_alerts()
