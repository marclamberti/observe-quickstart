import logging

# get the airflow.task logger
t_log = logging.getLogger("airflow.task")


def get_data_from_aq_sensor(
    sensor_id: str,
    timestamp: str,
    simulate_api_delay: bool,
    include_random_sensor_failures: bool = True,
) -> dict:
    """
    Mocks fetching data from an air quality sensor.
    Args:
        sensor_id (str): The ID of the sensor to be accessed.
        timestamp (str): The current timestamp for the data fetch.
        simulate_api_delay (bool): Whether to simulate an API delay with retries.
    Returns:
        dict: A dictionary containing the PM2.5 and PM10 air quality values.
    """
    import random
    import time

    if simulate_api_delay:
        while True:
            t_log.error(f"Failed to get data from sensor: {sensor_id}")
            t_log.info("Retrying in 5 minutes")
            time.sleep(300)

    if include_random_sensor_failures and random.random() < 0.1:
        t_log.error(f"Failed to get data from sensor: {sensor_id}")
        return {"status": 500, "fetch_time": 0.001}

    t_log.info(f"Getting data from sensor: {sensor_id}")

    # mock PM2.5 and PM10 values
    pm2_5 = round(random.uniform(5.0, 100.0), 1)  # µg/m³ for fine particles
    pm10 = round(random.uniform(10.0, 150.0), 1)  # µg/m³ for coarse particles

    return {
        "sensor_id": sensor_id,
        "aq": {"pm2_5": pm2_5, "pm10": pm10},
        "timestamp": timestamp,
        "status": 200,
        "fetch_time": 0.05,
    }
