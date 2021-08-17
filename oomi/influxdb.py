"""Implement influxdb database interface."""

from datetime import datetime
import os
import pytz
import pandas as pd

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from oomi.database import Database


class InfluxDBConfig:  # pylint: disable=too-few-public-methods
    """Basic configuration of the database."""

    BUCKET = "oomi"  # collection to save hourly consumption data
    TIMEZONE = pytz.timezone("Europe/Helsinki")


class Influxdb(Database):
    """Implements database interface for influxdb."""

    def __init__(self, config: InfluxDBConfig) -> None:
        """Creates influxdb client with the config defined in environment.

        The available configurations are:
            INFLUXDB_V2_URL - the url to connect to InfluxDB
            INFLUXDB_V2_ORG - default destination organization for writes and queries
            INFLUXDB_V2_TOKEN - the token to use for the authorization
            INFLUXDB_V2_TIMEOUT - socket timeout in ms (default value is 10000)
            INFLUXDB_V2_VERIFY_SSL - set this to false to skip verifying SSL certificate
            INFLUXDB_V2_SSL_CA_CERT - set this to customize the certificate file to verify the peer
            INFLUXDB_V2_CONNECTION_POOL_MAXSIZE - set this to customize the certificate file to verify the peer
        """
        super().__init__()
        self.client = InfluxDBClient.from_env_properties()
        self.config = config

    def download_data(self) -> pd.DataFrame:
        """Query written data."""
        query = f'from(bucket: "{self.config.BUCKET}") |> range(start: -7d) |> filter(fn: (r) => r._measurement == "hourly_consumption")'  # pylint: disable=line-too-long
        print(f'Querying from InfluxDB: "{query}" ...')

        query_api = self.client.query_api()
        data_frame = query_api.query_data_frame(query=query)
        data_frame["time"] = data_frame["_time"].dt.tz_convert(self.config.TIMEZONE)
        data_frame["consumption"] = data_frame["_value"]
        return data_frame[["time", "location", "consumption"]]

    def upload_data(self, data: pd.DataFrame) -> None:
        with self.client.write_api(write_options=SYNCHRONOUS) as write_api:
            for time, consumption, location in zip(data["time"], data["consumption"], data["location"]):
                time = self.config.TIMEZONE.localize(time)
                record = (
                    Point("hourly_consumption")
                    .tag("location", location)
                    .field("consumption", float(consumption))
                    .time(time=time)
                )
                write_api.write(bucket=self.config.BUCKET, record=record)


# pylint: disable=invalid-name
def main():
    """Run a simple test with influxdb."""
    config = InfluxDBConfig()
    config.BUCKET = "oomi-test"
    assert (
        os.environ.get("INFLUXDB_V2_URL") and os.environ.get("INFLUXDB_V2_ORG") and os.environ.get("INFLUXDB_V2_TOKEN")
    )
    client = Influxdb(config=config)
    data = pd.DataFrame.from_records(
        [
            {"time": datetime(2021, 5, 17), "consumption": 69, "location": "TEST"},
            {"time": datetime(2021, 5, 18), "consumption": 42, "location": "TEST"},
        ]
    )
    client.upload_data(data)
    retrieved = client.download_data()
    print(retrieved)


if __name__ == "__main__":
    main()
