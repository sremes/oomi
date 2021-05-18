"""Main function for running as a Google Cloud Function."""

from base64 import b64decode
from datetime import datetime, timedelta
import os
from typing import Dict
import pandas as pd

from oomi.firestore import Firestore, FirestoreConfig
from oomi.influxdb import Influxdb, InfluxDBConfig
from oomi.oomi_downloader import OomiDownloader, OomiConfig


def get_data(timestamp: datetime) -> pd.DataFrame:
    """Get data from the given day indicated by `timestamp`."""
    first_date = timestamp - timedelta(days=2)  # data is available with 2 days delay
    last_date = timestamp - timedelta(days=1)
    print(f"Getting consumption data from: {first_date:%Y-%m-%d}")
    downloader = OomiDownloader(
        config=OomiConfig(), username=os.environ.get("OOMI_USER"), password=os.environ.get("OOMI_PASSWORD")
    )
    data_frame = downloader.get_consumption(f"{first_date:%Y-%m-%d}", f"{last_date:%Y-%m-%d}")

    return data_frame


def get_timestamp(context, timezone) -> datetime:
    """Get current timestamp from the context and convert to given timezone."""
    assert context.timestamp.endswith("Z"), "Timestamp has to be in UTC"
    timestamp = datetime.fromisoformat(context.timestamp.replace("Z", ""))
    timestamp = timezone.fromutc(timestamp)
    return timestamp


def update_firestore(event: Dict[str, str], context) -> None:
    """Update Firestore with a Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    """
    print(f"Firestore update was triggered by messageId {context.event_id} published at {context.timestamp}")

    if "data" in event:
        data = b64decode(event["data"]).decode("utf-8")
        print(f"Event data: {data}")

    firestore_config = FirestoreConfig()
    timestamp = get_timestamp(context, firestore_config.TIMEZONE)
    data_frame = get_data(timestamp)

    # upload
    client = Firestore(config=firestore_config)
    client.upload_data(data_frame)


def update_influxdb(event: Dict[str, str], context) -> None:
    """Update InfluxDB with a Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.

    Note: requires environment variables to be defined for influxdb (url, org, token)
    """
    print(f"InfluxDB update was triggered by messageId {context.event_id} published at {context.timestamp}")

    if "data" in event:
        data = b64decode(event["data"]).decode("utf-8")
        print(f"Event data: {data}")

    influxdb_config = InfluxDBConfig()
    timestamp = get_timestamp(context, influxdb_config.TIMEZONE)
    data_frame = get_data(timestamp)

    # upload
    client = Influxdb(config=influxdb_config)
    client.upload_data(data_frame)
