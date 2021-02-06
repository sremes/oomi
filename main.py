"""Main function for running as a Google Cloud Function."""

from base64 import b64decode
from datetime import datetime, timedelta
import os
from typing import Dict

from oomi.firestore import Firestore, FirestoreConfig
from oomi.oomi_downloader import OomiDownloader, OomiConfig


def update_firestore(event: Dict[str, str], context) -> None:
    """Update Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    """
    print(f"This Function was triggered by messageId {context.event_id} published at {context.timestamp}")

    if "data" in event:
        data = b64decode(event["data"]).decode("utf-8")
        print(f"Event data: {data}")

    # get dates from the current timestamp
    assert context.timestamp.endswith("Z"), "Timestamp has to be in UTC"
    firestore_config = FirestoreConfig()
    timestamp = datetime.fromisoformat(context.timestamp.replace("Z", ""))
    timestamp = firestore_config.TIMEZONE.fromutc(timestamp)

    first_date = timestamp - timedelta(days=2)  # data is available with 2 days delay
    last_date = timestamp - timedelta(days=1)

    # get data
    print(f"Getting consumption data from: {first_date:%Y-%m-%d}")
    downloader = OomiDownloader(
        config=OomiConfig(), username=os.environ.get("OOMI_USER"), password=os.environ.get("OOMI_PASSWORD")
    )
    data_frame = downloader.get_consumption(f"{first_date:%Y-%m-%d}", f"{last_date:%Y-%m-%d}")

    # upload
    client = Firestore(config=firestore_config)
    client.upload_data(data_frame)
