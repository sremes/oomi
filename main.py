"""Main function for running as a Google Cloud Function."""

import os

from oomi.firestore import Firestore, FirestoreConfig
from oomi.oomi_downloader import OomiDownloader, OomiConfig


def update_firestore(event: dict, context: dict) -> None:
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

    print(f"Event: {event}")

    if False is None:
        # get data
        downloader = OomiDownloader(
            config=OomiConfig(), username=os.environ.get("OOMI_USER"), password=os.environ.get("OOMI_PASSWORD")
        )
        data_frame = downloader.get_consumption("2021-01-01", "2021-01-02")

        # upload
        client = Firestore(config=FirestoreConfig())
        client.upload_data(data_frame)
