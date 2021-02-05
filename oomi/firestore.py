"""Implement firestore database interface."""
from datetime import datetime
import pytz

from google.cloud import firestore
import pandas as pd

from oomi.database import Database


class FirestoreConfig:  # pylint: disable=too-few-public-methods
    """Basic configuration of the database."""

    COLLECTION = "oomi"  # collection to save hourly consumption docs
    TIMEZONE = pytz.timezone("Europe/Helsinki")


class Firestore(Database):
    """Implements database interface for Firestore."""

    def __init__(self, config: FirestoreConfig, project=None, credentials=None) -> None:
        """Create the firestore client instance."""
        self.client = firestore.Client(project=project, credentials=credentials)
        self.config = config

    def upload_data(self, data: pd.DataFrame) -> None:
        """Upload dataframe to database."""
        for time, consumption, location in zip(data["time"], data["consumption"], data["location"]):
            time = self.config.TIMEZONE.localize(time)
            self.client.collection(self.config.COLLECTION).document(f"{time:%Y-%m-%d-%H%M}").set(
                {"time": time, "consumption": consumption, "location": location}
            )

    def download_data(self) -> pd.DataFrame:
        """Download data from database."""
        docs = [doc.to_dict() for doc in self.client.collection(self.config.COLLECTION).stream()]
        dataframe = pd.DataFrame.from_records(docs)
        dataframe["time"] = dataframe["time"].dt.tz_convert(self.config.TIMEZONE)
        return dataframe


# pylint: disable=invalid-name
def main():
    """Run a simple test with firestore."""
    config = FirestoreConfig()
    config.COLLECTION = "test"
    client = Firestore(config=config)
    tz = config.TIMEZONE
    data = pd.DataFrame.from_records(
        [
            {"time": tz.localize(datetime(2021, 1, 1)), "consumption": 69, "location": "TEST"},
            {"time": tz.localize(datetime(2021, 1, 2)), "consumption": 42, "location": "TEST"},
        ]
    )
    client.upload_data(data)
    retrieved = client.download_data()
    print(retrieved)


if __name__ == "__main__":
    main()
