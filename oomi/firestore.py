"""Implement firestore database interface."""
from datetime import datetime
import pytz

from google.cloud import firestore
import pandas as pd

from oomi.database import Database


class FirestoreConfig:  # pylint: disable=too-few-public-methods
    """Basic configuration of the database."""

    COLLECTION = "oomi"  # collection to save daily consumption docs
    TIMEZONE = pytz.timezone("Europe/Helsinki")


class Firestore(Database):
    """Implements database interface for Firestore."""

    def __init__(self, config: FirestoreConfig, project=None, credentials=None) -> None:
        """Create the firestore client instance."""
        self.client = firestore.Client(project=project, credentials=credentials)
        self.config = config

    def upload_data(self, data: pd.DataFrame) -> None:
        """Upload dataframe to database."""
        for date, consumption in zip(data["date"], data["consumption"]):
            self.client.collection(self.config.COLLECTION).document(f"{date:%Y-%m-%d}").set(
                {"date": date, "consumption": consumption}
            )

    def download_data(self) -> pd.DataFrame:
        """Download data from database."""
        docs = [doc.to_dict() for doc in self.client.collection(self.config.COLLECTION).stream()]
        dataframe = pd.DataFrame.from_records(docs)
        dataframe["date"] = dataframe["date"].dt.tz_convert(self.config.TIMEZONE)
        return dataframe


def main():
    """Run a simple test with firestore."""
    config = FirestoreConfig()
    config.COLLECTION = "test"  # pylint: disable=invalid-name
    client = Firestore(config=config)
    tz = config.TIMEZONE
    data = pd.DataFrame.from_records(
        [
            {"date": tz.localize(datetime(2021, 1, 1)), "consumption": 69},
            {"date": tz.localize(datetime(2021, 1, 2)), "consumption": 42},
        ]
    )
    client.upload_data(data)
    retrieved = client.download_data()
    print(retrieved)


if __name__ == "__main__":
    main()
