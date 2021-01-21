"""Implement firestore database interface."""
from google.cloud import firestore
import pandas as pd

from oomi.database import Database


class FirestoreConfig:  # pylint: disable=too-few-public-methods
    """Basic configuration of the database."""

    COLLECTION = "oomi"  # collection to save daily consumption docs


class Firestore(Database):
    """Implements database interface for Firestore."""

    def __init__(self, config: FirestoreConfig, project=None, credentials=None) -> None:
        """Create the firestore client instance."""
        # Project ID is determined by the GCLOUD_PROJECT environment variable
        self.client = firestore.Client(project=project, credentials=credentials)
        self.config = config

    def upload_data(self, data: pd.DataFrame) -> None:
        """Upload dataframe to database."""
        for date, consumption in zip(data["date"], data["consumption"]):
            self.client.collection(self.config.COLLECTION).document(date).set({"date": date, "consumption": consumption})

    def download_data(self) -> pd.DataFrame:
        """Download data from database."""
        return pd.DataFrame.from_records(
            [doc.to_dict() for doc in self.client.collection(self.config.COLLECTION).stream()]
        )


def main():
    """Run a simple test with firestore."""
    config = FirestoreConfig()
    config.COLLECTION = "test"
    client = Firestore(config=config)
    data = pd.DataFrame.from_records([{"date": "monday", "consumption": 69}, {"date": "tuesday", "consumption": 42}])
    client.upload_data(data)
    retrieved = client.download_data()
    print(retrieved)


if __name__ == "__main__":
    main()
