"""Interface for a database connection."""
import pandas as pd


class Database:
    """Defines the interface used for database connection."""

    def upload_data(self, data: pd.DataFrame) -> None:
        """Upload dataframe to database."""
        raise NotImplementedError

    def download_data(self) -> pd.DataFrame:
        """Download data from database."""
        raise NotImplementedError
