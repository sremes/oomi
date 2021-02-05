"""Parser for the Oomi customer electricity data."""
import argparse
from io import BytesIO
import re

import requests
import pandas as pd


class OomiConfig:  # pylint: disable=too-few-public-methods
    """URL's needed for downloading data."""

    OOMI_NO_AUTH_PAGE = "https://online.oomi.fi/eServices/Online/IndexNoAuth"
    OOMI_LOGIN_URL = "https://online.oomi.fi/eServices/Online/Login"
    OOMI_GENERATE_EXCEL_URL = "https://online.oomi.fi/Reporting/CustomerConsumption/GenerateExcelFile"
    OOMI_DOWNLOAD_EXCEL_URL = "https://online.oomi.fi/Reporting/CustomerConsumption/DownloadExcelFile?identifier="
    OOMI_VERIFICATION_TOKEN_REGEX = (
        r'(?<=<input name="__RequestVerificationToken" type="hidden" value=")([\w-]+)(?=" />)'
    )


class OomiDownloader:
    """Utility for downloading comsumption data."""

    def __init__(self, config: OomiConfig, username: str, password: str) -> None:
        """Init downloader with given config and login data."""
        self.config = config
        self.username = username
        self.password = password

    def get_consumption(self, start: str, end: str) -> pd.DataFrame:
        """Get the consumption between start and end as pandas dataframe."""
        with requests.Session() as session:
            self.login_to_oomi(session)
            response = session.post(
                self.config.OOMI_GENERATE_EXCEL_URL, data={"start": start, "end": end, "selectedTimeSpan": "hour"},
            )
            data = session.get(self.config.OOMI_DOWNLOAD_EXCEL_URL + response.json()["identifier"])
        df = pd.read_excel(  # pylint: disable=invalid-name
            BytesIO(data.content),
            header=1,
            parse_dates=[0],
            date_parser=lambda x: pd.to_datetime(x, format="%d.%m.%Y %H.%M"),
        )
        # parse column name that contains the address
        location = ", ".join(s.strip() for s in df.columns[1].split("\n"))
        df.columns = ["time", "consumption"]  # rename columns
        df["location"] = location  # add new column
        return df

    def login_to_oomi(self, session: requests.Session) -> None:
        """Log-in to Oomi inside the given session."""
        response = session.get(self.config.OOMI_NO_AUTH_PAGE)
        match = re.search(self.config.OOMI_VERIFICATION_TOKEN_REGEX, response.text)
        if match is not None:
            verification_token = match.group()
        else:
            raise ValueError("Request verification token not found!")
        _ = session.post(
            self.config.OOMI_LOGIN_URL,
            data={
                "UserName": self.username,
                "Password": self.password,
                "__RequestVerificationToken": verification_token,
            },
        )


def main():
    """Simple main to test the downloader."""
    parser = argparse.ArgumentParser(description="Download and print consumption data.")
    parser.add_argument("--username", type=str, help="Oomi user name")
    parser.add_argument("--password", type=str, help="Oomi user password")
    parser.add_argument("--start", type=str, default="2021-01-01", help="Start date")
    parser.add_argument("--end", type=str, default="2021-01-02", help="End date")
    args = parser.parse_args()
    downloader = OomiDownloader(OomiConfig(), username=args.username, password=args.password)
    data_frame = downloader.get_consumption(args.start, args.end)
    print(data_frame)


if __name__ == "__main__":
    main()
