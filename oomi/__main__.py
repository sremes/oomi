"""Main entrypoint for the module is defined here."""

import argparse
from oomi.firestore import Firestore, FirestoreConfig
from oomi.oomi_downloader import OomiDownloader, OomiConfig


def parse_arguments():
    """Parse arguments with argument parser."""
    parser = argparse.ArgumentParser(description="Argument parser example.")
    parser.add_argument("--username", type=str, help="Oomi user name")
    parser.add_argument("--password", type=str, help="Oomi user password")
    parser.add_argument("--start", type=str, default="2021-01-01", help="Start date")
    parser.add_argument("--end", type=str, default="2021-01-02", help="End date")
    return parser.parse_args()


def main():
    """Get data and upload to database."""
    args = parse_arguments()

    # get data
    downloader = OomiDownloader(OomiConfig(), username=args.username, password=args.password)
    data_frame = downloader.get_consumption(args.start, args.end)

    # upload
    client = Firestore(config=FirestoreConfig())
    client.upload_data(data_frame)


if __name__ == "__main__":
    main()
