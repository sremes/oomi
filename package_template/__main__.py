"""Main entrypoint for the module is defined here."""

import argparse


def parse_arguments():
    """Parse arguments with argument parser."""
    parser = argparse.ArgumentParser(description="Argument parser example.")
    return parser.parse_args()


def main():
    """Main function for the package entrypoint."""
    _ = parse_arguments()


if __name__ == "__main__":
    main()
