import sys

from airbyte_cdk.entrypoint import launch

from destination_dropbox_files.destination import DestinationDropboxFiles


def main() -> None:
    launch(DestinationDropboxFiles(), sys.argv[1:])
