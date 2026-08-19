import sys

from airbyte_cdk.entrypoint import launch

from destination_dropbox.destination import DestinationDropbox


def main() -> None:
    launch(DestinationDropbox(), sys.argv[1:])


if __name__ == "__main__":
    main()
