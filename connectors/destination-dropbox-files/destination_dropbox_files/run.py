import sys

from destination_dropbox_files.destination import DestinationDropboxFiles


def main() -> None:
    # Destinations own their protocol runner.  The generic CDK `launch` helper
    # is source-only and exposes `read` rather than the destination `write`
    # command.
    DestinationDropboxFiles().run(sys.argv[1:])
