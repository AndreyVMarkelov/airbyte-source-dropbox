import sys

from destination_dropbox_files.destination import DestinationDropboxFiles
from destination_dropbox_files.run import main


def test_main_delegates_to_destination_protocol_runner(monkeypatch) -> None:
    captured: list[list[str]] = []

    monkeypatch.setattr(sys, "argv", ["destination-dropbox-files", "write", "--config", "config.json", "--catalog", "catalog.json"])
    monkeypatch.setattr(DestinationDropboxFiles, "run", lambda _self, args: captured.append(args))

    main()

    assert captured == [["write", "--config", "config.json", "--catalog", "catalog.json"]]


def test_destination_protocol_parser_accepts_write() -> None:
    parsed = DestinationDropboxFiles().parse_args(["write", "--config", "config.json", "--catalog", "catalog.json"])

    assert parsed.command == "write"
