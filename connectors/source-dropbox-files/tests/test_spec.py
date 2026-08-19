import pytest

from source_dropbox_files.spec import MAX_FILE_TRANSFER_SIZE_MB, SourceDropboxFilesSpec


def test_spec_uses_native_raw_file_delivery_and_hides_generic_streams() -> None:
    schema = SourceDropboxFilesSpec.schema()

    assert "streams" not in schema["properties"]
    delivery = schema["properties"]["delivery_method"]["allOf"][0]
    delivery_type = delivery["properties"]["delivery_type"]
    assert delivery_type["default"] == "use_file_transfer"
    settings = schema["properties"]["file_transfer"]["properties"]
    assert settings["download_chunk_size_mb"]["default"] == 8
    assert settings["download_chunk_size_mb"]["maximum"] == 32
    assert settings["max_file_size_mb"]["default"] == 1024
    assert settings["max_file_size_mb"]["maximum"] == MAX_FILE_TRANSFER_SIZE_MB


def test_rejects_size_limit_above_native_airbyte_staging_limit() -> None:
    with pytest.raises(ValueError, match="max_file_size_mb"):
        SourceDropboxFilesSpec(
            credentials={"auth_type": "access_token", "access_token": "token"},
            file_transfer={"max_file_size_mb": MAX_FILE_TRANSFER_SIZE_MB + 1},
        )
