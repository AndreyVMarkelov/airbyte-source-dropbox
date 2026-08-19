from source_dropbox_files.spec import SourceDropboxFilesSpec


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
