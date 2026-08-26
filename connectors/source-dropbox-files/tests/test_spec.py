import pytest
from jsonschema import Draft7Validator

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
    assert schema["properties"]["rename_policy"]["default"] == "ignore"
    assert schema["properties"]["delete_policy"]["default"] == "ignore"


def test_rejects_size_limit_above_native_airbyte_staging_limit() -> None:
    with pytest.raises(ValueError, match="max_file_size_mb"):
        SourceDropboxFilesSpec(
            credentials={"auth_type": "access_token", "access_token": "token"},
            file_transfer={"max_file_size_mb": MAX_FILE_TRANSFER_SIZE_MB + 1},
        )


def test_validates_propagation_policies() -> None:
    config = SourceDropboxFilesSpec(
        credentials={"auth_type": "access_token", "access_token": "token"},
        rename_policy="propagate",
        delete_policy="delete",
    )
    assert config.rename_policy == "propagate"
    assert config.delete_policy == "delete"

    with pytest.raises(ValueError, match="rename_policy"):
        SourceDropboxFilesSpec(
            credentials={"auth_type": "access_token", "access_token": "token"},
            rename_policy="overwrite",
        )


def test_business_context_and_path_root_are_conditional_schema_variants() -> None:
    schema = SourceDropboxFilesSpec.schema()
    team_variants = {
        variant["properties"]["mode"]["const"]: variant
        for variant in schema["properties"]["team_context"]["oneOf"]
    }
    root_variants = {
        variant["properties"]["mode"]["const"]: variant
        for variant in schema["properties"]["path_root"]["oneOf"]
    }

    assert set(team_variants) == {"none", "user", "admin"}
    assert team_variants["none"]["properties"]["select_user"]["airbyte_hidden"] is True
    assert team_variants["none"]["properties"]["select_user"]["type"] == "null"
    assert team_variants["none"]["properties"]["select_admin"]["airbyte_hidden"] is True
    assert team_variants["none"]["properties"]["select_admin"]["type"] == "null"
    assert set(team_variants["user"]["required"]) == {"mode", "select_user"}
    assert set(team_variants["admin"]["required"]) == {"mode", "select_admin"}

    assert set(root_variants) == {"default", "home", "root", "namespace_id"}
    assert root_variants["default"]["properties"]["namespace_id"]["airbyte_hidden"] is True
    assert root_variants["default"]["properties"]["namespace_id"]["type"] == "null"
    assert root_variants["home"]["properties"]["namespace_id"]["airbyte_hidden"] is True
    assert root_variants["root"]["properties"]["namespace_id"]["airbyte_hidden"] is True
    assert set(root_variants["namespace_id"]["required"]) == {"mode", "namespace_id"}


def test_existing_business_context_and_path_root_configs_still_validate() -> None:
    validator = Draft7Validator(SourceDropboxFilesSpec.schema())
    base = {"credentials": {"auth_type": "access_token", "access_token": "token"}}

    for config in [
        {**base, "team_context": {"mode": "none"}},
        {
            **base,
            "team_context": {
                "mode": "none",
                "select_user": None,
                "select_admin": None,
            },
        },
        {**base, "team_context": {"mode": "user", "select_user": "dbmid:member"}},
        {**base, "team_context": {"mode": "admin", "select_admin": "dbmid:admin"}},
        {**base, "path_root": {"mode": "default"}},
        {**base, "path_root": {"mode": "default", "namespace_id": None}},
        {**base, "path_root": {"mode": "home"}},
        {**base, "path_root": {"mode": "root"}},
        {**base, "path_root": {"mode": "namespace_id", "namespace_id": "123"}},
    ]:
        validator.validate(config)
