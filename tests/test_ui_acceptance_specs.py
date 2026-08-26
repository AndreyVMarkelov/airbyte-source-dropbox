from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft7Validator

ROOT = Path(__file__).parents[1]


def _spec(path: str) -> dict[str, object]:
    return json.loads((ROOT / path).read_text())["connectionSpecification"]


def _validate(spec_path: str, config: dict[str, object]) -> None:
    Draft7Validator(_spec(spec_path)).validate(config)


def _access_token_config(**extra: object) -> dict[str, object]:
    return {
        "credentials": {"auth_type": "access_token", "access_token": "token"},
        **extra,
    }


def _variants(spec_path: str, field: str) -> dict[str, dict[str, object]]:
    schema = _spec(spec_path)
    properties = schema["properties"]  # type: ignore[index]
    field_schema = properties[field]  # type: ignore[index]
    return {
        variant["properties"]["mode"]["const"]: variant  # type: ignore[index]
        for variant in field_schema["oneOf"]  # type: ignore[index]
    }


def test_source_spec_uses_conditional_team_context_path_root_and_namespace_selection() -> None:
    spec_path = "source_dropbox/spec.json"

    team = _variants(spec_path, "team_context")
    assert set(team) == {"none", "user", "admin"}
    assert team["none"]["properties"]["select_user"]["airbyte_hidden"] is True
    assert team["none"]["properties"]["select_user"]["type"] == "null"
    assert team["none"]["properties"]["select_admin"]["airbyte_hidden"] is True
    assert team["none"]["properties"]["select_admin"]["type"] == "null"
    assert team["user"]["required"] == ["mode", "select_user"]
    assert team["admin"]["required"] == ["mode", "select_admin"]

    path_root = _variants(spec_path, "path_root")
    assert set(path_root) == {"default", "home", "root", "namespace_id"}
    assert path_root["default"]["properties"]["namespace_id"]["airbyte_hidden"] is True
    assert path_root["default"]["properties"]["namespace_id"]["type"] == "null"
    assert path_root["home"]["properties"]["namespace_id"]["airbyte_hidden"] is True
    assert path_root["root"]["properties"]["namespace_id"]["airbyte_hidden"] is True
    assert path_root["namespace_id"]["required"] == ["mode", "namespace_id"]

    namespace_selection = _variants(spec_path, "namespace_selection")
    assert set(namespace_selection) == {"current", "selected", "all_accessible"}
    assert (
        namespace_selection["current"]["properties"]["namespace_ids"]["airbyte_hidden"]
        is True
    )
    assert namespace_selection["current"]["properties"]["namespace_ids"]["maxItems"] == 0
    assert (
        namespace_selection["all_accessible"]["properties"]["namespace_ids"][
            "airbyte_hidden"
        ]
        is True
    )
    assert namespace_selection["selected"]["required"] == ["mode", "namespace_ids"]
    assert namespace_selection["selected"]["properties"]["namespace_ids"]["minItems"] == 1
    assert namespace_selection["selected"]["properties"]["namespace_ids"]["uniqueItems"] is True


def test_source_spec_accepts_existing_conditional_config_shapes() -> None:
    spec_path = "source_dropbox/spec.json"

    for config in [
        _access_token_config(team_context={"mode": "none"}),
        _access_token_config(team_context={"mode": "user", "select_user": "dbmid:member"}),
        _access_token_config(team_context={"mode": "admin", "select_admin": "dbmid:admin"}),
        _access_token_config(path_root={"mode": "default"}),
        _access_token_config(path_root={"mode": "home"}),
        _access_token_config(path_root={"mode": "root"}),
        _access_token_config(path_root={"mode": "namespace_id", "namespace_id": "123"}),
        _access_token_config(namespace_selection={"mode": "current"}),
        _access_token_config(
            team_context={
                "mode": "none",
                "select_user": None,
                "select_admin": None,
            },
            path_root={"mode": "default", "namespace_id": None},
            namespace_selection={"mode": "current", "namespace_ids": []},
        ),
        _access_token_config(
            namespace_selection={"mode": "selected", "namespace_ids": ["123", "456"]}
        ),
        _access_token_config(namespace_selection={"mode": "all_accessible"}),
        _access_token_config(
            namespace_selection={"mode": "all_accessible", "namespace_ids": []}
        ),
    ]:
        _validate(spec_path, config)


def test_source_spec_rejects_non_default_legacy_fields() -> None:
    spec_path = "source_dropbox/spec.json"
    validator = Draft7Validator(_spec(spec_path))

    for config in [
        _access_token_config(team_context={"mode": "none", "select_user": "dbmid:x"}),
        _access_token_config(path_root={"mode": "default", "namespace_id": "123"}),
        _access_token_config(namespace_selection={"mode": "current", "namespace_ids": ["123"]}),
    ]:
        assert list(validator.iter_errors(config))


def test_destination_specs_use_conditional_team_context_and_path_root() -> None:
    for spec_path in [
        "destination_dropbox/spec.json",
        "connectors/destination-dropbox-files/destination_dropbox_files/spec.json",
    ]:
        team = _variants(spec_path, "team_context")
        path_root = _variants(spec_path, "path_root")
        assert set(team) == {"none", "user", "admin"}
        assert team["none"]["properties"]["select_user"]["airbyte_hidden"] is True
        assert team["none"]["properties"]["select_user"]["type"] == "null"
        assert team["none"]["properties"]["select_admin"]["airbyte_hidden"] is True
        assert set(path_root) == {"default", "home", "root", "namespace_id"}
        assert path_root["default"]["properties"]["namespace_id"]["airbyte_hidden"] is True
        assert path_root["default"]["properties"]["namespace_id"]["type"] == "null"

        _validate(spec_path, _access_token_config(team_context={"mode": "none"}))
        _validate(
            spec_path,
            _access_token_config(
                team_context={
                    "mode": "none",
                    "select_user": None,
                    "select_admin": None,
                },
                path_root={"mode": "default", "namespace_id": None},
            ),
        )
        _validate(
            spec_path,
            _access_token_config(team_context={"mode": "user", "select_user": "dbmid:member"}),
        )
        _validate(
            spec_path,
            _access_token_config(
                path_root={"mode": "namespace_id", "namespace_id": "namespace"}
            ),
        )


def test_airbyte_dockerfiles_define_expected_entrypoints() -> None:
    expected = {
        "docker/source.Dockerfile": "source-dropbox",
        "docker/destination.Dockerfile": "destination-dropbox",
        "docker/source-files.Dockerfile": "source-dropbox-files",
        "docker/destination-files.Dockerfile": "destination-dropbox-files",
    }
    for dockerfile, entrypoint in expected.items():
        text = (ROOT / dockerfile).read_text()
        assert f'ENV AIRBYTE_ENTRYPOINT="{entrypoint}"' in text
        assert f'ENTRYPOINT ["{entrypoint}"]' in text
