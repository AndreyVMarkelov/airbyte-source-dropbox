from __future__ import annotations

from typing import Any, Literal

from airbyte_cdk.sources.file_based.config.abstract_file_based_spec import (
    AbstractFileBasedSpec,
    DeliverRawFiles,
)
from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.config.unstructured_format import UnstructuredFormat
from airbyte_cdk.utils.oneof_option_config import OneOfOptionConfig
from pydantic.v1 import BaseModel, Field

CDK_FILE_SIZE_LIMIT_BYTES = 1_500_000_000
MAX_FILE_TRANSFER_SIZE_MB = CDK_FILE_SIZE_LIMIT_BYTES // (1024 * 1024)


def _raw_files_stream() -> FileBasedStreamConfig:
    # FileBasedSource requires a stream configuration even though raw transfer never parses it.
    return FileBasedStreamConfig(name="raw_files", format=UnstructuredFormat())


class OAuthCredentials(BaseModel):
    class Config(OneOfOptionConfig):
        title = "OAuth 2.0 refresh token (recommended)"
        discriminator = "auth_type"

    auth_type: Literal["oauth2_pkce"] = Field("oauth2_pkce", const=True)
    app_key: str = Field(title="Dropbox App Key")
    refresh_token: str = Field(title="Dropbox Refresh Token", airbyte_secret=True)


class AccessTokenCredentials(BaseModel):
    class Config(OneOfOptionConfig):
        title = "Access token (development/manual testing only)"
        discriminator = "auth_type"

    auth_type: Literal["access_token"] = Field("access_token", const=True)
    access_token: str = Field(title="Dropbox Access Token", airbyte_secret=True)


class FileTransferSettings(BaseModel):
    download_chunk_size_mb: int = Field(
        8,
        ge=1,
        le=32,
        title="Download Chunk Size (MB)",
        description="Bytes read from Dropbox in each bounded file-transfer download chunk.",
    )
    max_file_size_mb: int = Field(
        1024,
        ge=1,
        le=MAX_FILE_TRANSFER_SIZE_MB,
        title="Maximum File Size (MB)",
        description=(
            "Files above this limit are skipped and reported without a file reference. "
            f"Limited to {MAX_FILE_TRANSFER_SIZE_MB} MiB by Airbyte native staging."
        ),
    )


class TeamContextCurrent(BaseModel):
    class Config(OneOfOptionConfig):
        title = "Use current account"
        discriminator = "mode"

    mode: Literal["none"] = Field("none", const=True, title="Team context mode")
    select_user: Literal[None] = Field(
        None,
        title="Legacy selected member",
        airbyte_hidden=True,
    )
    select_admin: Literal[None] = Field(
        None,
        title="Legacy selected admin",
        airbyte_hidden=True,
    )


class TeamContextUser(BaseModel):
    class Config(OneOfOptionConfig):
        title = "Act as team member"
        discriminator = "mode"

    mode: Literal["user"] = Field("user", const=True, title="Team context mode")
    select_user: str = Field(
        title="Select member",
        description="Dropbox Business team member ID, for example dbmid:...",
    )
    select_admin: Literal[None] = Field(
        None,
        title="Legacy selected admin",
        airbyte_hidden=True,
    )


class TeamContextAdmin(BaseModel):
    class Config(OneOfOptionConfig):
        title = "Act as team admin"
        discriminator = "mode"

    mode: Literal["admin"] = Field("admin", const=True, title="Team context mode")
    select_admin: str = Field(
        title="Select admin",
        description="Dropbox Business admin member ID, for example dbmid:...",
    )
    select_user: Literal[None] = Field(
        None,
        title="Legacy selected member",
        airbyte_hidden=True,
    )


TeamContext = TeamContextCurrent | TeamContextUser | TeamContextAdmin


class PathRootDefault(BaseModel):
    class Config(OneOfOptionConfig):
        title = "Default Dropbox root"
        discriminator = "mode"

    mode: Literal["default"] = Field("default", const=True, title="Root mode")
    namespace_id: Literal[None] = Field(
        None,
        title="Legacy namespace ID",
        airbyte_hidden=True,
    )


class PathRootHome(BaseModel):
    class Config(OneOfOptionConfig):
        title = "Member home"
        discriminator = "mode"

    mode: Literal["home"] = Field("home", const=True, title="Root mode")
    namespace_id: Literal[None] = Field(
        None,
        title="Legacy namespace ID",
        airbyte_hidden=True,
    )


class PathRootRoot(BaseModel):
    class Config(OneOfOptionConfig):
        title = "Team/account root"
        discriminator = "mode"

    mode: Literal["root"] = Field("root", const=True, title="Root mode")
    namespace_id: Literal[None] = Field(
        None,
        title="Legacy namespace ID",
        airbyte_hidden=True,
    )


class PathRootNamespace(BaseModel):
    class Config(OneOfOptionConfig):
        title = "Explicit namespace ID"
        discriminator = "mode"

    mode: Literal["namespace_id"] = Field("namespace_id", const=True, title="Root mode")
    namespace_id: str = Field(
        title="Namespace ID",
        description="Explicit Dropbox namespace/root ID.",
    )


PathRootConfig = PathRootDefault | PathRootHome | PathRootRoot | PathRootNamespace


class SourceDropboxFilesSpec(AbstractFileBasedSpec):
    class Config:
        title = "Dropbox Files Source Spec"

    credentials: OAuthCredentials | AccessTokenCredentials = Field(
        title="Authentication", discriminator="auth_type"
    )
    path: str = Field(
        "",
        title="Root Path",
        description="Dropbox folder path to transfer. Use an empty string for the app root.",
    )
    recursive: bool = Field(True, title="Recursive")
    team_context: TeamContext = Field(
        default_factory=TeamContextCurrent,
        title="Dropbox Business",
        discriminator="mode",
    )
    path_root: PathRootConfig = Field(
        default_factory=PathRootDefault,
        title="Dropbox Root",
        discriminator="mode",
    )
    rename_policy: Literal["ignore", "propagate"] = Field(
        "ignore",
        title="Rename Policy",
        description=(
            "Whether to ignore source renames or propagate them as Dropbox destination moves."
        ),
    )
    delete_policy: Literal["ignore", "delete"] = Field(
        "ignore",
        title="Delete Policy",
        description=(
            "Whether to retain destination-only files or delete confirmed source deletions."
        ),
    )
    file_transfer: FileTransferSettings = Field(default_factory=FileTransferSettings)
    delivery_method: DeliverRawFiles = Field(
        default_factory=DeliverRawFiles,
        title="Delivery Method",
        description="Original bytes are delivered through Airbyte native File Transfer.",
    )
    streams: list[FileBasedStreamConfig] = Field(
        default_factory=lambda: [_raw_files_stream()],
        airbyte_hidden=True,
    )

    @classmethod
    def documentation_url(cls) -> str:
        return "https://github.com/AndreyVMarkelov/airbyte-source-dropbox"

    @classmethod
    def schema(cls, *args: Any, **kwargs: Any) -> dict[str, Any]:
        schema = super().schema(*args, **kwargs)
        schema["properties"].pop("streams", None)
        return schema
