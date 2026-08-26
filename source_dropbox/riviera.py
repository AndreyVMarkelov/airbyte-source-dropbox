from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from dropbox.exceptions import ApiError, AuthError, BadInputError, RateLimitError
from dropbox.riviera import FileIdOrUrl, GetMarkdownAsyncCheckResult

from source_dropbox.errors import (
    DropboxExtractionInfrastructureError,
    DropboxRateLimitError,
    raise_auth_or_refresh_error,
)


@dataclass(frozen=True)
class MarkdownExtraction:
    markdown: str | None
    extraction_status: str
    error_type: str | None = None
    error_code: str | None = None
    error_details: dict[str, Any] | None = None
    error_message: str | None = None


def extract_markdown(
    file_id: str,
    timeout_seconds: int,
    *,
    client_for_namespace: Callable[[str | None], Any],
    check_markdown_job: Callable[[str, Any], GetMarkdownAsyncCheckResult],
    sleeper: Callable[[float], None],
    monotonic_clock: Callable[[], float],
    namespace_id: str | None = None,
) -> MarkdownExtraction:
    """Convert a Dropbox file to Markdown through Riviera's asynchronous API."""
    client = client_for_namespace(namespace_id)
    try:
        launch = client.riviera_get_markdown_async(
            file_id_or_url=FileIdOrUrl.file_id(file_id),
            enable_ocr=False,
            embed_images=False,
        )
    except (AuthError, BadInputError) as exc:
        raise_auth_or_refresh_error(exc, required_scope="files.content.read")
    except RateLimitError as exc:
        raise DropboxRateLimitError("Dropbox rate limited content extraction.") from exc
    except ApiError as exc:
        raise DropboxExtractionInfrastructureError(
            "Riviera could not launch extraction."
        ) from exc

    if not getattr(launch, "is_async_job_id", lambda: False)():
        raise DropboxExtractionInfrastructureError(
            "Riviera returned an invalid extraction launch."
        )

    deadline = monotonic_clock() + timeout_seconds
    delay = 1.0
    job_id = launch.get_async_job_id()
    while True:
        if monotonic_clock() >= deadline:
            return _timed_out(timeout_seconds)
        result = check_markdown_job(job_id, client)
        if not isinstance(result, GetMarkdownAsyncCheckResult):
            raise DropboxExtractionInfrastructureError(
                "Riviera returned an invalid extraction status."
            )
        if result.is_complete():
            markdown = result.get_complete().markdown
            if not isinstance(markdown, str):
                raise DropboxExtractionInfrastructureError(
                    "Riviera returned an invalid Markdown result."
                )
            return MarkdownExtraction(markdown=markdown, extraction_status="succeeded")
        if result.is_failed():
            return normalize_markdown_failure(result.get_failed())
        if result.is_other():
            return MarkdownExtraction(
                markdown=None,
                extraction_status="failed",
                error_type="unknown_status",
                error_message="Riviera returned an unknown extraction status.",
            )
        if not result.is_in_progress():
            raise DropboxExtractionInfrastructureError(
                "Riviera returned an invalid extraction status."
            )

        remaining = deadline - monotonic_clock()
        if remaining <= 0:
            return _timed_out(timeout_seconds)
        sleeper(min(delay, remaining))
        delay = min(delay * 2, 10.0)


def check_markdown_job(job_id: str, *, client: Any) -> GetMarkdownAsyncCheckResult:
    try:
        return client.riviera_get_markdown_async_check(job_id)
    except (AuthError, BadInputError) as exc:
        raise_auth_or_refresh_error(exc, required_scope="files.content.read")
    except RateLimitError as exc:
        raise DropboxRateLimitError("Dropbox rate limited content extraction.") from exc
    except ApiError as exc:
        raise DropboxExtractionInfrastructureError(
            "Riviera could not check extraction status."
        ) from exc


def normalize_markdown_failure(error: Any) -> MarkdownExtraction:
    error_code = getattr(error.error_code, "_tag", None)
    details = getattr(error, "error_details", None)
    error_type = getattr(details, "_tag", None)
    file_error_types = {
        "unsupported_format_error",
        "limit_exceeded_error",
        "conversion_failure_error",
        "not_found_error",
        "is_a_folder_error",
        "user_error",
    }
    if error_type in file_error_types:
        normalized_details: dict[str, Any] = {"type": error_type}
        error_message = f"Riviera extraction failed: {error_type}."
        if error_type == "user_error":
            message = details.get_user_error()
            if isinstance(message, str):
                normalized_details["message"] = message
                error_message = message
        return MarkdownExtraction(
            markdown=None,
            extraction_status="failed",
            error_type=error_type,
            error_code=error_code,
            error_details=normalized_details,
            error_message=error_message,
        )

    if error_code in {
        "access_error",
        "ratelimit_error",
        "unavailable",
        "api_error",
        "bad_request",
        "unknown_error",
        "other",
    }:
        raise DropboxExtractionInfrastructureError(
            f"Riviera extraction failed with systemic error: {error_code}."
        )
    raise DropboxExtractionInfrastructureError(
        "Riviera returned an unexpected extraction error."
    )


def _timed_out(timeout_seconds: int) -> MarkdownExtraction:
    return MarkdownExtraction(
        markdown=None,
        extraction_status="timed_out",
        error_type="timeout",
        error_message=f"Riviera extraction exceeded {timeout_seconds} seconds.",
    )
