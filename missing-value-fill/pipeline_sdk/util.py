"""Shared helpers with no dependency on the rest of the SDK (stdlib-only).

This is the bottom layer: `util` <- `env` <- `storage` <- `context`. Nothing
here reads environment variables (that is `pipeline_sdk.env`'s job) or knows
about storage; callers pass in the URL and the bearer token.

    from pipeline_sdk import util

    result = util.post_json(url, {"name": "X"}, token=token, label="/api/node/secret/get")
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
from typing import Any

__all__ = ["ApiError", "MAX_RETRIES", "RETRY_DELAY_S", "diag", "error_body", "post_json"]

MAX_RETRIES = 3
RETRY_DELAY_S = 1.0


class ApiError(RuntimeError):
    """A backend call failed. When the failure came from an HTTP response,
    `status` carries the code and `detail` the server's error message / body
    snippet (both also appear in str(error))."""

    def __init__(self, message: str, *, status: int | None = None, detail: str | None = None):
        super().__init__(message)
        self.status = status
        self.detail = detail


def diag(message: str) -> None:
    """Diagnostics to stderr. Pod logs are archived and shown in the execution
    UI, so every server-reported error is printed here as well as raised."""
    print(f"[pipeline_sdk] {message}", file=sys.stderr, flush=True)


def error_body(error: urllib.error.HTTPError) -> str:
    """The first part of an HTTP error response's body ('' if unreadable)."""
    try:
        return error.read().decode("utf-8", "replace")[:300]
    except Exception:
        return ""


def post_json(
    url: str,
    payload: dict,
    *,
    token: str,
    label: str | None = None,
    retries: int = MAX_RETRIES,
    timeout: float = 60,
) -> Any:
    """POST JSON with bearer auth and retry; return the decoded JSON response.

    4xx responses fail immediately (retrying cannot help); 5xx responses and
    network errors are retried with exponential backoff. Every failure raises
    ApiError. `label` names the call in messages and logs (default: the URL).
    """
    name = label or url
    body = json.dumps(payload).encode("utf-8")
    last_error: ApiError | None = None
    for attempt in range(retries):
        request = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            raw = error_body(error)
            try:
                detail = json.loads(raw).get("error", "") or raw
            except Exception:
                detail = raw
            if 400 <= error.code < 500:
                diag(f"POST {name} rejected ({error.code}): {detail}")
                raise ApiError(f"{name} failed ({error.code}): {detail}", status=error.code, detail=detail) from error
            last_error = ApiError(f"{name} failed ({error.code}): {detail}", status=error.code, detail=detail)
            diag(f"POST {name} attempt {attempt + 1}/{retries} failed ({error.code}): {detail}")
        except (urllib.error.URLError, TimeoutError) as error:
            last_error = ApiError(f"{name} unreachable: {error}")
            diag(f"POST {name} attempt {attempt + 1}/{retries} unreachable: {error}")
        time.sleep(RETRY_DELAY_S * (2**attempt))
    raise last_error or ApiError(f"{name} failed")
