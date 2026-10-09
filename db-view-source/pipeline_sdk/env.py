"""Typed access to every environment variable the platform injects (stdlib-only).

The platform injects NODE_CONTEXT, BACKEND_URL, optionally API_KEY + AUTHZ_URL,
and the bucket wiring (INPUT_S3_* / ARTIFACT_S3_* endpoint, bucket, SSL flag,
region). It does NOT inject storage credentials: reads and writes go through
pipeline_sdk.storage. This module reads the variables lazily, validates
presence, and raises EnvError naming the exact missing variable instead of a
bare KeyError.

    from pipeline_sdk import env

    env.backend_url()                 # required — clear error when absent
    env.api_key()                     # None when the API-key path is disabled
    env.require_api_key()             # raises EnvError instead of returning None
    env.get_secret("OPENAI_API_KEY")  # user-stored secret, fetched live each call;
                                      # its value is masked ([REDACTED]) in logs

    env.artifact_bucket()             # bucket names, for telling a path's bucket apart
    env.user_id()                     # the calling user's id
"""

from __future__ import annotations

import json
import os

from . import log_mask, util

__all__ = [
    "EnvError",
    "S3Env",
    "api_key",
    "artifact_bucket",
    "artifact_s3",
    "authz_url",
    "backend_url",
    "get_secret",
    "input_bucket",
    "input_s3",
    "node_context_raw",
    "require_api_key",
    "require_artifact_bucket",
    "s3_for_path",
    "use_placeholder_s3_credentials",
    "user_id",
]


class EnvError(RuntimeError):
    """A required environment variable is missing or malformed."""


def _require(name: str, purpose: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise EnvError(f"{name} is not set — {purpose}.")
    return value


def node_context_raw() -> str:
    """The raw NODE_CONTEXT JSON string (use pipeline_sdk.context.load() for
    the parsed, ergonomic form)."""
    return _require("NODE_CONTEXT", "this process is not running as a pipeline node")


def backend_url() -> str:
    """Cluster-internal backend base URL (no trailing slash)."""
    return _require(
        "BACKEND_URL", "this pod was not given the backend address, so SDK storage is unavailable"
    ).rstrip("/")


def api_key() -> str | None:
    """The per-user platform API key, or None — key injection is best-effort
    and the API-key path can be disabled, so absence is a normal state the
    node must handle (per the runtime contract)."""
    return os.environ.get("API_KEY") or None


def require_api_key() -> str:
    """API_KEY, or a clear EnvError when no credential was injected."""
    key = api_key()
    if not key:
        raise EnvError("No auth credential injected (API_KEY absent) — cannot call platform APIs.")
    return key


def authz_url() -> str | None:
    """omop-auth base URL; always accompanies API_KEY, None when absent."""
    return os.environ.get("AUTHZ_URL") or None


def artifact_bucket() -> str | None:
    """Name of the artifact bucket (ARTIFACT_S3_BUCKET), or None when unset."""
    return os.environ.get("ARTIFACT_S3_BUCKET") or None


def require_artifact_bucket() -> str:
    """ARTIFACT_S3_BUCKET, or a clear EnvError when the pod was not given it."""
    bucket = artifact_bucket()
    if not bucket:
        raise EnvError("ARTIFACT_S3_BUCKET is not set — this pod was not given the artifact bucket name.")
    return bucket


def input_bucket() -> str | None:
    """Name of the upload (input) bucket (INPUT_S3_BUCKET), or None when unset."""
    return os.environ.get("INPUT_S3_BUCKET") or None


def _user_id_from_node_context(raw: str) -> str | None:
    # The declared output paths carry the caller's prefix: s3://bucket/{userId}/...
    # — any declared output or input path yields the user id.
    try:
        parsed = json.loads(raw)
        paths = [f.get("path", "") for f in parsed.get("output", {}).get("files", [])]
        paths += [i.get("output", {}).get("path", "") for i in parsed.get("inputs", [])]
        for path in paths:
            if path.startswith("s3://"):
                return path[len("s3://") :].split("/")[1]
    except (ValueError, KeyError, IndexError, AttributeError):
        pass
    return None


def user_id() -> str:
    """The calling user's id (PIPELINE_USER_ID, else recovered from the
    declared s3://bucket/{userId}/... paths in NODE_CONTEXT)."""
    user = os.environ.get("PIPELINE_USER_ID")
    if user:
        return user
    raw = os.environ.get("NODE_CONTEXT")
    if raw:
        recovered = _user_id_from_node_context(raw)
        if recovered:
            return recovered
    raise EnvError(
        "Cannot determine the calling user's id (no PIPELINE_USER_ID and no usable NODE_CONTEXT)."
    )


class S3Env:
    """One bucket's {prefix}_S3_* settings including credentials. The platform
    does not inject credentials into node pods (see pipeline_sdk.storage), so
    this only succeeds where a deployment or a local test supplies them; on the
    platform it raises EnvError."""

    def __init__(self, prefix: str):
        if prefix not in ("INPUT", "ARTIFACT"):
            raise EnvError(f"unknown S3 env prefix {prefix!r} (expected 'INPUT' or 'ARTIFACT')")
        self.prefix = prefix
        purpose = (
            f"the {prefix}_S3_* credentials are not injected into node pods — "
            "use pipeline_sdk.storage (or pipeline_sdk.boto3_compat) to read and write"
        )
        self.endpoint: str = _require(f"{prefix}_S3_ENDPOINT", purpose)  # host:port, no scheme
        self.access_key: str = _require(f"{prefix}_S3_ACCESS_KEY", purpose)
        self.secret_key: str = _require(f"{prefix}_S3_SECRET_KEY", purpose)
        self.bucket: str = _require(f"{prefix}_S3_BUCKET", purpose)
        # Optional by contract: token may be absent or empty; region has a default.
        self.session_token: str | None = os.environ.get(f"{prefix}_S3_SESSION_TOKEN") or None
        self.use_ssl: bool = os.environ.get(f"{prefix}_S3_USE_SSL", "false").lower() == "true"
        self.region: str = os.environ.get(f"{prefix}_S3_REGION", "us-east-1")

    @property
    def endpoint_url(self) -> str:
        """The endpoint with a scheme applied from use_ssl (boto3 wants one)."""
        if "://" in self.endpoint:
            return self.endpoint
        return ("https://" if self.use_ssl else "http://") + self.endpoint

    def boto3_kwargs(self) -> dict:
        """Keyword arguments for boto3.client("s3", **kwargs) — boto3 itself
        is not imported here; the SDK stays stdlib-only."""
        return {
            "endpoint_url": self.endpoint_url,
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
            "aws_session_token": self.session_token,
            "region_name": self.region,
        }

    def duckdb_secret_sql(self, name: str | None = None) -> str:
        """CREATE SECRET statement scoped to this bucket — DuckDB picks the
        matching secret per query from the path's prefix, so both buckets can
        be queried in one connection."""
        secret_name = name or f"{self.prefix.lower()}_secret"
        session_line = f",\n    SESSION_TOKEN '{self.session_token}'" if self.session_token else ""
        return f"""CREATE SECRET {secret_name} (
    TYPE S3,
    KEY_ID '{self.access_key}',
    SECRET '{self.secret_key}',
    ENDPOINT '{self.endpoint}',
    SCOPE 's3://{self.bucket}',
    URL_STYLE 'path',
    USE_SSL {str(self.use_ssl).lower()},
    REGION '{self.region}'{session_line}
)"""


def get_secret(name: str) -> str:
    """Fetch one of the caller's own secrets by name (docs/SECRET_MANAGER.md) —
    a value the user stored once via the secret-manager UI panel, never
    written into this node's config or the saved pipeline. Named "get_secret",
    not "getEnv", because the value was never an actual environment variable:
    every call round-trips to the backend (which holds the real credential in
    OpenBao), so a secret added or changed after this pod started is picked up
    immediately — there is no pod-restart problem for it to have.

    Names are case-sensitive. The UI uppercases names as they are typed, so
    a secret stored there as "OPENAI_API_KEY" must be requested exactly
    that way — get_secret("openai_api_key") will not find it.

    The returned value is also registered for log masking: from now on,
    anything this process prints that contains it shows `[REDACTED]` instead
    (see pipeline_sdk.log_mask for what that does and does not cover). There is
    no opt-out.

    Raises EnvError if no secret with that name exists for this user, or if
    the secret manager feature is unavailable in this deployment.
    """
    endpoint = "/api/node/secret/get"
    try:
        response = util.post_json(
            f"{backend_url()}{endpoint}", {"name": name}, token=require_api_key(), label=endpoint
        )
    except util.ApiError as exc:
        if exc.status == 404:
            raise EnvError(f"No secret named {name!r} is stored for this user.") from exc
        raise EnvError(f"Could not fetch secret {name!r}: {exc}") from exc
    value = response.get("value")
    if not isinstance(value, str):
        raise EnvError(f"Secret {name!r}: malformed response from backend")
    log_mask.register(value)
    return value


def input_s3() -> S3Env:
    """Upload (input) bucket credentials — user source files."""
    return S3Env("INPUT")


def artifact_s3() -> S3Env:
    """Artifact (output) bucket credentials — workflow outputs."""
    return S3Env("ARTIFACT")


def s3_for_path(path: str) -> S3Env:
    """The credential set for an s3:// path: the bucket in the path selects it
    (artifact bucket -> ARTIFACT, anything else -> INPUT), matching where
    inputs may live — source files in the upload bucket, upstream outputs in
    the artifact bucket."""
    bucket = path[len("s3://") :].split("/", 1)[0] if path.startswith("s3://") else path.split("/", 1)[0]
    return artifact_s3() if bucket == artifact_bucket() else input_s3()


def use_placeholder_s3_credentials() -> None:
    """Fill any unset INPUT_/ARTIFACT_S3_{ENDPOINT,ACCESS_KEY,SECRET_KEY} with
    inert placeholders. For `pipeline_sdk.boto3_compat` only: legacy node code
    still reads these variables before it builds a client, but the adapter
    behind that client ignores them (all bytes go through the backend)."""
    for prefix in ("INPUT", "ARTIFACT"):
        os.environ.setdefault(f"{prefix}_S3_ENDPOINT", "storage-v2")
        os.environ.setdefault(f"{prefix}_S3_ACCESS_KEY", "backend-mediated")
        os.environ.setdefault(f"{prefix}_S3_SECRET_KEY", "backend-mediated")
