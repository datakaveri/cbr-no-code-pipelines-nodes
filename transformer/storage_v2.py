"""Storage v2 client for custom pipeline nodes (stdlib-only).

Writes go through the backend's upload-session REST API
(docs/STORAGE_V2_MULTIPART_DESIGN.md): the node never holds a writable
storage credential. Known-size writes (upload_file/write_bytes) declare the
size and the SERVER picks the strategy — one presigned PUT for small files,
a multipart session otherwise. Streaming writes (open_write) skip the size
declaration and go multipart with incremental quota metering. Either way the
server's Complete gate verifies what actually landed before anything becomes
a file.

Environment (injected into node pods by the platform):
    BACKEND_URL  cluster-internal backend base URL (required for writes/reads)
    API_KEY      bearer credential resolved server-side to the node's owner

Paths are relative to the caller's own storage prefix: "results/output.parquet"
targets {userId}/results/output.parquet in the artifact bucket. Absolute
"s3://bucket/..." / "{bucket}/..." paths are accepted for reads.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import shutil
import sys
import time
import urllib.error
import urllib.request

__all__ = [
    "open_write",
    "read",
    "read_to_file",
    "upload_file",
    "write_bytes",
    "StorageError",
    "StorageQuotaExceeded",
]

_URL_BATCH = 20
_MAX_RETRIES = 3
_RETRY_DELAY_S = 1.0
_COPY_BLOCK = 8 * 1024 * 1024


class StorageError(RuntimeError):
    """A storage operation failed. When the failure came from an HTTP response,
    `status` carries the code and `detail` the server's error message / body
    snippet (both also appear in str(error))."""

    def __init__(self, message: str, *, status: int | None = None, detail: str | None = None):
        super().__init__(message)
        self.status = status
        self.detail = detail


class StorageQuotaExceeded(StorageError):
    """The write hit the owner's storage quota — a distinct, documented error
    rather than a mysterious half-write."""


def _backend_url() -> str:
    url = os.environ.get("BACKEND_URL")
    if not url:
        raise StorageError(
            "BACKEND_URL is not set — this pod was not given the backend address, "
            "so SDK storage writes are unavailable."
        )
    return url.rstrip("/")


def _api_key() -> str:
    key = os.environ.get("API_KEY")
    if not key:
        raise StorageError("API_KEY is not set — cannot authenticate to the storage API.")
    return key


def _user_id() -> str:
    ctx = os.environ.get("NODE_CONTEXT")
    user = os.environ.get("PIPELINE_USER_ID")
    if user:
        return user
    if ctx:
        # The declared output paths carry the caller's prefix:
        # s3://bucket/{userId}/... — any of them yields the user id.
        try:
            parsed = json.loads(ctx)
            files = parsed.get("output", {}).get("files", [])
            for file_info in files:
                path = file_info.get("path", "")
                if path.startswith("s3://"):
                    return path[len("s3://"):].split("/")[1]
            for inp in parsed.get("inputs", []):
                path = inp.get("output", {}).get("path", "")
                if path.startswith("s3://"):
                    return path[len("s3://"):].split("/")[1]
        except (ValueError, KeyError, IndexError):
            pass
    raise StorageError(
        "Cannot determine the calling user's id (no PIPELINE_USER_ID and no usable NODE_CONTEXT)."
    )


def _normalize_write_path(display_path: str) -> str:
    """Accepts a caller-relative path ("results/output.parquet") or the
    absolute "s3://bucket/{userId}/..." form NODE_CONTEXT declares outputs
    with, and returns the {userId}/... key the backend expects."""
    user_id = _user_id()
    if display_path.startswith("s3://"):
        display_path = display_path[len("s3://"):].split("/", 1)[1]
    display_path = display_path.lstrip("/")
    if display_path.startswith(f"{user_id}/"):
        display_path = display_path[len(user_id) + 1:]
    return f"{user_id}/{display_path}"


def _log(message: str) -> None:
    # Diagnostics to stderr — pod logs are archived and shown in the execution
    # UI, so every server-reported error is printed here as well as raised.
    print(f"[pipeline_sdk] {message}", file=sys.stderr, flush=True)


def _error_body(error: urllib.error.HTTPError) -> str:
    try:
        return error.read().decode("utf-8", "replace")[:300]
    except Exception:
        return ""


def _post(endpoint: str, payload: dict, *, retries: int = _MAX_RETRIES) -> dict:
    """POST JSON to the backend node-runtime API with bearer auth + retry."""
    body = json.dumps(payload).encode("utf-8")
    url = f"{_backend_url()}{endpoint}"
    last_error: Exception | None = None
    for attempt in range(retries):
        request = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {_api_key()}",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            raw = _error_body(error)
            try:
                detail = json.loads(raw).get("error", "") or raw
            except Exception:
                detail = raw
            if error.code == 403 and "storage limit" in detail:
                _log(f"POST {endpoint} → quota exceeded: {detail}")
                raise StorageQuotaExceeded(detail, status=error.code, detail=detail) from error
            if error.code == 410:
                _log(f"POST {endpoint} → 410 Gone: {detail}")
                raise StorageError(
                    f"{endpoint} is gone — this SDK/backend pair is mismatched: {detail}",
                    status=410,
                    detail=detail,
                ) from error
            if 400 <= error.code < 500:
                _log(f"POST {endpoint} rejected ({error.code}): {detail}")
                raise StorageError(
                    f"{endpoint} failed ({error.code}): {detail}", status=error.code, detail=detail
                ) from error
            last_error = StorageError(
                f"{endpoint} failed ({error.code}): {detail}", status=error.code, detail=detail
            )
            _log(f"POST {endpoint} attempt {attempt + 1}/{retries} failed ({error.code}): {detail}")
        except (urllib.error.URLError, TimeoutError) as error:
            last_error = StorageError(f"{endpoint} unreachable: {error}")
            _log(f"POST {endpoint} attempt {attempt + 1}/{retries} unreachable: {error}")
        time.sleep(_RETRY_DELAY_S * (2**attempt))
    raise last_error or StorageError(f"{endpoint} failed")


def _put_part(url: str, data: bytes, checksum_b64: str, pinned: bool, *, retries: int = _MAX_RETRIES) -> None:
    last_error: Exception | None = None
    for attempt in range(retries):
        headers = {"Content-Type": "application/octet-stream"}
        if pinned:
            # The checksum is signed into the URL — the store rejects any
            # other content with BadDigest, so the header must match exactly.
            headers["x-amz-checksum-sha256"] = checksum_b64
        request = urllib.request.Request(url, data=data, method="PUT", headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=300):
                return
        except urllib.error.HTTPError as error:
            body = _error_body(error)
            if error.code == 403:
                # Expired part URL — the caller re-signs and retries.
                raise StorageError("part URL expired", status=403, detail=body) from error
            if 400 <= error.code < 500:
                _log(f"part upload rejected ({error.code}): {body}")
                raise StorageError(
                    f"part upload failed ({error.code}): {body}", status=error.code, detail=body
                ) from error
            last_error = StorageError(
                f"part upload failed ({error.code}): {body}", status=error.code, detail=body
            )
            _log(f"part upload attempt {attempt + 1}/{retries} failed ({error.code}): {body}")
        except (urllib.error.URLError, TimeoutError) as error:
            last_error = StorageError(f"part upload failed: {error}")
            _log(f"part upload attempt {attempt + 1}/{retries} failed: {error}")
        time.sleep(_RETRY_DELAY_S * (2**attempt))
    raise last_error or StorageError("part upload failed")


class _SessionWriter(io.RawIOBase):
    """File-like writer over an upload session: buffers to the part size,
    uploads parts with per-part sha256 and retry, fetches URL batches as
    needed. A clean close() commits (server-side Complete gate); closing
    while an exception is propagating, or being garbage-collected without a
    commit, ABORTS — a node that crashes mid-write must never publish a
    truncated file as its official output."""

    def __init__(
        self,
        display_path: str,
        chunk_bytes: int | None,
        content_type: str | None,
        *,
        _created: dict | None = None,
    ):
        super().__init__()
        self._buffer = bytearray()
        self._part_number = 1
        self._urls: dict[int, str] = {}
        self._committed: dict | None = None
        self._aborted = False

        # The part size is SERVER-chosen by default: omit chunkBytes and
        # buffer to the declaredChunkBytes the backend answers with. An
        # explicit chunk_bytes is still honored for nodes that genuinely
        # can't buffer the server's choice. `_created` adopts a session the
        # unified /create endpoint already opened (its "multipart" answer).
        if _created is None:
            payload: dict = {
                "path": _normalize_write_path(display_path),
                "contentType": content_type or "application/octet-stream",
                "firstPartCount": 1,
            }
            if chunk_bytes is not None:
                payload["chunkBytes"] = chunk_bytes
            _created = _post("/api/node/storage/create-session", payload)
        created = _created
        self._chunk_bytes: int = int(created["declaredChunkBytes"])
        self._session_id: int = created["sessionId"]
        self._checksums_pinned: bool = bool(created.get("checksumsEnabled"))
        # The first batch was signed without checksums known upfront; when
        # pinning is on those URLs are unusable for arbitrary content, so we
        # re-request per part with the real hash instead.
        if not self._checksums_pinned:
            for part in created.get("partUrls", []):
                self._urls[part["partNumber"]] = part["url"]

    # -- io plumbing ---------------------------------------------------------
    def writable(self) -> bool:
        return True

    def write(self, data) -> int:  # type: ignore[override]
        if self._committed is not None or self._aborted:
            raise ValueError("write to a closed storage file")
        view = memoryview(bytes(data))
        self._buffer.extend(view)
        while len(self._buffer) >= self._chunk_bytes:
            chunk = bytes(self._buffer[: self._chunk_bytes])
            del self._buffer[: self._chunk_bytes]
            self._flush_part(chunk)
        return len(view)

    # -- session mechanics ---------------------------------------------------
    def _url_for(self, part_number: int, checksum_b64: str) -> str:
        url = None if self._checksums_pinned else self._urls.pop(part_number, None)
        if url:
            return url
        response = _post(
            "/api/node/storage/part-urls",
            {
                "sessionId": self._session_id,
                "parts": [{"partNumber": part_number, "checksumSha256": checksum_b64}],
            },
        )
        for part in response["partUrls"]:
            if part["partNumber"] == part_number:
                return part["url"]
        raise StorageError(f"no URL issued for part {part_number}")

    def _flush_part(self, chunk: bytes) -> None:
        checksum = base64.b64encode(hashlib.sha256(chunk).digest()).decode("ascii")
        part_number = self._part_number
        url = self._url_for(part_number, checksum)
        try:
            _put_part(url, chunk, checksum, self._checksums_pinned)
        except StorageError as error:
            if "expired" not in str(error):
                raise
            url = self._url_for(part_number, checksum)
            _put_part(url, chunk, checksum, self._checksums_pinned)
        self._part_number += 1

    def abort(self) -> None:
        if self._aborted or self._committed is not None:
            return
        self._aborted = True
        try:
            _post("/api/node/storage/abort", {"sessionId": self._session_id}, retries=1)
        except StorageError:
            pass  # session TTL + lifecycle rule clean up server-side

    def commit(self) -> dict:
        if self._committed is not None:
            return self._committed
        if self._aborted:
            raise StorageError("upload was aborted")
        # Final (possibly sub-minimum-size, possibly empty-for-empty-file) part.
        if self._buffer or self._part_number == 1:
            self._flush_part(bytes(self._buffer))
            self._buffer.clear()
        self._committed = _post("/api/node/storage/complete", {"sessionId": self._session_id})
        return self._committed

    def close(self) -> None:
        if self.closed:
            return
        try:
            if not self._aborted and self._committed is None:
                if sys.exc_info()[0] is not None:
                    # close() reached while an exception is unwinding (a bare
                    # try/finally around the writer): the data is incomplete —
                    # abort instead of committing a truncated file.
                    self.abort()
                else:
                    self.commit()
        finally:
            super().close()

    def __del__(self) -> None:
        # Interpreter teardown / GC of a writer nobody closed. IOBase.__del__
        # would call close() — which must NOT commit here: reaching __del__
        # without an explicit commit/close means the node crashed or forgot,
        # and whatever was flushed so far is not a complete file. Abort
        # (best-effort; the server-side session TTL cleans up regardless).
        if self.closed:
            return
        if self._committed is None and not self._aborted:
            try:
                self.abort()
            except Exception:
                pass
        try:
            super().close()
        except Exception:
            pass

    # -- context manager -----------------------------------------------------
    def __enter__(self) -> "_SessionWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            self.abort()
            super().close()
        else:
            self.close()

    # -- results -------------------------------------------------------------
    @property
    def result(self) -> dict | None:
        """The server's commit response ({fileId, path, name, sizeBytes, ...})."""
        return self._committed


def open_write(path: str, *, chunk_bytes: int | None = None, content_type: str | None = None) -> _SessionWriter:
    """Open a storage path for writing through an upload session.

    `path` is relative to the caller's own prefix ("results/output.parquet").
    No upfront size declaration — quota is metered incrementally; hitting the
    wall raises StorageQuotaExceeded mid-stream.

    The part size is SERVER-chosen; leave `chunk_bytes` unset unless this
    node genuinely can't buffer the server's choice in memory.

    Committing (a clean close()/commit()) is MANDATORY: the file does not
    exist until the server's Complete gate returns — an uncommitted
    session's bytes are deleted server-side after the session expires
    (~1 day).

    When the size IS known upfront, prefer upload_file()/write_bytes() — the
    server then answers with a single PUT for small files instead of the
    multipart machinery.
    """
    return _SessionWriter(path, chunk_bytes, content_type)


_config_cache: dict | None = None


def _upload_config() -> dict:
    """Server-owned upload strategy (/storage/config), cached per process."""
    global _config_cache
    if _config_cache is None:
        _config_cache = _post("/api/node/storage/config", {})
    return _config_cache


def _sha256_b64(stream) -> str:
    """Whole-stream base64 sha256; rewinds the stream when done."""
    digest = hashlib.sha256()
    while True:
        block = stream.read(1024 * 1024)
        if not block:
            break
        digest.update(block)
    stream.seek(0)
    return base64.b64encode(digest.digest()).decode("ascii")


def _put_single(
    url: str, body: bytes, content_type: str, checksum_b64: str | None, *, retries: int = _MAX_RETRIES
) -> None:
    """One presigned PUT of the whole object. Content-Type and Content-Length
    are signed into the URL and must match exactly; the checksum header is
    sent only when the URL was checksum-pinned at create time."""
    last_error: Exception | None = None
    for attempt in range(retries):
        headers = {"Content-Type": content_type}
        if checksum_b64 is not None:
            headers["x-amz-checksum-sha256"] = checksum_b64
        request = urllib.request.Request(url, data=body, method="PUT", headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=300):
                return
        except urllib.error.HTTPError as error:
            snippet = _error_body(error)
            if 400 <= error.code < 500:
                _log(f"single-PUT upload rejected ({error.code}): {snippet}")
                raise StorageError(
                    f"single-PUT upload failed ({error.code}): {snippet}",
                    status=error.code,
                    detail=snippet,
                ) from error
            last_error = StorageError(
                f"single-PUT upload failed ({error.code}): {snippet}", status=error.code, detail=snippet
            )
            _log(f"single-PUT attempt {attempt + 1}/{retries} failed ({error.code}): {snippet}")
        except (urllib.error.URLError, TimeoutError) as error:
            last_error = StorageError(f"single-PUT upload failed: {error}")
            _log(f"single-PUT attempt {attempt + 1}/{retries} failed: {error}")
        time.sleep(_RETRY_DELAY_S * (2**attempt))
    raise last_error or StorageError("single-PUT upload failed")


def _upload_known_size(display_path: str, stream, size_bytes: int, content_type: str | None) -> dict:
    """Known-size upload through the unified /create endpoint: the SERVER
    answers "single" (one presigned PUT) or "multipart" (a session with a
    server-chosen part size); either way Complete commits it. `stream` must
    be seekable and positioned at 0."""
    content_type = content_type or "application/octet-stream"
    payload: dict = {
        "path": _normalize_write_path(display_path),
        "contentType": content_type,
        "sizeBytes": size_bytes,
    }
    # A whole-file checksum pins the single-PUT URL where the backend has
    # checksums on. Only worth the hashing pass when "single" is possible.
    try:
        single_max = int(_upload_config().get("singlePutMaxBytes", 0))
    except StorageError:
        single_max = 0  # config unavailable — the server still decides the kind
    checksum = _sha256_b64(stream) if size_bytes <= single_max else None
    if checksum is not None:
        payload["checksumSha256"] = checksum

    created = _post("/api/node/storage/create", payload)

    if created.get("kind") == "single":
        try:
            body = stream.read()
            if len(body) != size_bytes:
                raise StorageError(
                    f"{display_path}: size changed during upload (declared {size_bytes}, read {len(body)})"
                )
            pinned = bool(created.get("checksumsEnabled")) and checksum is not None
            _put_single(created["url"], body, content_type, checksum if pinned else None)
            return _post("/api/node/storage/complete", {"sessionId": created["sessionId"]})
        except BaseException:
            # Never leave a half-done session holding quota reservation.
            try:
                _post("/api/node/storage/abort", {"sessionId": created["sessionId"]}, retries=1)
            except StorageError:
                pass  # session TTL + lifecycle rule clean up server-side
            raise

    # "multipart" — stream through the standard session machinery (which
    # aborts on exception via its context manager).
    with _SessionWriter(display_path, None, content_type, _created=created) as writer:
        shutil.copyfileobj(stream, writer, length=_COPY_BLOCK)
    return writer.result


def upload_file(path: str, local_path: str, *, content_type: str | None = None) -> dict:
    """Upload a local file to `path` (relative to the caller's own prefix).
    The size is declared upfront, so the SERVER picks the strategy — one
    presigned PUT for small files, a multipart session otherwise. Returns the
    commit response ({fileId, path, name, sizeBytes, ...})."""
    size_bytes = os.path.getsize(local_path)
    with open(local_path, "rb") as stream:
        return _upload_known_size(path, stream, size_bytes, content_type)


def write_bytes(path: str, data: bytes, *, content_type: str | None = None) -> dict:
    """Upload an in-memory payload to `path` (see upload_file)."""
    data = bytes(data)
    return _upload_known_size(path, io.BytesIO(data), len(data), content_type)


def _normalize_read_path(path: str) -> str:
    """Accepts "s3://bucket/...", "{bucket}/..." or a caller-relative path
    ("{flow}/{exec}/{node}/file") and returns what the download-url endpoint
    expects."""
    if path.startswith("s3://"):
        return path
    cleaned = path.lstrip("/")
    first = cleaned.split("/")[0]
    if first in (os.environ.get("ARTIFACT_S3_BUCKET"), os.environ.get("INPUT_S3_BUCKET")):
        return cleaned
    return f"{_user_id()}/{cleaned}"


def _download_url(path: str) -> str:
    response = _post("/api/node/storage/download-url", {"path": _normalize_read_path(path)})
    return response["url"]


def read(path: str) -> bytes:
    """Read a storage object. `path` may be relative to the caller's prefix
    ("{flow}/{exec}/{node}/file"), or an absolute "s3://bucket/..." /
    "{bucket}/..." path (e.g. an input's declared path from NODE_CONTEXT).
    The backend resolves it to the file's CURRENT version and returns a fresh
    GET URL — which is how outputs written moments ago by an upstream node in
    this run are readable."""
    request = urllib.request.Request(_download_url(path))
    with urllib.request.urlopen(request, timeout=300) as response:
        return response.read()


def read_to_file(path: str, local_path: str) -> str:
    """Stream a storage object to `local_path` (see `read` for path forms)."""
    request = urllib.request.Request(_download_url(path))
    with urllib.request.urlopen(request, timeout=600) as response, open(local_path, "wb") as out:
        while True:
            block = response.read(1024 * 1024)
            if not block:
                break
            out.write(block)
    return local_path
