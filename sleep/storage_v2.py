"""Credentialless Storage v2 client for pipeline nodes (stdlib only)."""
from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import sys
import time
import urllib.error
import urllib.request

__all__ = ["open_write", "read", "read_to_file", "StorageError", "StorageQuotaExceeded"]

_MAX_RETRIES = 3
_RETRY_DELAY_S = 1.0


class StorageError(RuntimeError):
    """A storage operation failed."""


class StorageQuotaExceeded(StorageError):
    """The write exceeded the caller's storage quota."""


def _backend_url() -> str:
    url = os.environ.get("BACKEND_URL")
    if not url:
        raise StorageError("BACKEND_URL is not set — SDK storage is unavailable.")
    return url.rstrip("/")


def _api_key() -> str:
    key = os.environ.get("API_KEY")
    if not key:
        raise StorageError("API_KEY is not set — cannot authenticate to the storage API.")
    return key


def _user_id() -> str:
    if user := os.environ.get("PIPELINE_USER_ID"):
        return user
    try:
        ctx = json.loads(os.environ.get("NODE_CONTEXT", ""))
        for entry in ctx.get("output", {}).get("files", []):
            path = entry.get("path", "")
            if path.startswith("s3://"):
                return path[len("s3://"):].split("/")[1]
        for entry in ctx.get("inputs", []):
            path = entry.get("output", {}).get("path", "")
            if path.startswith("s3://"):
                return path[len("s3://"):].split("/")[1]
    except (ValueError, KeyError, IndexError):
        pass
    raise StorageError("Cannot determine the calling user (no PIPELINE_USER_ID or usable NODE_CONTEXT).")


def _post(endpoint: str, payload: dict, *, retries: int = _MAX_RETRIES) -> dict:
    body = json.dumps(payload).encode("utf-8")
    last_error: Exception | None = None
    for attempt in range(retries):
        request = urllib.request.Request(
            f"{_backend_url()}{endpoint}", data=body, method="POST",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {_api_key()}"},
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            try:
                detail = json.loads(error.read().decode("utf-8")).get("error", "")
            except Exception:
                detail = ""
            if error.code == 403 and "storage limit" in detail.lower():
                raise StorageQuotaExceeded(detail) from error
            if error.code == 410:
                raise StorageError(f"{endpoint} is gone — SDK/backend pair is mismatched: {detail}") from error
            if 400 <= error.code < 500:
                raise StorageError(f"{endpoint} failed ({error.code}): {detail}") from error
            last_error = StorageError(f"{endpoint} failed ({error.code}): {detail}")
        except (urllib.error.URLError, TimeoutError) as error:
            last_error = StorageError(f"{endpoint} unreachable: {error}")
        time.sleep(_RETRY_DELAY_S * 2**attempt)
    raise last_error or StorageError(f"{endpoint} failed")


def _put_part(url: str, data: bytes, checksum: str, pinned: bool) -> None:
    last_error: Exception | None = None
    for attempt in range(_MAX_RETRIES):
        headers = {"Content-Type": "application/octet-stream"}
        if pinned:
            headers["x-amz-checksum-sha256"] = checksum
        try:
            with urllib.request.urlopen(urllib.request.Request(url, data=data, method="PUT", headers=headers), timeout=300):
                return
        except urllib.error.HTTPError as error:
            if error.code == 403:
                raise StorageError("part URL expired") from error
            if 400 <= error.code < 500:
                raise StorageError(f"part upload failed ({error.code})") from error
            last_error = StorageError(f"part upload failed ({error.code})")
        except (urllib.error.URLError, TimeoutError) as error:
            last_error = StorageError(f"part upload failed: {error}")
        time.sleep(_RETRY_DELAY_S * 2**attempt)
    raise last_error or StorageError("part upload failed")


class _SessionWriter(io.RawIOBase):
    def __init__(self, path: str, chunk_bytes: int | None, content_type: str | None):
        super().__init__()
        user_id = _user_id()
        # NodeContext declares outputs as s3://bucket/{user}/…; accept that
        # assigned form while the backend still receives a scoped relative key.
        if path.startswith("s3://"):
            path = path[len("s3://"):].split("/", 1)[1]
        path = path.lstrip("/")
        if path.startswith(f"{user_id}/"):
            path = path[len(user_id) + 1:]
        payload = {"path": f"{user_id}/{path}", "contentType": content_type or "application/octet-stream", "firstPartCount": 1}
        if chunk_bytes is not None:
            payload["chunkBytes"] = chunk_bytes
        created = _post("/api/node/storage/create-session", payload)
        self._chunk_bytes = int(created["declaredChunkBytes"])
        self._session_id = int(created["sessionId"])
        self._pinned = bool(created.get("checksumsEnabled"))
        self._urls = {} if self._pinned else {p["partNumber"]: p["url"] for p in created.get("partUrls", [])}
        self._buffer = bytearray()
        self._part = 1
        self._committed: dict | None = None
        self._aborted = False

    def writable(self) -> bool:
        return True

    def write(self, data) -> int:  # type: ignore[override]
        if self._committed is not None or self._aborted:
            raise ValueError("write to a closed storage file")
        raw = bytes(data)
        self._buffer.extend(raw)
        while len(self._buffer) >= self._chunk_bytes:
            chunk = bytes(self._buffer[:self._chunk_bytes]); del self._buffer[:self._chunk_bytes]
            self._flush(chunk)
        return len(raw)

    def _url_for(self, checksum: str) -> str:
        if not self._pinned and (url := self._urls.pop(self._part, None)):
            return url
        response = _post("/api/node/storage/part-urls", {"sessionId": self._session_id, "parts": [{"partNumber": self._part, "checksumSha256": checksum}]})
        for part in response["partUrls"]:
            if part["partNumber"] == self._part:
                return part["url"]
        raise StorageError(f"no URL issued for part {self._part}")

    def _flush(self, chunk: bytes) -> None:
        checksum = base64.b64encode(hashlib.sha256(chunk).digest()).decode("ascii")
        try:
            _put_part(self._url_for(checksum), chunk, checksum, self._pinned)
        except StorageError as error:
            if "expired" not in str(error):
                raise
            _put_part(self._url_for(checksum), chunk, checksum, self._pinned)
        self._part += 1

    def abort(self) -> None:
        if self._aborted or self._committed is not None:
            return
        self._aborted = True
        try: _post("/api/node/storage/abort", {"sessionId": self._session_id}, retries=1)
        except StorageError: pass

    def commit(self) -> dict:
        if self._committed is not None: return self._committed
        if self._aborted: raise StorageError("upload was aborted")
        if self._buffer or self._part == 1:
            self._flush(bytes(self._buffer)); self._buffer.clear()
        self._committed = _post("/api/node/storage/complete", {"sessionId": self._session_id})
        return self._committed

    def close(self) -> None:
        if self.closed: return
        try:
            if not self._aborted and self._committed is None:
                self.abort() if sys.exc_info()[0] else self.commit()
        finally: super().close()

    def __del__(self) -> None:
        if not self.closed and self._committed is None: self.abort()

    def __enter__(self): return self
    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type: self.abort(); super().close()
        else: self.close()

    @property
    def result(self) -> dict | None: return self._committed


def open_write(path: str, *, chunk_bytes: int | None = None, content_type: str | None = None) -> _SessionWriter:
    return _SessionWriter(path, chunk_bytes, content_type)


def _normalize_read_path(path: str) -> str:
    if path.startswith("s3://"): return path
    cleaned = path.lstrip("/")
    if cleaned.split("/", 1)[0] in (os.environ.get("ARTIFACT_S3_BUCKET"), os.environ.get("INPUT_S3_BUCKET")):
        return cleaned
    return f"{_user_id()}/{cleaned}"


def _download_url(path: str) -> str:
    return _post("/api/node/storage/download-url", {"path": _normalize_read_path(path)})["url"]


def read(path: str) -> bytes:
    with urllib.request.urlopen(urllib.request.Request(_download_url(path)), timeout=300) as response: return response.read()


def read_to_file(path: str, local_path: str) -> str:
    with urllib.request.urlopen(urllib.request.Request(_download_url(path)), timeout=600) as response, open(local_path, "wb") as out:
        while block := response.read(1024 * 1024): out.write(block)
    return local_path
