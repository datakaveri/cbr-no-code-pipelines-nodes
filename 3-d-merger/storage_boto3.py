"""Small compatibility adapter for legacy node code during Storage v2 migration.

It deliberately implements only the object methods used by this repository.
No S3 request is ever made: all bytes go through :mod:`storage_v2`.
"""
from __future__ import annotations

import io
import os
from pathlib import Path

from storage_v2 import open_write, read, read_to_file

# Legacy constructors still inspect these settings before invoking client().
# They are inert placeholders, not storage credentials; this adapter ignores them.
for _prefix in ("INPUT", "ARTIFACT"):
    os.environ.setdefault(f"{_prefix}_S3_ENDPOINT", "storage-v2")
    os.environ.setdefault(f"{_prefix}_S3_ACCESS_KEY", "backend-mediated")
    os.environ.setdefault(f"{_prefix}_S3_SECRET_KEY", "backend-mediated")


def _path(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


class _Client:
    def get_object(self, *, Bucket: str, Key: str, **_: object) -> dict:
        return {"Body": io.BytesIO(read(_path(Bucket, Key)))}

    def put_object(self, *, Bucket: str, Key: str, Body, **_: object) -> None:
        with open_write(_path(Bucket, Key)) as out:
            while chunk := Body.read(1024 * 1024) if hasattr(Body, "read") else b"":
                out.write(chunk)
            if not hasattr(Body, "read"):
                out.write(Body)

    def download_file(self, bucket: str, key: str, filename: str, **_: object) -> None:
        read_to_file(_path(bucket, key), filename)

    def upload_file(self, filename: str, bucket: str, key: str, **_: object) -> None:
        with open(filename, "rb") as source, open_write(_path(bucket, key)) as out:
            while chunk := source.read(1024 * 1024):
                out.write(chunk)

    def copy_object(self, *, Bucket: str, Key: str, CopySource, **_: object) -> None:
        source_bucket = CopySource["Bucket"] if isinstance(CopySource, dict) else CopySource.split("/", 1)[0]
        source_key = CopySource["Key"] if isinstance(CopySource, dict) else CopySource.split("/", 1)[1]
        self.put_object(Bucket=Bucket, Key=Key, Body=read(_path(source_bucket, source_key)))


def client(*_: object, **__: object) -> _Client:
    return _Client()
