"""A boto3-shaped client that goes through `pipeline_sdk.storage` (stdlib-only).

For node code that already talks to storage through `boto3.client("s3", ...)`:
import this module instead of boto3 and the same calls work, without the node
holding a storage credential.

    from pipeline_sdk import boto3_compat as boto3

    client = boto3.client("s3", **env.s3_for_path(path).boto3_kwargs())
    client.download_file(bucket, key, "/tmp/in.parquet")

It implements only the object methods nodes use (get_object, put_object,
download_file, upload_file, copy_object). No S3 request is ever made: reads
resolve a fresh presigned URL through the backend and writes are upload
sessions. Whatever arguments are passed to `client()` are ignored. New code
should call `pipeline_sdk.storage` directly.
"""

from __future__ import annotations

import io

from . import env, storage

__all__ = ["client"]

# Legacy constructors still inspect the S3 settings before invoking client().
env.use_placeholder_s3_credentials()


def _path(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


class _Client:
    def get_object(self, *, Bucket: str, Key: str, **_: object) -> dict:
        return {"Body": io.BytesIO(storage.read(_path(Bucket, Key)))}

    def put_object(self, *, Bucket: str, Key: str, Body, **_: object) -> None:
        with storage.open_write(_path(Bucket, Key)) as out:
            if hasattr(Body, "read"):
                while chunk := Body.read(1024 * 1024):
                    out.write(chunk)
            else:
                out.write(Body)

    def download_file(self, bucket: str, key: str, filename: str, **_: object) -> None:
        storage.read_to_file(_path(bucket, key), filename)

    def upload_file(self, filename: str, bucket: str, key: str, **_: object) -> None:
        with open(filename, "rb") as source, storage.open_write(_path(bucket, key)) as out:
            while chunk := source.read(1024 * 1024):
                out.write(chunk)

    def copy_object(self, *, Bucket: str, Key: str, CopySource, **_: object) -> None:
        if isinstance(CopySource, dict):
            source_bucket, source_key = CopySource["Bucket"], CopySource["Key"]
        else:
            source_bucket, source_key = CopySource.split("/", 1)
        self.put_object(Bucket=Bucket, Key=Key, Body=storage.read(_path(source_bucket, source_key)))


def client(*_: object, **__: object) -> _Client:
    return _Client()
