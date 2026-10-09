"""Ergonomic NODE_CONTEXT access for custom pipeline nodes (stdlib-only).

Wraps the raw runtime contract (skills/udf/references/runtime-contract.md) so
node code never dict-dives into NODE_CONTEXT, derives storage keys by hand, or
re-implements the download/upload/status boilerplate:

    from pipeline_sdk import context

    def handler(ctx, workdir):
        src = ctx.input().download(workdir)       # presigned first, backend fallback
        result = process(src, ctx.config)
        ctx.output("result").upload_file(result)  # upload session to the declared path
        return {"rowCount": 123}                  # extra fields for the final status JSON

    context.run(handler)  # parses NODE_CONTEXT, workdir, status JSON, exit code

Reads and writes delegate to pipeline_sdk.storage — the node still holds no
writable storage credential.
"""

from __future__ import annotations

import json
import mimetypes
import os
import shutil
import sys
import tempfile
import traceback
import urllib.request

from . import env, storage

__all__ = [
    "ContextError",
    "Input",
    "NodeContext",
    "Output",
    "load",
    "log",
    "log_error",
    "run",
]

_log_prefix = "[NODE]"


def log(message: str) -> None:
    print(f"{_log_prefix} {message}", flush=True)


def log_error(message: str) -> None:
    print(f"{_log_prefix} ERROR: {message}", file=sys.stderr, flush=True)


class ContextError(RuntimeError):
    """NODE_CONTEXT is missing/malformed, or a lookup into it failed."""


class Input:
    """One upstream input (an entry of NODE_CONTEXT.inputs)."""

    def __init__(self, raw: dict):
        self.raw = raw
        output = raw.get("output", {})
        self.node_slug: str | None = raw.get("nodeSlug")
        self.node_name: str | None = raw.get("nodeName")
        self.name: str | None = output.get("name")
        self.path: str = output.get("path", "")
        self.format: str | None = output.get("format")
        self.presigned_url: str | None = output.get("presignedUrl")

    def download(self, dest: str) -> str:
        """Download to `dest` (a directory, or a full file path) and return the
        local path. Uses the platform-resolved presigned GET URL when present;
        if it has expired (long-running node), falls back to a fresh URL from
        the backend via pipeline_sdk.storage."""
        if os.path.isdir(dest):
            local_path = os.path.join(dest, os.path.basename(self.path))
        else:
            local_path = dest
        if self.presigned_url:
            try:
                with urllib.request.urlopen(self.presigned_url, timeout=600) as response, open(
                    local_path, "wb"
                ) as out:
                    shutil.copyfileobj(response, out)
                return local_path
            except Exception as exc:  # expired URL, network blip — re-resolve fresh
                log(f"Presigned URL for {self.path} failed ({exc}); fetching a fresh one")
        return storage.read_to_file(self.path, local_path)

    def read(self) -> bytes:
        """Read the whole input into memory (see download() for URL handling)."""
        if self.presigned_url:
            try:
                with urllib.request.urlopen(self.presigned_url, timeout=600) as response:
                    return response.read()
            except Exception as exc:
                log(f"Presigned URL for {self.path} failed ({exc}); fetching a fresh one")
        return storage.read(self.path)


class Output:
    """One declared output file (an entry of NODE_CONTEXT.output.files)."""

    def __init__(self, raw: dict):
        self.raw = raw
        self.name: str | None = raw.get("name")
        self.path: str = raw["path"]
        self.format: str | None = raw.get("format")

    @property
    def key(self) -> str:
        """The storage key relative to the caller's own prefix — what
        storage.open_write expects: "s3://bucket/{userId}/rest" -> "rest"."""
        if not self.path.startswith("s3://"):
            return self.path.lstrip("/")
        parts = self.path[len("s3://") :].split("/", 2)
        if len(parts) < 3 or not parts[2]:
            raise ContextError(f"output path {self.path!r} has no key after bucket/user")
        return parts[2]

    def _content_type(self, explicit: str | None) -> str | None:
        return explicit or mimetypes.guess_type(self.path)[0]

    def open(self, *, chunk_bytes: int | None = None, content_type: str | None = None):
        """Open this output for writing through an upload session (see
        storage.open_write — a clean close commits, an exception aborts)."""
        return storage.open_write(
            self.key, chunk_bytes=chunk_bytes, content_type=self._content_type(content_type)
        )

    def upload_file(self, local_path: str, *, content_type: str | None = None) -> dict:
        """Upload a local file to this output's declared path — the size is
        known, so the server sizes the multipart session for the whole file.
        Returns the commit response ({fileId, path, name, sizeBytes, ...})."""
        return storage.upload_file(
            self.key, local_path, content_type=self._content_type(content_type)
        )

    def write_bytes(self, data: bytes, *, content_type: str | None = None) -> dict:
        return storage.write_bytes(self.key, data, content_type=self._content_type(content_type))

    def write_text(self, text: str, *, content_type: str | None = None) -> dict:
        return self.write_bytes(text.encode("utf-8"), content_type=content_type)


class NodeContext:
    """Parsed NODE_CONTEXT. `raw` keeps the original dict as an escape hatch."""

    def __init__(self, raw: dict):
        self.raw = raw
        node = raw.get("node", {})
        self.name: str | None = node.get("name")
        self.slug: str | None = node.get("slug")
        self.config: dict = raw.get("config") or {}
        self.base_path: str | None = raw.get("output", {}).get("basePath")
        self.inputs: list[Input] = [Input(i) for i in raw.get("inputs", [])]
        self.outputs: list[Output] = [Output(f) for f in raw.get("output", {}).get("files", [])]

    def input(self, node_name: str | None = None) -> Input:
        """The sole input, or the one from the upstream node named `node_name`
        (the user-entered name shown in the editor)."""
        if node_name is None:
            if len(self.inputs) != 1:
                raise ContextError(
                    f"input() without a name needs exactly one input; node has {len(self.inputs)}"
                )
            return self.inputs[0]
        for inp in self.inputs:
            if inp.node_name == node_name:
                return inp
        raise ContextError(
            f"no input from upstream node {node_name!r} "
            f"(have: {[i.node_name for i in self.inputs]})"
        )

    def output(self, name: str | None = None) -> Output:
        """The sole declared output, or the one named `name`."""
        if name is None:
            if len(self.outputs) != 1:
                raise ContextError(
                    f"output() without a name needs exactly one output; node has {len(self.outputs)}"
                )
            return self.outputs[0]
        for out in self.outputs:
            if out.name == name:
                return out
        raise ContextError(
            f"no declared output named {name!r} (have: {[o.name for o in self.outputs]})"
        )


def load() -> NodeContext:
    """Parse NODE_CONTEXT from the environment; fail fast when absent."""
    try:
        raw = env.node_context_raw()
    except env.EnvError as exc:
        raise ContextError(str(exc)) from exc
    try:
        parsed = json.loads(raw)
    except ValueError as exc:
        raise ContextError(f"NODE_CONTEXT is not valid JSON: {exc}") from exc
    return NodeContext(parsed)


def run(handler, *, prefix: str | None = None) -> None:
    """Run `handler(ctx, workdir)` with the container obligations handled:
    NODE_CONTEXT parsing (exit 1 with a JSON error status when missing), a
    temp workdir (cleaned up afterwards), the final JSON status on stdout, and
    the exit code — the only signal the platform uses to mark the step.

    `handler` may return a dict of extra fields to merge into the success
    status (e.g. {"rowCount": 123}), or a list to replace its "outputs".
    `prefix` sets the log prefix for log()/log_error() (default: the node name).
    """
    global _log_prefix
    if prefix:
        _log_prefix = prefix
    try:
        ctx = load()
    except ContextError as exc:
        log_error(str(exc))
        print(json.dumps({"success": False, "error": str(exc)}))
        sys.exit(1)
    if not prefix and ctx.name:
        _log_prefix = f"[{ctx.name}]"
    workdir = tempfile.mkdtemp(prefix="node_")
    try:
        extra = handler(ctx, workdir)
        status = {
            "success": True,
            "nodeName": ctx.name,
            "outputs": [{"name": o.name, "path": o.path} for o in ctx.outputs],
        }
        if isinstance(extra, list):
            status["outputs"] = extra
        elif isinstance(extra, dict):
            status.update(extra)
        print(json.dumps(status))
    except Exception as exc:  # final status must reach stdout, exit code must signal failure
        log_error(str(exc))
        traceback.print_exc()
        print(json.dumps({"success": False, "nodeName": ctx.name, "error": str(exc)}))
        sys.exit(1)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
