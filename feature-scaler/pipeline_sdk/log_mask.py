"""Mask secret values in everything a node prints (stdlib-only).

`env.get_secret()` registers each secret it returns; from then on, anything the
process writes to stdout or stderr has that value replaced with `[REDACTED]`
before it reaches the pod's log (and so the run view and the archived copy).

    from pipeline_sdk import env

    key = env.get_secret("OPENAI_API_KEY")
    print(f"using {key}")          # logs: using [REDACTED]

How it works: `install()` replaces `sys.stdout` / `sys.stderr` with a wrapper
that buffers text and releases it only once it cannot be the start of a secret,
so a value split across several `write` calls is still caught. Until a secret
is registered the wrapper passes text straight through. `install()` runs when
this module is imported, which is before a node configures `logging`, so
handlers created afterwards write through the wrapper too.

Limits (best effort, not a guarantee):
- Only text written through this Python process's `sys.stdout` / `sys.stderr`.
  Subprocess output, C-level writes to fd 1/2 and `sys.stdout.buffer` bypass it.
- Matches the value as is and in a few common encodings (JSON/`repr`-escaped,
  base64, URL-quoted). Any other transformation is not recognised.
- Values shorter than MIN_SECRET_LENGTH are not masked: replacing a 1-3
  character string would mangle ordinary log text.
- It keeps a secret out of the logs; it does not stop a node writing the value
  to an output file or sending it elsewhere.
"""

from __future__ import annotations

import atexit
import base64
import json
import sys
import threading
from typing import Any
from urllib.parse import quote

__all__ = ["MIN_SECRET_LENGTH", "REDACTED", "install", "mask", "register"]

REDACTED = "[REDACTED]"
MIN_SECRET_LENGTH = 4

_lock = threading.RLock()
# Every string to hide, longest first so a long value wins over a shorter one
# it contains.
_needles: list[str] = []


def _variants(value: str) -> set[str]:
    raw = value.encode("utf-8")
    forms = {
        value,
        json.dumps(value)[1:-1],
        repr(value)[1:-1],
        quote(value, safe=""),
    }
    for encoded in (base64.b64encode(raw), base64.urlsafe_b64encode(raw)):
        text = encoded.decode("ascii")
        forms.add(text)
        forms.add(text.rstrip("="))
    return {form for form in forms if len(form) >= MIN_SECRET_LENGTH}


def register(value: Any) -> None:
    """Hide `value` (and its common encodings) from this process's output."""
    if not isinstance(value, str) or len(value) < MIN_SECRET_LENGTH:
        return
    with _lock:
        merged = set(_needles) | _variants(value)
        _needles[:] = sorted(merged, key=len, reverse=True)


def mask(text: str) -> str:
    """`text` with every registered value replaced by `[REDACTED]`."""
    with _lock:
        needles = list(_needles)
    for needle in needles:
        text = text.replace(needle, REDACTED)
    return text


def _reset() -> None:
    """Forget every registered value (tests only)."""
    with _lock:
        _needles.clear()


def _could_start_secret(tail: str) -> bool:
    """True if `tail` is a proper prefix of some registered value."""
    return any(len(needle) > len(tail) and needle.startswith(tail) for needle in _needles)


class MaskingStream:
    """A text stream that releases text only once it cannot begin a secret."""

    def __init__(self, stream: Any) -> None:
        self._stream = stream
        self._pending = ""
        self._stream_lock = threading.RLock()

    # -- writing -----------------------------------------------------------
    def write(self, text: Any) -> int:
        if not isinstance(text, str):
            return self._stream.write(text)
        with self._stream_lock:
            if not _needles and not self._pending:
                return self._stream.write(text)
            self._pending += text
            self._drain(final=False)
        return len(text)

    def writelines(self, lines: Any) -> None:
        for line in lines:
            self.write(line)

    def flush(self) -> None:
        with self._stream_lock:
            self._drain(final=False)
        self._stream.flush()

    def close(self) -> None:
        with self._stream_lock:
            self._drain(final=True)
        self._stream.close()

    def flush_all(self) -> None:
        """Release everything still held back (used at interpreter exit)."""
        with self._stream_lock:
            self._drain(final=True)
        try:
            self._stream.flush()
        except (ValueError, OSError):
            pass

    def _drain(self, final: bool) -> None:
        text = self._pending
        with _lock:
            needles = list(_needles)
        longest = max((len(n) for n in needles), default=0)
        out: list[str] = []
        i = 0
        while i < len(text):
            hit = next((n for n in needles if text.startswith(n, i)), None)
            if hit is not None:
                out.append(REDACTED)
                i += len(hit)
                continue
            rest = text[i:]
            if not final and len(rest) < longest and _could_start_secret(rest):
                break
            out.append(text[i])
            i += 1
        if out:
            self._stream.write("".join(out))
        self._pending = text[i:]

    # -- everything else behaves like the wrapped stream --------------------
    def __getattr__(self, name: str) -> Any:
        return getattr(self._stream, name)


_installed = False


def _flush_streams_at_exit() -> None:
    for stream in (sys.stdout, sys.stderr):
        if isinstance(stream, MaskingStream):
            stream.flush_all()


def install() -> None:
    """Wrap sys.stdout and sys.stderr (idempotent)."""
    global _installed
    with _lock:
        if _installed:
            return
        if sys.stdout is not None and not isinstance(sys.stdout, MaskingStream):
            sys.stdout = MaskingStream(sys.stdout)
        if sys.stderr is not None and not isinstance(sys.stderr, MaskingStream):
            sys.stderr = MaskingStream(sys.stderr)
        atexit.register(_flush_streams_at_exit)
        _installed = True


install()
