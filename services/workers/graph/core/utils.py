from __future__ import annotations
import io, gzip, codecs, contextlib, tempfile, math, mimetypes, html, logging, zipfile, tarfile, base64, os, sqlite3
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple, Union, IO, Set, MutableMapping, cast
from .types import BinaryInput
from .constants import _ARCHIVE_STREAM_CHUNK_SIZE

logger = logging.getLogger(__name__)

# Jinja2 env bootstrap (copy the conditional import + _JINJA_ENV creation)
try:
    from jinja2 import Environment, FileSystemLoader, select_autoescape
except ModuleNotFoundError:
    Environment = None  # type: ignore
    FileSystemLoader = None  # type: ignore
    select_autoescape = None  # type: ignore
_JINJA_ENV = None
if Environment and FileSystemLoader and select_autoescape:
    from .constants import _TEMPLATE_DIR
    _JINJA_ENV = Environment(loader=FileSystemLoader(str(_TEMPLATE_DIR)), autoescape=select_autoescape(["html","xml"]))

_MATPLOTLIB_SETUP = False
_PYLAB: Any = None


def _get_pyplot():
    global _MATPLOTLIB_SETUP, _PYLAB
    if _PYLAB is not None:
        return _PYLAB
    try:
        import matplotlib  # type: ignore

        if not _MATPLOTLIB_SETUP:
            matplotlib.use("Agg")
            _MATPLOTLIB_SETUP = True
        import matplotlib.pyplot as plt  # type: ignore
    except ImportError:
        return None

    _PYLAB = plt
    return plt

def _open_binary_stream(body: BinaryInput) -> Tuple[IO[bytes], bool]:
    if isinstance(body, bytes):
        return io.BytesIO(body), True
    if isinstance(body, bytearray):
        return io.BytesIO(bytes(body)), True
    if hasattr(body, "read"):
        return cast(IO[bytes], body), False
    raise TypeError("body must be bytes-like or a binary stream")


def _ensure_bytes(body: BinaryInput) -> bytes:
    if isinstance(body, bytes):
        return body
    if isinstance(body, bytearray):
        return bytes(body)
    data = cast(IO[bytes], body).read()
    return data


@contextlib.contextmanager
def _open_archive_stream(body: BinaryInput) -> Iterator[IO[bytes]]:
    stream, should_close = _open_binary_stream(body)
    temp_file: Optional[tempfile.NamedTemporaryFile] = None
    try:
        try:
            stream.seek(0)
        except (AttributeError, OSError, io.UnsupportedOperation):
            temp_file = tempfile.NamedTemporaryFile(
                prefix="metricfoundry-archive-", suffix=".tmp"
            )
            while True:
                chunk = stream.read(_ARCHIVE_STREAM_CHUNK_SIZE)
                if not chunk:
                    break
                temp_file.write(chunk)
            temp_file.flush()
            temp_file.seek(0)
            if should_close:
                stream.close()
                should_close = False
            stream = temp_file
        else:
            stream.seek(0)
        yield stream
    finally:
        if should_close:
            stream.close()
        if temp_file is not None:
            temp_file.close()

def _format_preview(value: Any) -> Any:
    if value is None:
        return None
    text = str(value)
    if len(text) > 80:
        return text[:77] + "..."
    return text

def _sanitize_filename(name: str) -> str:
    sanitized = [
        ch if ch.isalnum() or ch in {"-", "_"} else "_"
        for ch in name
    ]
    collapsed = "".join(sanitized).strip("_")
    return collapsed or "column"

def _guess_content_type_for_artifact(relative_key: str, spec: Mapping[str, Any]) -> str:
    content_type = spec.get("contentType")
    if isinstance(content_type, str) and content_type:
        return content_type

    kind = spec.get("kind")
    if kind == "html":
        return "text/html"
    if kind == "text":
        return "text/plain"
    if kind == "json":
        return "application/json"
    if kind == "csv":
        return "text/csv"
    if kind == "image":
        return "image/png"

    guessed, _ = mimetypes.guess_type(relative_key)
    if guessed:
        return guessed

    return "application/octet-stream"

def _is_visualization_artifact(relative_key: str, spec: Mapping[str, Any]) -> bool:
    if relative_key.startswith("results/graphs/"):
        return True

    bundle_hint = spec.get("bundle")
    if isinstance(bundle_hint, str) and bundle_hint.lower() == "visualization":
        return True

    content_type = spec.get("contentType")
    if isinstance(content_type, str) and content_type.lower().startswith("image/"):
        return True

    kind = spec.get("kind")
    if isinstance(kind, str) and kind.lower() == "image":
        return True

    return False
