"""Regression tests for streaming ingestion of large archive uploads."""

import base64
import gzip
import io
import os
import sys
import tarfile
import zipfile

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from tests.integration.test_graph_pipeline import _ensure_state_graph_stub

_ensure_state_graph_stub()

from services.workers.graph import graph


class ChunkTrackingStream(io.RawIOBase):
    """Binary stream wrapper that records read behaviour."""

    def __init__(self, data: bytes, max_request: int) -> None:
        super().__init__()
        self._buffer = memoryview(data)
        self._position = 0
        self._max_request = max_request
        self.requests: list[int] = []
        self.chunks: list[int] = []
        self.total_read = 0
        self.max_request_observed = 0

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            request = self._max_request
        else:
            request = size
        self.max_request_observed = max(self.max_request_observed, request)
        self.requests.append(request)
        if self._position >= len(self._buffer):
            self.chunks.append(0)
            return b""
        end = min(len(self._buffer), self._position + request)
        chunk = self._buffer[self._position : end].tobytes()
        self._position = end
        self.chunks.append(len(chunk))
        self.total_read += len(chunk)
        return chunk

    def readable(self) -> bool:  # pragma: no cover - io interface contract
        return True


def _generate_large_csv_bytes(minimum_size: int) -> tuple[bytes, int]:
    header = "id,value\n"
    rows: list[str] = []
    total_length = len(header)
    value_length = 60_000  # keep each field below the csv module's 128 KiB limit
    row_index = 0

    while total_length < minimum_size:
        raw = os.urandom(((value_length + 3) // 4 + 1) * 3)
        value = base64.b64encode(raw).decode("ascii")[:value_length]
        row = f"{row_index},{value}\n"
        rows.append(row)
        total_length += len(row)
        row_index += 1

    return (header + "".join(rows)).encode("utf-8"), row_index


def test_zip_ingestion_streams_large_archives() -> None:
    csv_bytes, expected_rows = _generate_large_csv_bytes(
        graph._ARCHIVE_STREAM_CHUNK_SIZE * 2 + 4096
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("large.csv", csv_bytes)
    zip_bytes = buffer.getvalue()
    assert len(zip_bytes) > graph._ARCHIVE_STREAM_CHUNK_SIZE

    stream = ChunkTrackingStream(zip_bytes, graph._ARCHIVE_STREAM_CHUNK_SIZE)
    summary = graph.ingest_dataset("large.zip", stream)

    assert summary.row_count == expected_rows
    assert summary.column_names == ["id", "value"]
    assert stream.total_read == len(zip_bytes)
    assert stream.max_request_observed <= graph._ARCHIVE_STREAM_CHUNK_SIZE
    assert any(size == graph._ARCHIVE_STREAM_CHUNK_SIZE for size in stream.chunks if size)


def test_tar_gz_ingestion_streams_large_archives() -> None:
    csv_bytes, expected_rows = _generate_large_csv_bytes(
        graph._ARCHIVE_STREAM_CHUNK_SIZE * 2 + 4096
    )
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        tarinfo = tarfile.TarInfo(name="large.csv")
        tarinfo.size = len(csv_bytes)
        archive.addfile(tarinfo, io.BytesIO(csv_bytes))
    tar_bytes = buffer.getvalue()
    assert len(tar_bytes) > graph._ARCHIVE_STREAM_CHUNK_SIZE

    stream = ChunkTrackingStream(tar_bytes, graph._ARCHIVE_STREAM_CHUNK_SIZE)
    summary = graph.ingest_dataset("large.tar.gz", stream)

    assert summary.row_count == expected_rows
    assert summary.column_names == ["id", "value"]
    assert stream.total_read == len(tar_bytes)
    assert stream.max_request_observed <= graph._ARCHIVE_STREAM_CHUNK_SIZE
    assert any(size == graph._ARCHIVE_STREAM_CHUNK_SIZE for size in stream.chunks if size)


def test_gzip_ingestion_streams_large_archives() -> None:
    csv_bytes, expected_rows = _generate_large_csv_bytes(
        graph._ARCHIVE_STREAM_CHUNK_SIZE + 2048
    )
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz_stream:
        gz_stream.write(csv_bytes)
    gzip_bytes = buffer.getvalue()

    stream = ChunkTrackingStream(gzip_bytes, graph._ARCHIVE_STREAM_CHUNK_SIZE)
    summary = graph.ingest_dataset("large.csv.gz", stream)

    assert summary.row_count == expected_rows
    assert summary.column_names == ["id", "value"]
    assert stream.total_read == len(gzip_bytes)
    assert stream.max_request_observed <= graph._ARCHIVE_STREAM_CHUNK_SIZE
