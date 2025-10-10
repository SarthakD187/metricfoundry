from __future__ import annotations
import io, json, csv, gzip, zipfile, tarfile, sqlite3, contextlib, tempfile, mimetypes, base64, codecs
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple, Union, IO, Set, MutableMapping, cast
from ..core.types import DatasetSummary, ColumnAccumulator, RunningStats, BinaryInput
from ..core.utils import _open_binary_stream, _ensure_bytes, _open_archive_stream, _format_preview
from ..core.constants import (
    _DELIMITED_STREAM_CHUNK_SIZE, _ZIP_PREFERRED_EXTENSIONS, _NULL_SENTINELS,
    _MAX_PREVIEW_ROWS, _MAX_SAMPLE_VALUES, _MAX_DISTINCT_TRACK, _MAX_NUMERIC_ROW_SAMPLES, _MAX_AUTOML_ROWS
)

def _classify_value(value: Any) -> Tuple[str, Optional[Any], Optional[float]]:
    if value is None:
        return "null", None, None

    if isinstance(value, bool):
        return "boolean", value, None

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return "numeric", value, float(value)

    if isinstance(value, str):
        trimmed = value.strip()
        if not trimmed or trimmed in _NULL_SENTINELS:
            return "null", None, None
        lowered = trimmed.lower()
        if lowered in ("true", "false"):
            return "boolean", lowered == "true", None
        try:
            parsed = float(trimmed)
        except ValueError:
            return "text", trimmed, None
        return "numeric", parsed, float(parsed)

    return "text", value, None

class _DatasetBuilder:
    def __init__(self, bytes_read: int, source_format: str) -> None:
        self.bytes_read = bytes_read
        self.source_format = source_format
        self.columns: Dict[str, ColumnAccumulator] = {}
        self.preview_rows: List[Dict[str, Any]] = []
        self.numeric_row_samples: List[Dict[str, float]] = []
        self.training_rows: List[Dict[str, Any]] = []
        self.row_count = 0
        self._row_signature_counts: Dict[Tuple[Tuple[str, Any], ...], int] = {}
        self.duplicate_rows = 0

    def ensure_column(self, name: str) -> ColumnAccumulator:
        if name not in self.columns:
            self.columns[name] = ColumnAccumulator(name=name, null_count=self.row_count)
        return self.columns[name]

    def register_missing_columns(self, row_columns: Sequence[str]) -> None:
        known = set(self.columns.keys())
        missing = known.difference(row_columns)
        for name in missing:
            self.columns[name].register_value("null", None, None)

    def process_row(self, row: Mapping[str, Any]) -> None:
        row_columns = list(row.keys())
        self.register_missing_columns(row_columns)

        preview_row: Dict[str, Any] = {}
        numeric_row: Dict[str, float] = {}
        processed_row: Dict[str, Any] = {}

        for name in row_columns:
            col = self.ensure_column(name)
            value = row.get(name)
            kind, display_value, numeric_value = _classify_value(value)
            col.register_value(kind, display_value, numeric_value)

            if len(self.preview_rows) < _MAX_PREVIEW_ROWS:
                preview_row[name] = _format_preview(display_value)

            if kind == "numeric" and numeric_value is not None:
                numeric_row[name] = float(numeric_value)

            processed_row[name] = display_value if kind != "null" else None

        for col_name in self.columns.keys():
            processed_row.setdefault(col_name, None)

        signature = tuple((col_name, processed_row.get(col_name)) for col_name in sorted(self.columns.keys()))
        prior_count = self._row_signature_counts.get(signature, 0)
        if prior_count:
            self.duplicate_rows += 1
        self._row_signature_counts[signature] = prior_count + 1

        if len(self.preview_rows) < _MAX_PREVIEW_ROWS:
            # Include columns with no values in this row as empty strings for preview consistency.
            for col_name in self.columns.keys():
                preview_row.setdefault(col_name, "")
            self.preview_rows.append(preview_row)

        if numeric_row and len(self.numeric_row_samples) < _MAX_NUMERIC_ROW_SAMPLES:
            self.numeric_row_samples.append(numeric_row)

        if len(self.training_rows) < _MAX_AUTOML_ROWS:
            self.training_rows.append(dict(processed_row))

        self.row_count += 1

    def increment_bytes_read(self, amount: int) -> None:
        self.bytes_read += amount

    def build(self) -> DatasetSummary:
        return DatasetSummary(
            row_count=self.row_count,
            columns=self.columns,
            preview_rows=self.preview_rows,
            numeric_row_samples=self.numeric_row_samples,
            bytes_read=self.bytes_read,
            source_format=self.source_format,
            training_rows=self.training_rows,
            duplicate_row_count=self.duplicate_rows,
        )


class _HeaderNormalizer:
    """Normalizes and deduplicates column headers for delimited inputs."""

    def __init__(self) -> None:
        self._base_counts: Dict[str, int] = {}
        self._used: Set[str] = set()

    def _clean(self, raw: Any, index: int) -> str:
        text = "" if raw is None else str(raw)
        text = text.lstrip("\ufeff").strip()
        if not text:
            return f"column_{index + 1}"
        return text

    def _allocate(self, base: str) -> str:
        count = self._base_counts.get(base, 0)
        candidate = base if count == 0 else f"{base}_{count + 1}"
        while candidate in self._used:
            count += 1
            candidate = f"{base}_{count + 1}"
        self._base_counts[base] = count + 1
        self._used.add(candidate)
        return candidate

    def normalize(self, fieldnames: Sequence[Any]) -> List[str]:
        return [self._allocate(self._clean(name, index)) for index, name in enumerate(fieldnames)]

    def generate_default(self, index: int) -> str:
        return self._allocate(f"column_{index + 1}")


def _ingest_delimited(key: str, body: BinaryInput, delimiter: str, source_format: str) -> DatasetSummary:
    stream, should_close = _open_binary_stream(body)
    decoder = codecs.getincrementaldecoder("utf-8")("replace")
    builder = _DatasetBuilder(bytes_read=0, source_format=source_format)
    buffer = ""
    normalizer = _HeaderNormalizer()
    headers: List[str] = []

    def _iter_lines() -> Iterable[str]:
        nonlocal buffer
        while True:
            chunk = stream.read(_DELIMITED_STREAM_CHUNK_SIZE)
            if not chunk:
                break
            if isinstance(chunk, bytes):
                data = chunk
            elif isinstance(chunk, bytearray):
                data = bytes(chunk)
            else:
                raise TypeError(f"Delimited dataset chunk from {key} must be bytes-like")
            builder.increment_bytes_read(len(data))
            decoded = decoder.decode(data)
            if decoded:
                buffer += decoded
            while True:
                newline_index = buffer.find("\n")
                if newline_index == -1:
                    break
                line = buffer[: newline_index + 1]
                yield line
                buffer = buffer[newline_index + 1 :]
        remainder = decoder.decode(b"", final=True)
        if remainder:
            buffer += remainder
        if buffer:
            yield buffer

    try:
        raw_reader = csv.reader(_iter_lines(), delimiter=delimiter)
        try:
            first_row = next(raw_reader)
        except StopIteration:
            return builder.build()

        headers = normalizer.normalize(first_row)

        for raw_row in raw_reader:
            if not raw_row or all((cell.strip() == "" for cell in raw_row)):
                continue

            row = list(raw_row)
            if len(row) < len(headers):
                row.extend([""] * (len(headers) - len(row)))
            elif len(row) > len(headers):
                while len(headers) < len(row):
                    headers.append(normalizer.generate_default(len(headers)))

            record = {headers[index]: row[index] if index < len(row) else "" for index in range(len(headers))}
            builder.process_row(record)
        return builder.build()
    finally:
        if should_close:
            stream.close()


def _ingest_csv(key: str, body: BinaryInput) -> DatasetSummary:
    return _ingest_delimited(key, body, ",", "csv")


def _ingest_tsv(key: str, body: BinaryInput) -> DatasetSummary:
    return _ingest_delimited(key, body, "\t", "tsv")


def _ingest_json(body: bytes) -> DatasetSummary:
    data = json.loads(body.decode("utf-8", errors="replace"))
    records: Iterable[Mapping[str, Any]]
    if isinstance(data, list):
        records = [r for r in data if isinstance(r, Mapping)]
    elif isinstance(data, Mapping):
        records = [data]
    else:
        records = []
    builder = _DatasetBuilder(bytes_read=len(body), source_format="json")
    for record in records:
        builder.process_row(record)
    return builder.build()


def _ingest_jsonl(body: bytes) -> DatasetSummary:
    text = body.decode("utf-8", errors="replace")
    builder = _DatasetBuilder(bytes_read=len(body), source_format="jsonl")
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            obj = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, Mapping):
            builder.process_row(obj)
    return builder.build()


def _dataframe_to_dataset(frame: Any, source_format: str, bytes_read: int) -> DatasetSummary:
    try:
        import pandas as pd  # type: ignore
    except ImportError as exc:  # pragma: no cover - dependency issues are surfaced at runtime
        raise RuntimeError(
            f"{source_format} ingestion requires the pandas dependency"
        ) from exc

    if not hasattr(frame, "to_dict"):
        frame = pd.DataFrame(frame)

    sanitized = frame.copy()
    sanitized.columns = [str(col) for col in sanitized.columns]
    sanitized = sanitized.where(pd.notnull(sanitized), None)

    builder = _DatasetBuilder(bytes_read=bytes_read, source_format=source_format)
    for record in sanitized.to_dict(orient="records"):
        builder.process_row(record)
    return builder.build()


def _ingest_excel(body: bytes) -> DatasetSummary:
    try:
        import pandas as pd  # type: ignore
    except ImportError as exc:  # pragma: no cover - dependency issues are surfaced at runtime
        raise RuntimeError("Excel ingestion requires pandas with openpyxl installed") from exc

    with io.BytesIO(body) as stream:
        frames = pd.read_excel(stream, sheet_name=None, dtype=object)

    if isinstance(frames, dict):
        collected = []
        for sheet_name, frame in frames.items():
            if frame is None or frame.empty:
                continue
            sheet_frame = frame.copy()
            sheet_frame.insert(0, "__sheet__", str(sheet_name))
            collected.append(sheet_frame)
        if collected:
            combined = pd.concat(collected, ignore_index=True)
        else:
            combined = pd.DataFrame()
        target = combined
    else:
        target = frames

    return _dataframe_to_dataset(target, "excel", len(body))


def _ingest_parquet(body: bytes) -> DatasetSummary:
    try:
        import pandas as pd  # type: ignore
    except ImportError as exc:  # pragma: no cover - dependency issues are surfaced at runtime
        raise RuntimeError("Parquet ingestion requires pandas with pyarrow installed") from exc

    with io.BytesIO(body) as stream:
        frame = pd.read_parquet(stream)

    return _dataframe_to_dataset(frame, "parquet", len(body))


def _normalize_database_value(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    return value


def _ingest_sqlite(body: bytes) -> DatasetSummary:
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".sqlite")
    try:
        temp.write(body)
        temp.flush()
        temp.close()

        conn = sqlite3.connect(temp.name)
        try:
            cursor = conn.cursor()
            tables = [
                row[0]
                for row in cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') "
                    "AND name NOT LIKE 'sqlite_%' ORDER BY name"
                )
            ]

            builder = _DatasetBuilder(bytes_read=len(body), source_format="sqlite")
            if not tables:
                return builder.build()

            multi_table = len(tables) > 1
            for table in tables:
                quoted = table.replace("\"", "\"\"")
                cursor.execute(f'SELECT * FROM "{quoted}"')
                columns = [desc[0] if desc[0] else f"column_{index}" for index, desc in enumerate(cursor.description or [])]
                for values in cursor:
                    record = {
                        columns[i]: _normalize_database_value(values[i]) for i in range(len(columns))
                    }
                    if multi_table:
                        record["_table"] = table
                    builder.process_row(record)
            return builder.build()
        finally:
            conn.close()
    finally:
        try:
            os.unlink(temp.name)
        except FileNotFoundError:  # pragma: no cover - best effort cleanup
            pass


_ZIP_PREFERRED_EXTENSIONS = [
    ".csv",
    ".tsv",
    ".jsonl",
    ".ndjson",
    ".json",
    ".parquet",
    ".pq",
    ".pqt",
    ".parq",
    ".xlsx",
    ".xls",
    ".xlsm",
    ".sqlite",
    ".sqlite3",
    ".db",
]


def _zip_member_priority(name: str) -> Tuple[int, str]:
    lowered = name.lower()
    for index, ext in enumerate(_ZIP_PREFERRED_EXTENSIONS):
        if lowered.endswith(ext):
            return index, lowered
    return len(_ZIP_PREFERRED_EXTENSIONS), lowered


def _ingest_zip(key: str, body: BinaryInput) -> DatasetSummary:
    try:
        with _open_archive_stream(body) as archive_stream:
            with zipfile.ZipFile(archive_stream) as archive:
                members = [info for info in archive.infolist() if not info.is_dir()]
                members.sort(key=lambda info: _zip_member_priority(info.filename))
                for info in members:
                    with archive.open(info, "r") as handle:
                        try:
                            return ingest_dataset(info.filename, handle)
                        except ValueError:
                            continue
    except zipfile.BadZipFile as exc:
        raise ValueError(f"Invalid ZIP archive: {key}") from exc
    raise ValueError(f"ZIP archive does not contain a supported dataset: {key}")


def _ingest_tar(key: str, body: BinaryInput) -> DatasetSummary:
    try:
        with _open_archive_stream(body) as archive_stream:
            with tarfile.open(fileobj=archive_stream, mode="r:*") as archive:
                members = [member for member in archive.getmembers() if member.isfile()]
                members.sort(key=lambda member: _zip_member_priority(member.name))
                for member in members:
                    handle = archive.extractfile(member)
                    if handle is None:
                        continue
                    with contextlib.closing(handle) as extracted:
                        try:
                            return ingest_dataset(member.name, extracted)
                        except ValueError:
                            continue
    except tarfile.TarError as exc:
        raise ValueError(f"Invalid TAR archive: {key}") from exc
    raise ValueError(f"TAR archive does not contain a supported dataset: {key}")


def _strip_compression_suffix(name: str) -> str:
    lowered = name.lower()
    if lowered.endswith(".gzip"):
        return name[: -len(".gzip")]
    if lowered.endswith(".gz"):
        return name[: -len(".gz")]
    return name


def ingest_dataset(key: str, body: BinaryInput) -> DatasetSummary:
    lowered = key.lower()
    if lowered.endswith(".zip"):
        return _ingest_zip(key, body)
    if lowered.endswith(".tar") or lowered.endswith(".tar.gz") or lowered.endswith(".tgz"):
        return _ingest_tar(key, body)
    if lowered.endswith(".gz") or lowered.endswith(".gzip"):
        stream, should_close = _open_binary_stream(body)
        try:
            try:
                with gzip.GzipFile(fileobj=stream) as decompressed:
                    return ingest_dataset(_strip_compression_suffix(key), decompressed)
            except OSError as exc:
                raise ValueError(f"Invalid GZIP payload: {key}") from exc
        finally:
            if should_close:
                stream.close()
    if lowered.endswith(".parquet") or lowered.endswith(".pq") or lowered.endswith(".pqt") or lowered.endswith(".parq"):
        return _ingest_parquet(_ensure_bytes(body))
    if lowered.endswith(".xlsx") or lowered.endswith(".xls") or lowered.endswith(".xlsm"):
        return _ingest_excel(_ensure_bytes(body))
    if lowered.endswith(".sqlite") or lowered.endswith(".sqlite3") or lowered.endswith(".db"):
        return _ingest_sqlite(_ensure_bytes(body))
    if lowered.endswith(".tsv") or lowered.endswith(".tab"):
        return _ingest_tsv(key, body)
    if lowered.endswith(".jsonl") or lowered.endswith(".ndjson"):
        return _ingest_jsonl(_ensure_bytes(body))
    if lowered.endswith(".json"):
        return _ingest_json(_ensure_bytes(body))
    return _ingest_csv(key, body)

