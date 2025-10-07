"""LangGraph analytics pipeline for MetricFoundry."""
from __future__ import annotations

import base64
import csv
import gzip
import io
import json
import math
import os
import sqlite3
import tarfile
import tempfile
import zipfile
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from langgraph.graph import END, StateGraph

PHASE_ORDER = [
    "ingest",
    "profile",
    "dq_validate",
    "descriptive_stats",
    "ml_inference",
    "nl_report",
    "finalize",
]

PhaseCallback = Callable[[str, Mapping[str, Any], int, int], None]

_NULL_SENTINELS = {"", "null", "NULL", "NaN", "nan"}
_MAX_PREVIEW_ROWS = 5
_MAX_SAMPLE_VALUES = 5
_MAX_DISTINCT_TRACK = 25
_MAX_NUMERIC_ROW_SAMPLES = 500
_MAX_AUTOML_ROWS = 2000
_MAX_HISTOGRAMS = 8
_MAX_SCATTERS = 3


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


@dataclass
class RunningStats:
    """Numerically stable streaming statistics tracker."""

    count: int = 0
    mean: float = 0.0
    m2: float = 0.0
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    samples: List[float] = field(default_factory=list)

    def update(self, value: float) -> None:
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.m2 += delta * delta2
        self.min_value = value if self.min_value is None else min(self.min_value, value)
        self.max_value = value if self.max_value is None else max(self.max_value, value)
        if len(self.samples) < _MAX_NUMERIC_ROW_SAMPLES:
            self.samples.append(value)

    @property
    def variance(self) -> float:
        if self.count < 2:
            return 0.0
        return self.m2 / (self.count - 1)

    @property
    def stddev(self) -> float:
        return math.sqrt(self.variance)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "count": self.count,
            "mean": self.mean,
            "stddev": self.stddev,
            "min": self.min_value,
            "max": self.max_value,
        }


@dataclass
class ColumnAccumulator:
    name: str
    null_count: int = 0
    type_counts: Dict[str, int] = field(default_factory=lambda: {"numeric": 0, "boolean": 0, "text": 0})
    sample_values: List[Any] = field(default_factory=list)
    distinct_values: set[str] = field(default_factory=set)
    numeric_stats: Optional[RunningStats] = None

    def register_value(self, kind: str, display_value: Any, numeric_value: Optional[float]) -> None:
        if kind == "null":
            self.null_count += 1
            return

        self.type_counts[kind] = self.type_counts.get(kind, 0) + 1

        if display_value is not None and len(self.sample_values) < _MAX_SAMPLE_VALUES:
            self.sample_values.append(display_value)

        if display_value is not None and len(self.distinct_values) < _MAX_DISTINCT_TRACK:
            self.distinct_values.add(str(display_value))

        if kind == "numeric" and numeric_value is not None:
            if self.numeric_stats is None:
                self.numeric_stats = RunningStats()
            self.numeric_stats.update(float(numeric_value))

    @property
    def non_null_count(self) -> int:
        return sum(self.type_counts.values())

    @property
    def inferred_type(self) -> str:
        if not self.type_counts:
            return "unknown"
        kind, count = max(self.type_counts.items(), key=lambda item: item[1])
        if count == 0:
            return "unknown"
        return kind

    def distinct_sample(self) -> List[str]:
        return sorted(self.distinct_values)

    def to_profile(self, total_rows: int) -> Dict[str, Any]:
        non_null = self.non_null_count
        null_ratio = (self.null_count / total_rows) if total_rows else 0.0
        return {
            "name": self.name,
            "inferredType": self.inferred_type,
            "nullCount": self.null_count,
            "nullRatio": null_ratio,
            "nonNullCount": non_null,
            "sampleValues": list(self.sample_values),
            "distinctSample": self.distinct_sample(),
        }


@dataclass
class DatasetSummary:
    row_count: int
    columns: Dict[str, ColumnAccumulator]
    preview_rows: List[Dict[str, Any]]
    numeric_row_samples: List[Dict[str, float]]
    bytes_read: int
    source_format: str
    training_rows: List[Dict[str, Any]]

    @property
    def column_names(self) -> List[str]:
        return sorted(self.columns.keys())

    def column(self, name: str) -> ColumnAccumulator:
        return self.columns[name]


@dataclass
class PipelineResult:
    phases: Dict[str, Dict[str, Any]]
    metrics: Dict[str, Any]
    manifest: Dict[str, Any]
    artifact_contents: Dict[str, Dict[str, Any]]
    correlations: List[Dict[str, Any]]
    outliers: List[Dict[str, Any]]
    ml_inference: Dict[str, Any]


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

        if len(self.preview_rows) < _MAX_PREVIEW_ROWS:
            # Include columns with no values in this row as empty strings for preview consistency.
            for col_name in self.columns.keys():
                preview_row.setdefault(col_name, "")
            self.preview_rows.append(preview_row)

        if numeric_row and len(self.numeric_row_samples) < _MAX_NUMERIC_ROW_SAMPLES:
            self.numeric_row_samples.append(numeric_row)

        if len(self.training_rows) < _MAX_AUTOML_ROWS:
            for col_name in self.columns.keys():
                processed_row.setdefault(col_name, None)
            self.training_rows.append(dict(processed_row))

        self.row_count += 1

    def build(self) -> DatasetSummary:
        return DatasetSummary(
            row_count=self.row_count,
            columns=self.columns,
            preview_rows=self.preview_rows,
            numeric_row_samples=self.numeric_row_samples,
            bytes_read=self.bytes_read,
            source_format=self.source_format,
            training_rows=self.training_rows,
        )


@dataclass
class _AutoMLArtifacts:
    payload: Dict[str, Any]
    prediction_headers: Optional[List[str]] = None
    prediction_rows: Optional[List[Dict[str, Any]]] = None
    model_bytes: Optional[bytes] = None


def _format_preview(value: Any) -> Any:
    if value is None:
        return None
    text = str(value)
    if len(text) > 80:
        return text[:77] + "..."
    return text


def _ingest_delimited(key: str, body: bytes, delimiter: str, source_format: str) -> DatasetSummary:
    text = body.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    builder = _DatasetBuilder(bytes_read=len(body), source_format=source_format)
    for row in reader:
        builder.process_row(row)
    return builder.build()


def _ingest_csv(key: str, body: bytes) -> DatasetSummary:
    return _ingest_delimited(key, body, ",", "csv")


def _ingest_tsv(key: str, body: bytes) -> DatasetSummary:
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


def _ingest_zip(key: str, body: bytes) -> DatasetSummary:
    try:
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            members = [info for info in archive.infolist() if not info.is_dir()]
            members.sort(key=lambda info: _zip_member_priority(info.filename))
            for info in members:
                data = archive.read(info.filename)
                try:
                    return ingest_dataset(info.filename, data)
                except ValueError:
                    continue
    except zipfile.BadZipFile as exc:
        raise ValueError(f"Invalid ZIP archive: {key}") from exc
    raise ValueError(f"ZIP archive does not contain a supported dataset: {key}")


def _ingest_tar(key: str, body: bytes) -> DatasetSummary:
    try:
        with tarfile.open(fileobj=io.BytesIO(body), mode="r:*") as archive:
            members = [member for member in archive.getmembers() if member.isfile()]
            members.sort(key=lambda member: _zip_member_priority(member.name))
            for member in members:
                handle = archive.extractfile(member)
                if handle is None:
                    continue
                data = handle.read()
                handle.close()
                try:
                    return ingest_dataset(member.name, data)
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


def ingest_dataset(key: str, body: bytes) -> DatasetSummary:
    lowered = key.lower()
    if lowered.endswith(".zip"):
        return _ingest_zip(key, body)
    if lowered.endswith(".tar") or lowered.endswith(".tar.gz") or lowered.endswith(".tgz"):
        return _ingest_tar(key, body)
    if lowered.endswith(".gz") or lowered.endswith(".gzip"):
        try:
            decompressed = gzip.decompress(body)
        except OSError as exc:
            raise ValueError(f"Invalid GZIP payload: {key}") from exc
        return ingest_dataset(_strip_compression_suffix(key), decompressed)
    if lowered.endswith(".parquet") or lowered.endswith(".pq") or lowered.endswith(".pqt") or lowered.endswith(".parq"):
        return _ingest_parquet(body)
    if lowered.endswith(".xlsx") or lowered.endswith(".xls") or lowered.endswith(".xlsm"):
        return _ingest_excel(body)
    if lowered.endswith(".sqlite") or lowered.endswith(".sqlite3") or lowered.endswith(".db"):
        return _ingest_sqlite(body)
    if lowered.endswith(".tsv") or lowered.endswith(".tab"):
        return _ingest_tsv(key, body)
    if lowered.endswith(".jsonl") or lowered.endswith(".ndjson"):
        return _ingest_jsonl(body)
    if lowered.endswith(".json"):
        return _ingest_json(body)
    return _ingest_csv(key, body)


def _with_phase(state: MutableMapping[str, Any], phase: str, payload: Dict[str, Any], **extra: Any) -> Dict[str, Any]:
    phases = dict(state.get("phase_outputs", {}))
    phases[phase] = payload
    update: Dict[str, Any] = {"phase_outputs": phases}
    update.update(extra)
    return update


def _emit_callback(state: Mapping[str, Any], phase: str, payload: Mapping[str, Any]) -> None:
    callback = state.get("_callback")
    if not callable(callback):
        return
    index = PHASE_ORDER.index(phase)
    callback(phase, payload, index, len(PHASE_ORDER))


def ingest_node(state: MutableMapping[str, Any]) -> Dict[str, Any]:
    source = state.get("source", {})
    key = source.get("key", "dataset.csv")
    body: bytes = state.get("raw_input", b"")
    dataset = ingest_dataset(key, body)

    payload = {
        "rows": dataset.row_count,
        "columns": dataset.column_names,
        "bytesRead": dataset.bytes_read,
        "sourceFormat": dataset.source_format,
        "preview": dataset.preview_rows,
    }

    update = _with_phase(state, "ingest", payload, dataset=dataset, raw_input=None)
    _emit_callback(state, "ingest", payload)
    return update


def profile_node(state: MutableMapping[str, Any]) -> Dict[str, Any]:
    dataset: DatasetSummary = state["dataset"]
    profiles = [dataset.column(name).to_profile(dataset.row_count) for name in dataset.column_names]
    completeness = 0.0
    if profiles and dataset.row_count:
        completeness = sum(1.0 - p["nullRatio"] for p in profiles) / len(profiles)

    payload = {
        "columnProfiles": profiles,
        "datasetCompleteness": completeness,
    }

    update = _with_phase(state, "profile", payload)
    _emit_callback(state, "profile", payload)
    return update


def dq_validate_node(state: MutableMapping[str, Any]) -> Dict[str, Any]:
    dataset: DatasetSummary = state["dataset"]
    issues: List[Dict[str, Any]] = []
    rules_evaluated = 0

    for name in dataset.column_names:
        column = dataset.column(name)
        total = dataset.row_count or 1
        null_ratio = column.null_count / total
        rules_evaluated += 1
        if null_ratio > 0.2:
            issues.append(
                {
                    "column": name,
                    "rule": "null_ratio_threshold",
                    "severity": "WARNING" if null_ratio < 0.4 else "ERROR",
                    "value": null_ratio,
                    "message": f"{int(null_ratio * 100)}% nulls detected",
                }
            )

        inferred = column.inferred_type
        rules_evaluated += 1
        if inferred == "text" and column.type_counts.get("numeric", 0) > 0:
            issues.append(
                {
                    "column": name,
                    "rule": "mixed_type",
                    "severity": "WARNING",
                    "value": column.type_counts,
                    "message": "Mixed types detected; values parsed as both text and numeric.",
                }
            )

    score = max(0.0, 1.0 - (len(issues) * 0.1))

    payload = {
        "rulesEvaluated": rules_evaluated,
        "issues": issues,
        "score": score,
    }

    update = _with_phase(state, "dq_validate", payload)
    _emit_callback(state, "dq_validate", payload)
    return update


def _compute_quantiles(values: Sequence[float], quantiles: Sequence[float]) -> Dict[str, Optional[float]]:
    if not values:
        return {f"q{int(q * 100)}": None for q in quantiles}
    sorted_vals = sorted(values)
    results: Dict[str, Optional[float]] = {}
    for q in quantiles:
        index = (len(sorted_vals) - 1) * q
        lower = math.floor(index)
        upper = math.ceil(index)
        if lower == upper:
            results[f"q{int(q * 100)}"] = sorted_vals[int(index)]
        else:
            lower_val = sorted_vals[int(lower)]
            upper_val = sorted_vals[int(upper)]
            interp = lower_val + (upper_val - lower_val) * (index - lower)
            results[f"q{int(q * 100)}"] = interp
    return results


def _detect_outliers(column: ColumnAccumulator) -> Optional[Dict[str, Any]]:
    stats = column.numeric_stats
    if not stats or stats.count < 5:
        return None
    stddev = stats.stddev
    if stddev == 0:
        return None
    mu = stats.mean
    outliers = []
    for value in stats.samples:
        score = abs(value - mu) / stddev
        if score >= 3.0:
            outliers.append({"value": value, "zscore": score})
    if not outliers:
        return None
    outliers.sort(key=lambda item: item["zscore"], reverse=True)
    return {
        "column": column.name,
        "method": "zscore",
        "threshold": 3.0,
        "values": outliers[:10],
    }


def _compute_correlations(samples: Sequence[Mapping[str, float]]) -> List[Dict[str, Any]]:
    stats: Dict[Tuple[str, str], Dict[str, float]] = {}
    for row in samples:
        columns = sorted(row.keys())
        for i, left in enumerate(columns):
            x = row[left]
            for right in columns[i + 1 :]:
                y = row[right]
                key = (left, right)
                agg = stats.setdefault(
                    key,
                    {
                        "count": 0.0,
                        "sum_x": 0.0,
                        "sum_y": 0.0,
                        "sum_x2": 0.0,
                        "sum_y2": 0.0,
                        "sum_xy": 0.0,
                    },
                )
                agg["count"] += 1
                agg["sum_x"] += x
                agg["sum_y"] += y
                agg["sum_x2"] += x * x
                agg["sum_y2"] += y * y
                agg["sum_xy"] += x * y

    correlations: List[Dict[str, Any]] = []
    for (left, right), agg in stats.items():
        count = agg["count"]
        if count < 2:
            continue
        numerator = count * agg["sum_xy"] - agg["sum_x"] * agg["sum_y"]
        denom_left = count * agg["sum_x2"] - agg["sum_x"] ** 2
        denom_right = count * agg["sum_y2"] - agg["sum_y"] ** 2
        denominator = math.sqrt(denom_left * denom_right)
        if denominator == 0:
            continue
        corr = numerator / denominator
        correlations.append(
            {
                "left": left,
                "right": right,
                "correlation": corr,
                "sampleSize": int(count),
            }
        )

    correlations.sort(key=lambda item: abs(item["correlation"]), reverse=True)
    return correlations[:20]


def _sanitize_filename(name: str) -> str:
    sanitized = [
        ch if ch.isalnum() or ch in {"-", "_"} else "_"
        for ch in name
    ]
    collapsed = "".join(sanitized).strip("_")
    return collapsed or "column"


def _histogram_bin_count(samples: Sequence[float]) -> int:
    unique = len(set(samples))
    if unique <= 1:
        return 1
    return max(5, min(30, int(math.sqrt(len(samples)))))


def _render_histogram(column: ColumnAccumulator) -> Optional[bytes]:
    stats = column.numeric_stats
    if not stats or len(stats.samples) < 2:
        return None

    plt = _get_pyplot()
    if plt is None:
        return None

    fig, ax = plt.subplots(figsize=(6, 4))
    bins = _histogram_bin_count(stats.samples)
    ax.hist(
        stats.samples,
        bins=bins,
        color="#2563eb",
        edgecolor="white",
        alpha=0.85,
    )
    ax.set_title(f"Distribution of {column.name}")
    ax.set_xlabel(column.name)
    ax.set_ylabel("Frequency")
    ax.grid(True, axis="y", linestyle="--", linewidth=0.5, alpha=0.5)
    fig.tight_layout()

    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150)
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


def _render_scatter(
    samples: Sequence[Mapping[str, float]], left: str, right: str
) -> Optional[bytes]:
    plt = _get_pyplot()
    if plt is None:
        return None

    xs: List[float] = []
    ys: List[float] = []
    for row in samples:
        if left in row and right in row:
            xs.append(row[left])
            ys.append(row[right])
    if len(xs) < 2:
        return None

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.scatter(xs, ys, s=24, alpha=0.75, c="#7c3aed", edgecolors="none")
    ax.set_title(f"{left} vs {right}")
    ax.set_xlabel(left)
    ax.set_ylabel(right)
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.4)
    fig.tight_layout()

    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150)
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


def _generate_visualization_artifacts(
    dataset: DatasetSummary, correlations: Sequence[Mapping[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    artifacts: Dict[str, Dict[str, Any]] = {}
    if _get_pyplot() is None:
        return artifacts

    numeric_columns: List[ColumnAccumulator] = []
    for name in dataset.column_names:
        column = dataset.column(name)
        if column.numeric_stats and column.numeric_stats.samples:
            numeric_columns.append(column)

    numeric_columns.sort(
        key=lambda col: col.numeric_stats.count if col.numeric_stats else 0,
        reverse=True,
    )

    for column in numeric_columns[:_MAX_HISTOGRAMS]:
        rendered = _render_histogram(column)
        if not rendered:
            continue
        slug = _sanitize_filename(column.name)
        artifacts[f"results/graphs/{slug}_histogram.png"] = {
            "kind": "image",
            "data": rendered,
            "description": f"Histogram showing the distribution of {column.name} values.",
            "contentType": "image/png",
        }

    for corr in list(correlations)[:_MAX_SCATTERS]:
        left = corr.get("left")
        right = corr.get("right")
        if not isinstance(left, str) or not isinstance(right, str):
            continue
        rendered = _render_scatter(dataset.numeric_row_samples, left, right)
        if not rendered:
            continue
        slug = _sanitize_filename(f"{left}_vs_{right}")
        artifacts[f"results/graphs/{slug}_scatter.png"] = {
            "kind": "image",
            "data": rendered,
            "description": (
                f"Scatter plot illustrating the relationship between {left} and {right}."
            ),
            "contentType": "image/png",
        }

    return artifacts


def _run_automl_training(dataset: DatasetSummary) -> _AutoMLArtifacts:
    if not dataset.training_rows:
        return _AutoMLArtifacts(
            payload={
                "status": "skipped",
                "message": "Automated modeling requires at least a handful of fully parsed rows.",
            }
        )

    try:
        import pandas as pd  # type: ignore
        from pandas.api import types as pdt  # type: ignore
    except ImportError as exc:  # pragma: no cover - surfaced at runtime
        return _AutoMLArtifacts(
            payload={
                "status": "failed",
                "message": "Automated modeling requires the pandas dependency.",
                "details": str(exc),
            }
        )

    try:
        from sklearn.compose import ColumnTransformer  # type: ignore
        from sklearn.ensemble import (  # type: ignore
            GradientBoostingClassifier,
            GradientBoostingRegressor,
            RandomForestClassifier,
            RandomForestRegressor,
        )
        from sklearn.impute import SimpleImputer  # type: ignore
        from sklearn.linear_model import LinearRegression, LogisticRegression  # type: ignore
        from sklearn.metrics import (  # type: ignore
            accuracy_score,
            f1_score,
            mean_absolute_error,
            mean_squared_error,
            r2_score,
        )
        from sklearn.model_selection import train_test_split  # type: ignore
        from sklearn.pipeline import Pipeline as SKPipeline  # type: ignore
        from sklearn.preprocessing import LabelEncoder, OneHotEncoder, StandardScaler  # type: ignore
        import joblib  # type: ignore
    except ImportError as exc:  # pragma: no cover - surfaced at runtime
        return _AutoMLArtifacts(
            payload={
                "status": "failed",
                "message": "Automated modeling requires scikit-learn to be installed.",
                "details": str(exc),
            }
        )

    frame = pd.DataFrame(dataset.training_rows)
    if frame.empty:
        return _AutoMLArtifacts(
            payload={
                "status": "skipped",
                "message": "No sampled rows were available for automated modeling.",
            }
        )

    # Ensure a stable column order aligned with the dataset summary.
    for name in dataset.column_names:
        if name not in frame.columns:
            frame[name] = None
    ordered_columns = [name for name in dataset.column_names if name in frame.columns]
    frame = frame[ordered_columns]

    # Drop columns that are entirely missing or contain nested structures we cannot encode.
    drop_candidates: List[str] = []
    for col in list(frame.columns):
        series = frame[col]
        if series.dropna().empty:
            drop_candidates.append(col)
            continue
        sample_values = list(series.dropna().head(10))
        if any(isinstance(value, (list, dict, set, tuple)) for value in sample_values):
            drop_candidates.append(col)
    if drop_candidates:
        frame = frame.drop(columns=drop_candidates)
    if frame.empty:
        return _AutoMLArtifacts(
            payload={
                "status": "skipped",
                "message": "All columns were excluded from automated modeling after sanitisation.",
            }
        )

    # Promote numeric-like strings to numeric dtype and normalise booleans.
    for col in frame.columns:
        series = frame[col]
        if pdt.is_bool_dtype(series):
            frame[col] = series.astype(int)
            continue
        if pdt.is_object_dtype(series):
            numeric_candidate = pd.to_numeric(series, errors="coerce")
            if numeric_candidate.notnull().sum() >= max(3, int(0.6 * series.notnull().sum())):
                frame[col] = numeric_candidate

    lower_to_actual = {name.lower(): name for name in frame.columns}
    target_hints = ["target", "label", "y", "class", "outcome", "response", "default", "churn"]
    target_column: Optional[str] = None
    for hint in target_hints:
        if hint in lower_to_actual:
            target_column = lower_to_actual[hint]
            break

    if target_column is None:
        balanced_candidates: List[Tuple[float, str]] = []
        fallback: Optional[str] = None
        row_count = len(frame)
        for name in frame.columns:
            column = dataset.columns.get(name)
            series = frame[name]
            non_null = series.dropna()
            unique = non_null.nunique()
            if unique < 2:
                continue
            ratio = unique / max(1, len(non_null))
            if fallback is None:
                fallback = name
            type_penalty = 0.0
            if column is None or column.inferred_type == "text":
                type_penalty = 0.1
            if ratio > 0.98 and row_count > 50:
                type_penalty += 0.2
            if ratio < 0.02:
                type_penalty += 0.2
            balanced_candidates.append((type_penalty + abs(0.5 - min(ratio, 1.0)), name))
        if balanced_candidates:
            balanced_candidates.sort(key=lambda item: item[0])
            target_column = balanced_candidates[0][1]
        elif fallback is not None:
            target_column = fallback

    if target_column is None:
        return _AutoMLArtifacts(
            payload={
                "status": "skipped",
                "message": "A suitable target column could not be inferred for automated modeling.",
            }
        )

    feature_columns = [col for col in frame.columns if col != target_column]
    feature_columns = [col for col in feature_columns if frame[col].notnull().sum() > 0]
    if not feature_columns:
        return _AutoMLArtifacts(
            payload={
                "status": "skipped",
                "message": "Automated modeling needs at least one informative feature column.",
            }
        )

    X = frame[feature_columns].copy()
    target_series = frame[target_column]

    # Remove rows without a target value or with fully missing feature rows.
    mask = target_series.notnull()
    if not mask.any():
        return _AutoMLArtifacts(
            payload={
                "status": "skipped",
                "message": "No rows with target values were available for automated modeling.",
            }
        )
    X = X.loc[mask]
    target_series = target_series.loc[mask]

    feature_mask = X.notnull().sum(axis=1) > 0
    X = X.loc[feature_mask]
    target_series = target_series.loc[feature_mask]
    if len(X) < 20:
        return _AutoMLArtifacts(
            payload={
                "status": "skipped",
                "message": "At least 20 rows with usable features and targets are required for automated modeling.",
            }
        )

    column_meta = dataset.columns.get(target_column)
    target_non_null = target_series.dropna()
    unique_targets = target_non_null.nunique()
    if column_meta and column_meta.inferred_type == "numeric" and unique_targets > 10:
        problem_type = "regression"
    elif unique_targets <= 10:
        problem_type = "classification"
    elif pdt.is_numeric_dtype(target_series):
        problem_type = "regression"
    else:
        problem_type = "classification"

    if problem_type == "regression":
        numeric_target = pd.to_numeric(target_series, errors="coerce")
        mask = numeric_target.notnull()
        X = X.loc[mask]
        target_series = numeric_target.loc[mask]
    else:
        target_series = target_series.astype(str)

    if len(X) < 20:
        return _AutoMLArtifacts(
            payload={
                "status": "skipped",
                "message": "After cleaning there were fewer than 20 rows available for modeling.",
            }
        )

    # Re-apply numeric promotion after row filtering to keep dtypes stable.
    for col in X.columns:
        series = X[col]
        if pdt.is_bool_dtype(series):
            X[col] = series.astype(int)
            continue
        if pdt.is_object_dtype(series):
            numeric_candidate = pd.to_numeric(series, errors="coerce")
            if numeric_candidate.notnull().sum() >= max(3, int(0.6 * series.notnull().sum())):
                X[col] = numeric_candidate

    numeric_features = [col for col in X.columns if pdt.is_numeric_dtype(X[col])]
    categorical_features = [col for col in X.columns if col not in numeric_features]

    if not numeric_features and not categorical_features:
        return _AutoMLArtifacts(
            payload={
                "status": "skipped",
                "message": "Feature engineering removed all usable columns for modeling.",
            }
        )

    if problem_type == "classification":
        label_encoder = LabelEncoder()
        encoded_target = label_encoder.fit_transform(target_series)
        metric_name = "accuracy"
    else:
        label_encoder = None
        encoded_target = target_series.astype(float)
        metric_name = "r2"

    stratify = encoded_target if problem_type == "classification" and len(set(encoded_target)) > 1 else None
    try:
        X_train, X_valid, y_train, y_valid = train_test_split(
            X,
            encoded_target,
            test_size=0.2,
            random_state=42,
            stratify=stratify,
        )
    except ValueError:
        X_train, X_valid, y_train, y_valid = train_test_split(
            X,
            encoded_target,
            test_size=0.2,
            random_state=42,
        )

    def _make_preprocessor() -> ColumnTransformer:
        transformers = []
        if numeric_features:
            transformers.append(
                (
                    "numeric",
                    SKPipeline(
                        steps=[
                            ("imputer", SimpleImputer(strategy="median")),
                            ("scaler", StandardScaler()),
                        ]
                    ),
                    numeric_features,
                )
            )
        if categorical_features:
            try:
                encoder = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
            except TypeError:  # pragma: no cover - older sklearn fallback
                encoder = OneHotEncoder(handle_unknown="ignore", sparse=False)
            transformers.append(
                (
                    "categorical",
                    SKPipeline(
                        steps=[
                            ("imputer", SimpleImputer(strategy="most_frequent")),
                            ("encoder", encoder),
                        ]
                    ),
                    categorical_features,
                )
            )
        return ColumnTransformer(transformers=transformers, remainder="drop")

    candidate_models: List[Tuple[str, Any]]
    if problem_type == "classification":
        candidate_models = [
            ("random_forest_classifier", RandomForestClassifier(n_estimators=200, random_state=42)),
            ("gradient_boosting_classifier", GradientBoostingClassifier(random_state=42)),
            ("logistic_regression", LogisticRegression(max_iter=500, multi_class="auto")),
        ]
    else:
        candidate_models = [
            ("random_forest_regressor", RandomForestRegressor(n_estimators=200, random_state=42)),
            ("gradient_boosting_regressor", GradientBoostingRegressor(random_state=42)),
            ("linear_regression", LinearRegression()),
        ]

    candidate_summaries: List[Dict[str, Any]] = []
    warnings_out: List[str] = []
    best_pipeline: Optional[SKPipeline] = None
    best_model_name: Optional[str] = None
    best_metrics: Dict[str, Any] = {}
    best_score: float = float("-inf")

    import warnings

    for model_name, estimator in candidate_models:
        pipeline = SKPipeline(
            steps=[
                ("preprocessor", _make_preprocessor()),
                ("model", estimator),
            ]
        )
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                pipeline.fit(X_train, y_train)
            preds = pipeline.predict(X_valid)
            if problem_type == "classification":
                accuracy = float(accuracy_score(y_valid, preds))
                try:
                    f1 = float(f1_score(y_valid, preds, average="macro"))
                except ValueError:
                    f1 = accuracy
                metrics_map = {"accuracy": accuracy, "f1Macro": f1}
                score = accuracy
            else:
                r2 = float(r2_score(y_valid, preds))
                rmse = float(math.sqrt(mean_squared_error(y_valid, preds)))
                mae = float(mean_absolute_error(y_valid, preds))
                metrics_map = {"r2": r2, "rmse": rmse, "mae": mae}
                score = r2
            candidate_summaries.append(
                {
                    "name": model_name,
                    "status": "evaluated",
                    "metrics": metrics_map,
                }
            )
            if score > best_score:
                best_score = score
                best_pipeline = pipeline
                best_model_name = model_name
                best_metrics = metrics_map
        except Exception as exc:  # pragma: no cover - defensive
            warnings_out.append(f"{model_name}: {exc}")
            candidate_summaries.append(
                {
                    "name": model_name,
                    "status": "error",
                    "message": str(exc),
                }
            )

    if best_pipeline is None or best_model_name is None:
        return _AutoMLArtifacts(
            payload={
                "status": "failed",
                "message": "All automated modeling attempts failed.",
                "warnings": warnings_out,
                "candidateModels": candidate_summaries,
            }
        )

    # Re-fit on the full dataset for artifact generation.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        best_pipeline.fit(X, encoded_target)

    predictions_encoded = best_pipeline.predict(X)

    if problem_type == "classification" and label_encoder is not None:
        predictions = label_encoder.inverse_transform(predictions_encoded.astype(int))
        actual_values = target_series.astype(str)
    else:
        predictions = predictions_encoded
        actual_values = target_series.astype(float if problem_type == "regression" else str)

    prediction_table = pd.DataFrame(
        {
            "rowId": list(range(len(X))),
            "prediction": predictions,
            "actual": actual_values.values,
        }
    )

    estimator = best_pipeline.named_steps.get("model")
    if problem_type == "classification" and estimator is not None and hasattr(estimator, "predict_proba"):
        try:
            probabilities = best_pipeline.predict_proba(X)
            classes = getattr(estimator, "classes_", [])
            if label_encoder is not None and len(classes):
                try:
                    classes = label_encoder.inverse_transform(classes.astype(int))
                except Exception:
                    classes = [str(cls) for cls in classes]
            for index, cls in enumerate(classes):
                column_name = f"prob_{cls}"
                prediction_table[column_name] = probabilities[:, index]
        except Exception as exc:  # pragma: no cover - probabilistic output may fail
            warnings_out.append(f"predict_proba unavailable: {exc}")

    prediction_table = prediction_table.where(pd.notnull(prediction_table), None)
    prediction_headers = [str(col) for col in prediction_table.columns]
    prediction_rows = prediction_table.to_dict(orient="records")

    feature_stats: List[Dict[str, Any]] = []
    for col in X.columns:
        non_null = int(X[col].notnull().sum())
        coverage = float(non_null / len(X)) if len(X) else 0.0
        unique = int(X[col].nunique(dropna=True))
        feature_stats.append(
            {
                "name": col,
                "nonNullCount": non_null,
                "coverage": coverage,
                "uniqueValues": unique,
            }
        )

    if problem_type == "classification" and label_encoder is not None:
        distribution = target_series.value_counts().to_dict()
    else:
        distribution = {
            "min": float(target_series.min()),
            "max": float(target_series.max()),
            "mean": float(target_series.mean()),
            "median": float(target_series.median()),
        }

    payload: Dict[str, Any] = {
        "status": "succeeded",
        "message": "Automated model selection completed successfully.",
        "target": target_column,
        "problemType": problem_type,
        "rowsUsed": len(X),
        "featureColumns": list(X.columns),
        "bestModel": {
            "name": best_model_name,
            "metric": metric_name,
            "score": best_score,
            "metrics": best_metrics,
        },
        "candidateModels": candidate_summaries,
        "trainTestSplit": {
            "trainRows": len(X_train),
            "validationRows": len(X_valid),
        },
        "featureStats": feature_stats,
        "targetDistribution": distribution,
        "predictionPreview": prediction_rows[:10],
    }
    if warnings_out:
        payload["warnings"] = warnings_out

    model_buffer = io.BytesIO()
    joblib.dump(best_pipeline, model_buffer)
    model_bytes = model_buffer.getvalue()
    payload["artifacts"] = {
        "predictions": "results/predictions.csv",
        "model": "results/model.joblib",
    }
    payload["modelSizeBytes"] = len(model_bytes)

    return _AutoMLArtifacts(
        payload=payload,
        prediction_headers=prediction_headers,
        prediction_rows=prediction_rows,
        model_bytes=model_bytes,
    )


def descriptive_stats_node(state: MutableMapping[str, Any]) -> Dict[str, Any]:
    dataset: DatasetSummary = state["dataset"]
    stats_rows: List[Dict[str, Any]] = []
    outliers: List[Dict[str, Any]] = []

    for name in dataset.column_names:
        column = dataset.column(name)
        stats_obj = column.numeric_stats
        if not stats_obj or stats_obj.count == 0:
            continue
        quantiles = _compute_quantiles(stats_obj.samples, [0.05, 0.25, 0.5, 0.75, 0.95])
        row = {
            "column": name,
            "count": stats_obj.count,
            "mean": stats_obj.mean,
            "stddev": stats_obj.stddev,
            "min": stats_obj.min_value,
            "max": stats_obj.max_value,
        }
        row.update(quantiles)
        stats_rows.append(row)
        maybe_outlier = _detect_outliers(column)
        if maybe_outlier:
            outliers.append(maybe_outlier)

    correlations = _compute_correlations(dataset.numeric_row_samples)

    payload = {
        "numericColumns": len(stats_rows),
        "descriptiveTable": stats_rows,
        "correlations": correlations,
        "outliers": outliers,
    }

    artifact_contents = dict(state.get("artifact_contents", {}))
    if stats_rows:
        artifact_contents["results/descriptive_stats.csv"] = {
            "kind": "csv",
            "headers": [
                "column",
                "count",
                "mean",
                "stddev",
                "min",
                "max",
                "q5",
                "q25",
                "q50",
                "q75",
                "q95",
            ],
            "rows": stats_rows,
            "description": "Descriptive statistics for numeric columns.",
            "contentType": "text/csv",
        }
    if correlations:
        artifact_contents["results/correlations.csv"] = {
            "kind": "csv",
            "headers": ["left", "right", "correlation", "sampleSize"],
            "rows": correlations,
            "description": "Pairwise Pearson correlations for numeric columns.",
            "contentType": "text/csv",
        }
    if outliers:
        artifact_contents["results/outliers.json"] = {
            "kind": "json",
            "data": outliers,
            "description": "Detected outliers per column using a z-score heuristic.",
            "contentType": "application/json",
        }

    visualization_artifacts = _generate_visualization_artifacts(dataset, correlations)
    artifact_contents.update(visualization_artifacts)

    update = _with_phase(state, "descriptive_stats", payload, artifact_contents=artifact_contents)
    _emit_callback(state, "descriptive_stats", payload)
    return update


def ml_inference_node(state: MutableMapping[str, Any]) -> Dict[str, Any]:
    dataset: DatasetSummary = state["dataset"]
    artifact_contents = dict(state.get("artifact_contents", {}))

    try:
        automl_result = _run_automl_training(dataset)
    except Exception as exc:  # pragma: no cover - defensive guardrail
        payload = {
            "status": "failed",
            "message": "Automated modeling encountered an unexpected error.",
            "details": str(exc),
        }
        update = _with_phase(state, "ml_inference", payload, artifact_contents=artifact_contents)
        _emit_callback(state, "ml_inference", payload)
        return update

    payload = dict(automl_result.payload)

    if automl_result.prediction_headers and automl_result.prediction_rows is not None:
        artifact_contents["results/predictions.csv"] = {
            "kind": "csv",
            "headers": automl_result.prediction_headers,
            "rows": automl_result.prediction_rows,
            "description": "Predictions generated by the automated model selection pipeline.",
            "contentType": "text/csv",
        }

    if automl_result.model_bytes:
        artifact_contents["results/model.joblib"] = {
            "kind": "binary",
            "data": automl_result.model_bytes,
            "description": "Serialized scikit-learn pipeline selected by automated modeling.",
            "contentType": "application/octet-stream",
        }

    update = _with_phase(state, "ml_inference", payload, artifact_contents=artifact_contents)
    _emit_callback(state, "ml_inference", payload)
    return update


def nl_report_node(state: MutableMapping[str, Any]) -> Dict[str, Any]:
    dataset: DatasetSummary = state["dataset"]
    phases = state.get("phase_outputs", {})
    dq = phases.get("dq_validate", {})
    stats_phase = phases.get("descriptive_stats", {})

    row_text = f"{dataset.row_count:,} rows" if dataset.row_count else "an unknown number of rows"
    column_text = f"{len(dataset.column_names)} columns"
    dq_score = dq.get("score")
    dq_text = f"Data quality score {dq_score:.2f}" if dq_score is not None else "Data quality checks executed"

    highlights: List[str] = []
    correlations = stats_phase.get("correlations", [])
    if correlations:
        top = correlations[0]
        highlights.append(
            f"Strongest correlation observed between {top['left']} and {top['right']} (r={top['correlation']:.2f})."
        )

    outliers = stats_phase.get("outliers", [])
    if outliers:
        sample_outlier = outliers[0]
        values = sample_outlier["values"][:2]
        formatted = ", ".join(f"{v['value']:.2f}" for v in values)
        highlights.append(
            f"Potential outliers detected in {sample_outlier['column']}: {formatted}."
        )

    summary_lines = [
        f"The dataset contains {row_text} across {column_text}.",
        dq_text + ".",
    ]
    summary_lines.extend(highlights)
    summary_lines.append("Further modeling pipelines can build on these profiling insights.")
    summary_text = " ".join(summary_lines)

    payload = {
        "summary": summary_text,
        "highlights": highlights,
    }

    artifact_contents = dict(state.get("artifact_contents", {}))
    artifact_contents["results/report.txt"] = {
        "kind": "text",
        "text": summary_text,
        "description": "Natural language narrative summarizing the dataset.",
        "contentType": "text/plain",
    }

    update = _with_phase(state, "nl_report", payload, artifact_contents=artifact_contents)
    _emit_callback(state, "nl_report", payload)
    return update


def finalize_node(state: MutableMapping[str, Any]) -> Dict[str, Any]:
    dataset: DatasetSummary = state["dataset"]
    phases: Dict[str, Dict[str, Any]] = state.get("phase_outputs", {})
    dq = phases.get("dq_validate", {})
    stats_phase = phases.get("descriptive_stats", {})

    metrics = {
        "rows": dataset.row_count,
        "columns": len(dataset.column_names),
        "bytesRead": dataset.bytes_read,
        "datasetCompleteness": phases.get("profile", {}).get("datasetCompleteness"),
        "dqScore": dq.get("score"),
    }

    correlations = stats_phase.get("correlations", [])
    outliers = stats_phase.get("outliers", [])

    ml_phase = phases.get("ml_inference")
    if isinstance(ml_phase, Mapping):
        ml_inference_payload = dict(ml_phase)
    else:
        ml_inference_payload = {
            "status": "skipped",
            "message": "Automated modeling did not execute for this dataset.",
        }

    artifact_prefix: str = state.get("artifact_prefix", "artifacts")
    manifest_entries: List[Dict[str, Any]] = []
    for phase in PHASE_ORDER:
        if phase not in phases:
            continue
        manifest_entries.append(
            {
                "name": f"{phase}_json",
                "description": f"Serialized output for the {phase} phase.",
                "contentType": "application/json",
                "key": f"{artifact_prefix}/phases/{phase}.json",
            }
        )

    for relative_key, spec in state.get("artifact_contents", {}).items():
        manifest_entries.append(
            {
                "name": relative_key.replace("/", "_"),
                "description": spec.get("description"),
                "contentType": spec.get("contentType"),
                "key": f"{artifact_prefix}/{relative_key}",
            }
        )

    manifest_entries.append(
        {
            "name": "results_json",
            "description": "Consolidated analytics results payload.",
            "contentType": "application/json",
            "key": f"{artifact_prefix}/results/results.json",
        }
    )
    manifest_entries.append(
        {
            "name": "results_manifest",
            "description": "Manifest describing generated analytics artifacts.",
            "contentType": "application/json",
            "key": f"{artifact_prefix}/results/manifest.json",
        }
    )

    manifest = {
        "jobId": state.get("job_id"),
        "basePath": artifact_prefix + "/",
        "artifacts": manifest_entries,
    }

    payload = {
        "metrics": metrics,
        "manifest": manifest,
        "mlInference": ml_inference_payload,
    }

    update = _with_phase(state, "finalize", payload, manifest=manifest, final_summary={
        "metrics": metrics,
        "correlations": correlations,
        "outliers": outliers,
        "mlInference": ml_inference_payload,
    })
    _emit_callback(state, "finalize", payload)
    return update


builder = StateGraph(dict)
builder.add_node("ingest", ingest_node)
builder.add_node("profile", profile_node)
builder.add_node("dq_validate", dq_validate_node)
builder.add_node("descriptive_stats", descriptive_stats_node)
builder.add_node("ml_inference", ml_inference_node)
builder.add_node("nl_report", nl_report_node)
builder.add_node("finalize", finalize_node)
builder.set_entry_point("ingest")
builder.add_edge("ingest", "profile")
builder.add_edge("profile", "dq_validate")
builder.add_edge("dq_validate", "descriptive_stats")
builder.add_edge("descriptive_stats", "ml_inference")
builder.add_edge("ml_inference", "nl_report")
builder.add_edge("nl_report", "finalize")
builder.add_edge("finalize", END)
PIPELINE = builder.compile()


def run_pipeline(
    job_id: str,
    source: Mapping[str, Any],
    body: bytes,
    *,
    artifact_prefix: str,
    on_phase: Optional[PhaseCallback] = None,
) -> PipelineResult:
    initial_state: Dict[str, Any] = {
        "job_id": job_id,
        "source": dict(source),
        "raw_input": body,
        "artifact_prefix": artifact_prefix,
        "phase_outputs": {},
        "artifact_contents": {},
    }
    if on_phase:
        initial_state["_callback"] = on_phase

    final_state = PIPELINE.invoke(initial_state)
    phases = final_state.get("phase_outputs", {})
    final_summary = final_state.get("final_summary", {})
    artifact_contents = final_state.get("artifact_contents", {})

    return PipelineResult(
        phases=phases,
        metrics=final_summary.get("metrics", {}),
        manifest=final_state.get("manifest", {}),
        artifact_contents=artifact_contents,
        correlations=final_summary.get("correlations", []),
        outliers=final_summary.get("outliers", []),
        ml_inference=final_summary.get("mlInference", {}),
    )


def lambda_handler(event: Mapping[str, Any], _context: Any) -> Dict[str, Any]:
    job_id = event.get("jobId")
    if not job_id:
        raise ValueError("jobId is required")

    artifact_prefix = event.get("artifactPrefix") or f"artifacts/{job_id}"
    source = event.get("source") or {}
    body = event.get("body")
    if body is None:
        raise ValueError("body is required")
    if isinstance(body, str):
        body_bytes = base64.b64decode(body)
    elif isinstance(body, (bytes, bytearray)):
        body_bytes = bytes(body)
    else:
        raise TypeError("body must be bytes or base64-encoded string")

    result = run_pipeline(job_id, source, body_bytes, artifact_prefix=artifact_prefix)
    return {
        "jobId": job_id,
        "phases": result.phases,
        "metrics": result.metrics,
        "manifest": result.manifest,
        "correlations": result.correlations,
        "outliers": result.outliers,
        "mlInference": result.ml_inference,
    }
