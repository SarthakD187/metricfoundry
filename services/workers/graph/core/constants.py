from pathlib import Path

PHASE_ORDER = [
    "ingest",
    "profile",
    "dq_validate",
    "descriptive_stats",
    "ml_inference",
    "nl_report",
    "finalize",
]

_MAX_PREVIEW_ROWS = 5
_MAX_SAMPLE_VALUES = 5
_MAX_DISTINCT_TRACK = 25
_DUPLICATE_WARNING_RATIO = 0.01
_DUPLICATE_ERROR_RATIO = 0.05
_NUMERIC_RANGE_WARNING_RATIO = 0.01
_NUMERIC_RANGE_ERROR_RATIO = 0.05
_CARDINALITY_WARNING_THRESHOLD = 15
_CARDINALITY_ERROR_THRESHOLD = 50
_DQ_SEVERITY_WEIGHTS = {"ERROR": 0.5, "WARNING": 0.2, "INFO": 0.05}
_MAX_NUMERIC_ROW_SAMPLES = 500
_MAX_AUTOML_ROWS = 2000
_MAX_HISTOGRAMS = 8
_MAX_SCATTERS = 3
_MAX_BOX_PLOTS = 8
_MAX_HEATMAP_COLUMNS = 10

_DELIMITED_STREAM_CHUNK_SIZE = 64 * 1024
_ARCHIVE_STREAM_CHUNK_SIZE = 4 * 1024 * 1024

_NULL_SENTINELS = {"", "null", "NULL", "NaN", "nan"}

_TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "templates"
_NL_REPORT_TEMPLATE_NAME = "nl_report.html.j2"

_ZIP_PREFERRED_EXTENSIONS = [
    ".csv", ".tsv", ".jsonl", ".ndjson", ".json",
    ".parquet", ".pq", ".pqt", ".parq",
    ".xlsx", ".xls", ".xlsm",
    ".sqlite", ".sqlite3", ".db",
]
