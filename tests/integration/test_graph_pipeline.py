import csv
import gzip
import importlib
import io
import json
import os
import sqlite3
import sys
import tempfile
from types import ModuleType
from typing import Iterable, List

import pytest


SAMPLE_ROWS = [{"id": index, "value": index * 10} for index in range(1, 11)]

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)


def _ensure_state_graph_stub() -> None:
    try:
        importlib.import_module("langgraph.graph")
        return
    except Exception:
        pass

    pkg = ModuleType("langgraph")
    mod = ModuleType("langgraph.graph")

    END = object()

    class _CompiledGraph:
        def __init__(self, nodes, order):
            self._nodes = nodes
            self._order = order

        def invoke(self, initial_state):
            state = dict(initial_state)
            for name in self._order:
                update = self._nodes[name](state)
                if update:
                    state.update(update)
            return state

    class StateGraph:
        def __init__(self, _state_type):
            self._nodes = {}
            self._edges = {}
            self._entry = None

        def add_node(self, name, func):
            self._nodes[name] = func

        def set_entry_point(self, name):
            self._entry = name

        def add_edge(self, source, dest):
            self._edges.setdefault(source, []).append(dest)

        def compile(self):
            order: List[str] = []
            current = self._entry
            visited: set[str] = set()
            while current is not None and current != END:
                if current in visited:
                    raise RuntimeError("cycle detected in stub graph")
                visited.add(current)
                order.append(current)
                next_nodes = self._edges.get(current, [])
                current = next_nodes[0] if next_nodes else None
            return _CompiledGraph(self._nodes, order)

    mod.END = END
    mod.StateGraph = StateGraph
    pkg.graph = mod
    sys.modules["langgraph"] = pkg
    sys.modules["langgraph.graph"] = mod


def _csv_bytes(rows: Iterable[dict]) -> bytes:
    buffer = io.StringIO()
    fieldnames = list(rows[0].keys()) if isinstance(rows, list) else None
    if fieldnames is None:
        first = next(iter(rows))
        fieldnames = list(first.keys())
        rows = [first] + list(rows)
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue().encode("utf-8")


def _jsonl_bytes(rows: Iterable[dict]) -> bytes:
    return "\n".join(json.dumps(row) for row in rows).encode("utf-8") + b"\n"


def _gzip_bytes(payload: bytes) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as handle:
        handle.write(payload)
    return buffer.getvalue()


def _sqlite_bytes(rows: Iterable[dict]) -> bytes:
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".sqlite")
    try:
        with sqlite3.connect(temp.name) as conn:
            conn.execute("CREATE TABLE metrics (id INTEGER, value INTEGER)")
            conn.executemany(
                "INSERT INTO metrics (id, value) VALUES (?, ?)",
                [(row["id"], row["value"]) for row in rows],
            )
            conn.commit()
        with open(temp.name, "rb") as handle:
            return handle.read()
    finally:
        try:
            os.unlink(temp.name)
        except FileNotFoundError:
            pass


_ensure_state_graph_stub()
module = importlib.import_module("services.workers.graph.graph")
importlib.reload(module)
run_pipeline = module.run_pipeline
PHASE_ORDER = module.PHASE_ORDER


@pytest.mark.parametrize(
    "key, body, expected_format",
    [
        ("sample.csv", _csv_bytes(SAMPLE_ROWS), "csv"),
        ("sample.jsonl", _jsonl_bytes(SAMPLE_ROWS), "jsonl"),
        ("compressed.csv.gz", _gzip_bytes(_csv_bytes(SAMPLE_ROWS)), "csv"),
        ("dataset.sqlite", _sqlite_bytes(SAMPLE_ROWS), "sqlite"),
    ],
)
def test_pipeline_ingests_diverse_formats(key, body, expected_format):
    job_id = f"job-{expected_format}"
    result = run_pipeline(job_id, {"key": key}, body, artifact_prefix=f"artifacts/{job_id}")

    for phase in PHASE_ORDER:
        assert phase in result.phases

    ingest = result.phases["ingest"]
    assert ingest["rows"] == len(SAMPLE_ROWS)
    assert ingest["sourceFormat"] == expected_format
    assert set(ingest["columns"]) == {"id", "value"}

    metrics = result.metrics
    assert metrics["rows"] == len(SAMPLE_ROWS)
    assert metrics["columns"] == 2
    assert 0.0 <= metrics["datasetCompleteness"] <= 1.0

    manifest_keys = {entry["key"] for entry in result.manifest["artifacts"]}
    assert f"artifacts/{job_id}/results/results.json" in manifest_keys
    assert f"artifacts/{job_id}/results/manifest.json" in manifest_keys

    contents_keys = result.artifact_contents.keys()
    assert "results/descriptive_stats.csv" in contents_keys
    assert "results/report.txt" in contents_keys

    ml_phase = result.phases["ml_inference"]
    assert isinstance(ml_phase, dict)
    assert ml_phase.get("status") in {"failed", "skipped", "completed"}

    summary = result.phases["nl_report"]["summary"]
    assert f"{len(SAMPLE_ROWS)} rows" in summary
