from __future__ import annotations

import os
import sqlite3
import base64
from typing import Any, Dict, Mapping, Optional, Callable
from collections.abc import Mapping as MappingABC

from langgraph.graph import END, StateGraph
from langgraph.checkpoint.sqlite import SqliteSaver
try:
    from langgraph.checkpoint.dynamodb import DynamoDBSaver
except Exception:
    DynamoDBSaver = None  # type: ignore

from .nodes import (
    ingest_node, profile_node, dq_validate_node, descriptive_stats_node,
    ml_inference_node, nl_report_node, finalize_node
)
from .core.constants import PHASE_ORDER
from .core.types import PipelineResult, BinaryInput
from .core.state import _with_phase  # only if you need it in handlers


PhaseCallback = Optional[Callable[..., None]]


def build_graph(checkpointer=None):
    g = StateGraph(dict)
    g.add_node("ingest", ingest_node)
    g.add_node("profile", profile_node)
    g.add_node("dq_validate", dq_validate_node)
    g.add_node("descriptive_stats", descriptive_stats_node)
    g.add_node("ml_inference", ml_inference_node)
    g.add_node("nl_report", nl_report_node)
    g.add_node("finalize", finalize_node)

    g.set_entry_point("ingest")
    g.add_edge("ingest", "profile")
    g.add_edge("profile", "dq_validate")
    g.add_edge("dq_validate", "descriptive_stats")
    g.add_edge("descriptive_stats", "ml_inference")
    g.add_edge("ml_inference", "nl_report")
    g.add_edge("nl_report", "finalize")
    g.add_edge("finalize", END)
    return g.compile(checkpointer=checkpointer)


def _to_bytes(b: Any) -> bytes:
    """
    Normalize a BinaryInput (bytes, bytearray, memoryview, or file-like) to bytes.
    Prevents non-serializable objects (e.g., BytesIO) from leaking into checkpoint state.
    """
    if isinstance(b, (bytes, bytearray, memoryview)):
        return bytes(b)
    # file-like (duck-type .read)
    read = getattr(b, "read", None)
    if callable(read):
        data = read()
        return bytes(data) if not isinstance(data, (bytes, bytearray, memoryview)) else bytes(data)
    raise TypeError(f"Unsupported BinaryInput type: {type(b).__name__}")


def _as_mapping(obj: Any) -> Dict[str, Any]:
    """
    Normalize arbitrary payloads to a dict to avoid attribute errors.
    Useful defensive layer if a node accidentally returns a non-mapping.
    """
    if isinstance(obj, MappingABC):
        return dict(obj)
    if isinstance(obj, set):
        return {"items": sorted(obj)}
    return {"value": obj}


def run_pipeline(
    job_id: str,
    source: Mapping[str, Any],
    body: BinaryInput,
    *,
    artifact_prefix: str,
    on_phase: PhaseCallback = None,
) -> PipelineResult:
    # Always normalize to raw bytes BEFORE placing into graph state (checkpoint-safe)
    body_bytes = _to_bytes(body)

    initial_state: Dict[str, Any] = {
        "job_id": job_id,
        "source": dict(source),
        # keep legacy key; ingest_node supports body/raw_input
        "raw_input": body_bytes,
        "artifact_prefix": artifact_prefix,
        "phase_outputs": {},
        "artifact_contents": {},
    }
    if on_phase:
        initial_state["_callback"] = on_phase

    disable_ckpt = os.environ.get("MF_DISABLE_CHECKPOINT", "").lower() in {"1", "true", "yes"}
    checkpointer = None

    if not disable_ckpt:
        table_name = os.environ.get("CHECKPOINT_TABLE")
        if table_name and DynamoDBSaver is not None:
            try:
                checkpointer = DynamoDBSaver(table_name=table_name)
            except Exception:
                checkpointer = None

        if checkpointer is None:
            db_path = os.environ.get("CHECKPOINT_SQLITE_PATH", "graph.ckpt.sqlite")
            use_uri = db_path.startswith("file:")
            if not use_uri:
                try:
                    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
                except Exception:
                    # best effort
                    pass

            # IMPORTANT: allow use across LangGraph worker threads
            conn = sqlite3.connect(
                db_path,
                uri=use_uri,
                check_same_thread=False,   # <-- key fix
            )
            # optional but recommended for concurrency
            try:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
            except Exception:
                pass

            checkpointer = SqliteSaver(conn)

    app = build_graph(checkpointer=checkpointer if not disable_ckpt else None)
    config = {
        "configurable": {
            "thread_id": f"job:{job_id}",
            "checkpoint_ns": "metricfoundry",
            "checkpoint_id": job_id,
        }
    } if checkpointer is not None else {}

    final_state = app.invoke(initial_state, config)

    phases = final_state.get("phase_outputs", {}) or {}
    final_summary = _as_mapping(final_state.get("final_summary", {}))
    artifact_contents = final_state.get("artifact_contents", {}) or {}
    manifest = final_state.get("manifest", {}) or {}

    return PipelineResult(
        phases=phases,
        metrics=final_summary.get("metrics", {}) or {},
        manifest=manifest,
        artifact_contents=artifact_contents,
        correlations=final_summary.get("correlations", []) or [],
        outliers=final_summary.get("outliers", []) or [],
        ml_inference=final_summary.get("mlInference", {}) or {},
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

    # Accept base64-encoded string or raw bytes
    if isinstance(body, str):
        body_bytes = base64.b64decode(body)
    elif isinstance(body, (bytes, bytearray, memoryview)):
        body_bytes = bytes(body)
    else:
        # If an API caller sent a file-like object, normalize it too
        body_bytes = _to_bytes(body)

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
