from __future__ import annotations

import os
import time
import sqlite3
import base64
from typing import Any, Dict, Mapping, Optional, Callable
from collections.abc import Mapping as MappingABC, Sequence

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from langgraph.graph import END, StateGraph
try:
    from langgraph.checkpoint.sqlite import SqliteSaver  # type: ignore
except Exception:
    SqliteSaver = None  # type: ignore

try:
    from langgraph.checkpoint.dynamodb import DynamoDBSaver  # type: ignore
except Exception:
    DynamoDBSaver = None
from .nodes import (
    ingest_node, profile_node, dq_validate_node, descriptive_stats_node,
    ml_inference_node, nl_report_node, finalize_node
)
from .core.constants import PHASE_ORDER
from .core.types import PipelineResult, BinaryInput
from .core.state import _with_phase  # only if you need it in handlers


_ddb_resource = None
_s3_client = None

_DEFAULT_MAX_BODY_BYTES = 512 * 1024 * 1024  # 512 MB safety guardrail
_STREAM_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB chunks keep memory bounded


class _LimitedStream:
    def __init__(self, stream: Any, limit: int) -> None:
        self._stream = stream
        self._limit = limit
        self._consumed = 0

    def read(self, size: int = -1) -> bytes:
        data = self._stream.read(size)
        if not data:
            return data
        if isinstance(data, memoryview):
            data = data.tobytes()
        elif isinstance(data, bytearray):
            data = bytes(data)
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("Stream produced non-bytes payload")
        self._consumed += len(data)
        if self._consumed > self._limit:
            raise ValueError(
                f"Staged input exceeds maximum supported size of {self._limit} bytes"
            )
        return data

    def close(self) -> None:
        closer = getattr(self._stream, "close", None)
        if callable(closer):
            closer()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._stream, name)

    def seek(self, *args: Any, **kwargs: Any) -> Any:
        seeker = getattr(self._stream, "seek", None)
        if callable(seeker):
            return seeker(*args, **kwargs)
        raise AttributeError("Underlying stream does not support seek")


def _resolve_max_body_bytes() -> int:
    value = os.environ.get("MAX_PIPELINE_BODY_BYTES")
    if value:
        try:
            parsed = int(value)
            if parsed > 0:
                return parsed
        except ValueError:
            pass
    return _DEFAULT_MAX_BODY_BYTES


_MAX_BODY_BYTES = _resolve_max_body_bytes()


PhaseCallback = Optional[Callable[..., None]]


def _normalize_for_json(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"__b64__": True, "data": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, MappingABC):
        return {str(k): _normalize_for_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, Sequence)) and not isinstance(value, (str, bytes, bytearray)):
        return [_normalize_for_json(v) for v in value]
    if isinstance(value, set):
        return [_normalize_for_json(v) for v in sorted(value)]
    return value


def _denormalize_from_json(value: Any) -> Any:
    if isinstance(value, MappingABC):
        if value.get("__b64__") and "data" in value:
            try:
                return base64.b64decode(value["data"])
            except Exception:
                return b""
        return {k: _denormalize_from_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_denormalize_from_json(v) for v in value]
    return value


def encode_pipeline_result(result: PipelineResult) -> Dict[str, Any]:
    return {
        "phases": _normalize_for_json(result.phases),
        "metrics": _normalize_for_json(result.metrics),
        "manifest": _normalize_for_json(result.manifest),
        "artifactContents": _normalize_for_json(result.artifact_contents),
        "correlations": _normalize_for_json(result.correlations),
        "outliers": _normalize_for_json(result.outliers),
        "mlInference": _normalize_for_json(result.ml_inference),
    }


def decode_pipeline_result(payload: Mapping[str, Any]) -> PipelineResult:
    return PipelineResult(
        phases=_denormalize_from_json(payload.get("phases", {})) or {},
        metrics=_denormalize_from_json(payload.get("metrics", {})) or {},
        manifest=_denormalize_from_json(payload.get("manifest", {})) or {},
        artifact_contents=_denormalize_from_json(payload.get("artifactContents", {})) or {},
        correlations=_denormalize_from_json(payload.get("correlations", [])) or [],
        outliers=_denormalize_from_json(payload.get("outliers", [])) or [],
        ml_inference=_denormalize_from_json(payload.get("mlInference", {})) or {},
    )


def _get_ddb_resource():
    global _ddb_resource
    if _ddb_resource is None:
        _ddb_resource = boto3.resource("dynamodb")
    return _ddb_resource


def _get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def _ddb_upsert_status(table_name: str, job_id: str, status: str, **attrs: Any) -> None:
    table = _get_ddb_resource().Table(table_name)
    expr_names = {"#s": "status"}
    expr_vals = {":s": status, ":u": int(time.time())}
    update_parts = ["#s = :s", "updatedAt = :u"]

    for key, value in attrs.items():
        placeholder = f":{key}"
        expr_vals[placeholder] = value
        update_parts.append(f"{key} = {placeholder}")

    try:
        table.update_item(
            Key={"pk": f"job#{job_id}", "sk": "meta"},
            UpdateExpression="SET " + ", ".join(update_parts),
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_vals,
        )
    except (BotoCoreError, ClientError) as exc:
        print(f"[LangGraphWorker] Warning: failed to upsert status for {job_id}: {exc}")


def _callback_from_event(event: Mapping[str, Any]) -> PhaseCallback:
    callback_cfg = event.get("callback")
    if not isinstance(callback_cfg, MappingABC):
        return None

    mode = str(callback_cfg.get("mode", "log")).lower()
    job_id = str(callback_cfg.get("jobId") or event.get("jobId") or "").strip()
    table_name = str(
        callback_cfg.get("table")
        or os.environ.get("TABLE_NAME")
        or os.environ.get("JOBS_TABLE")
        or ""
    ).strip()

    if mode == "ddb" and job_id and table_name:
        def _callback(phase: str, payload: Mapping[str, Any], index: int, total: int) -> None:
            progress = int(((index + 1) / max(total, 1)) * 100)
            summary: Dict[str, Any] = {}
            text = payload.get("summary") if isinstance(payload, MappingABC) else None
            if isinstance(text, str):
                summary["summary"] = text
            metrics = payload.get("metrics") if isinstance(payload, MappingABC) else None
            if isinstance(metrics, MappingABC):
                summary["metrics"] = dict(metrics)
            try:
                _ddb_upsert_status(
                    table_name,
                    job_id,
                    "RUNNING",
                    currentPhase=phase,
                    phaseIndex=index,
                    phaseCount=total,
                    progress=progress,
                    phaseSummary=summary,
                )
            except Exception as exc:  # pragma: no cover - defensive
                print(f"[LangGraphWorker] Warning: callback update failed for {phase}: {exc}")

        return _callback

    if mode == "log":
        def _log_callback(phase: str, _payload: Mapping[str, Any], index: int, total: int) -> None:
            print(f"[LangGraphWorker] Phase {index + 1}/{total}: {phase}")

        return _log_callback

    return None


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


def _stream_to_bytes(reader: Callable[[int], Any], *, limit: Optional[int]) -> bytes:
    chunks = bytearray()
    total = 0
    chunk_size = _STREAM_CHUNK_SIZE
    while True:
        try:
            piece = reader(chunk_size)
        except TypeError:
            piece = reader()
        if not piece:
            break
        if not isinstance(piece, (bytes, bytearray, memoryview)):
            raise TypeError("Stream produced non-bytes payload")
        data = bytes(piece)
        total += len(data)
        if limit is not None and total > limit:
            raise ValueError(
                f"Staged input exceeds maximum supported size of {limit} bytes"
            )
        chunks.extend(data)
    return bytes(chunks)


def _to_bytes(b: Any, *, limit: Optional[int] = None) -> bytes:
    """
    Normalize a BinaryInput (bytes, bytearray, memoryview, or file-like) to bytes.
    Prevents non-serializable objects (e.g., BytesIO) from leaking into checkpoint state.
    """
    effective_limit = _MAX_BODY_BYTES if limit is None else limit
    if isinstance(b, (bytes, bytearray, memoryview)):
        data = bytes(b)
        if len(data) > effective_limit:
            raise ValueError(
                f"Staged input exceeds maximum supported size of {effective_limit} bytes"
            )
        return data
    read = getattr(b, "read", None)
    if callable(read):
        return _stream_to_bytes(read, limit=effective_limit)
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
    artifact_prefix: Optional[str],
    body: BinaryInput,
    *,
    on_phase: PhaseCallback = None,
) -> PipelineResult:
    body_bytes: Optional[bytes] = None
    body_stream: Optional[BinaryInput] = None

    if isinstance(body, (bytes, bytearray, memoryview)):
        body_bytes = _to_bytes(body)
    else:
        read = getattr(body, "read", None)
        if callable(read):
            body_stream = body
        else:
            body_bytes = _to_bytes(body)

    normalized_prefix = (artifact_prefix or "").strip()
    if not normalized_prefix:
        normalized_prefix = f"artifacts/{job_id}"

    initial_state: Dict[str, Any] = {
        "job_id": job_id,
        "jobId": job_id,
        "source": dict(source),
        # keep legacy key; ingest_node supports body/raw_input
        "raw_input": body_bytes,
        "artifact_prefix": normalized_prefix,
        "artifactPrefix": normalized_prefix,
        "phase_outputs": {},
        "artifact_contents": {},
    }
    if body_stream is not None:
        initial_state["body"] = body_stream
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

            if SqliteSaver is not None:
                checkpointer = SqliteSaver(conn)
            else:  # pragma: no cover - fallback when sqlite saver unavailable
                try:
                    conn.close()
                except Exception:
                    pass

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
    body_s3 = event.get("bodyS3")
    pipeline_body: Optional[BinaryInput] = None
    limited_stream: Optional[_LimitedStream] = None

    if body_s3 and isinstance(body_s3, MappingABC):
        bucket = body_s3.get("bucket")
        key = body_s3.get("key")
        if not bucket or not key:
            raise ValueError("bodyS3 requires bucket and key")
        client = _get_s3_client()
        response = client.get_object(Bucket=bucket, Key=key)
        stream = response.get("Body")
        if stream is None:
            raise ValueError("S3 object body missing")
        content_length = response.get("ContentLength")
        if isinstance(content_length, int) and content_length > _MAX_BODY_BYTES:
            raise ValueError(
                f"Staged input is {content_length} bytes which exceeds the limit of {_MAX_BODY_BYTES}"
            )
        limited_stream = _LimitedStream(stream, _MAX_BODY_BYTES)
        pipeline_body = limited_stream
    elif body is None:
        raise ValueError("body or bodyS3 is required")
    elif isinstance(body, str):
        pipeline_body = base64.b64decode(body)
    elif isinstance(body, (bytes, bytearray, memoryview)):
        pipeline_body = bytes(body)
    else:
        pipeline_body = _to_bytes(body)

    if pipeline_body is None:
        pipeline_body = b""

    callback = _callback_from_event(event)

    try:
        result = run_pipeline(
            job_id,
            source,
            artifact_prefix,
            pipeline_body,
            on_phase=callback,
        )
    finally:
        if limited_stream is not None:
            try:
                limited_stream.close()
            except Exception:
                pass

    encoded = encode_pipeline_result(result)
    return {"jobId": job_id, **encoded}
