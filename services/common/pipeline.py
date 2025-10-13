"""Helpers for working with MetricFoundry's analytics pipeline outputs."""
from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, TYPE_CHECKING

from botocore.exceptions import ClientError

if TYPE_CHECKING:  # pragma: no cover - import only for static typing
    from services.workers.graph.graph import PipelineResult
else:  # pragma: no cover - at runtime we treat PipelineResult as ``Any``
    PipelineResult = Any  # type: ignore[misc,assignment]


ANALYSIS_VERSION = "2024.05"


def _base_prefix(job_id: str, artifact_prefix: Optional[str] = None) -> str:
    if artifact_prefix and str(artifact_prefix).strip():
        return str(artifact_prefix).rstrip("/")
    return f"artifacts/{job_id}"


def result_key_for(job_id: str, artifact_prefix: Optional[str] = None) -> str:
    base = _base_prefix(job_id, artifact_prefix)
    return f"{base}/results/results.json"


def manifest_key_for(job_id: str, artifact_prefix: Optional[str] = None) -> str:
    base = _base_prefix(job_id, artifact_prefix)
    return f"{base}/results/manifest.json"


def phase_key_for(job_id: str, phase: str, artifact_prefix: Optional[str] = None) -> str:
    base = _base_prefix(job_id, artifact_prefix)
    return f"{base}/phases/{phase}.json"


def error_key_for(job_id: str, artifact_prefix: Optional[str] = None) -> str:
    base = _base_prefix(job_id, artifact_prefix)
    return f"{base}/results/error.json"


def artifact_key_for(job_id: str, relative: str, artifact_prefix: Optional[str] = None) -> str:
    base = _base_prefix(job_id, artifact_prefix)
    return f"{base}/{relative}".replace("//", "/")


def _json_bytes(data: Any) -> bytes:
    return json.dumps(data, indent=2, default=str).encode("utf-8")


def _csv_bytes(headers: Iterable[str], rows: Iterable[Mapping[str, Any]]) -> bytes:
    output = io.StringIO()
    fieldnames = list(headers)
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow({name: row.get(name, "") for name in fieldnames})
    return output.getvalue().encode("utf-8")


def _bytes_for_artifact(spec: Mapping[str, Any]) -> bytes:
    kind = spec.get("kind")
    if kind == "json":
        return _json_bytes(spec.get("data"))
    if kind == "text":
        text = spec.get("text", "")
        if isinstance(text, bytes):
            return text
        return str(text).encode("utf-8")
    if kind == "csv":
        headers = spec.get("headers", [])
        rows = spec.get("rows", [])
        return _csv_bytes(list(headers), list(rows))
    if kind == "html":
        html = spec.get("html")
        if isinstance(html, bytes):
            return html
        return str(html or spec.get("text", "")).encode("utf-8")
    if kind in {"image", "binary"}:
        data = spec.get("data", b"")
        if isinstance(data, memoryview):  # pragma: no cover - defensive conversion
            data = data.tobytes()
        if not isinstance(data, (bytes, bytearray)):
            raise ValueError("Binary artifact data must be bytes-like")
        return bytes(data)
    raise ValueError(f"Unsupported artifact kind: {kind}")


def _content_type_for_artifact(relative_key: str, spec: Mapping[str, Any]) -> str:
    value = spec.get("contentType")
    if isinstance(value, str) and value:
        return value
    kind = spec.get("kind")
    if kind == "json":
        return "application/json"
    if kind == "text":
        return "text/plain"
    if kind == "csv":
        return "text/csv"
    if kind == "html":
        return "text/html"
    if kind == "image":
        return "image/png"
    if relative_key.endswith(".zip"):
        return "application/zip"
    if relative_key.endswith(".png"):
        return "image/png"
    if relative_key.endswith(".json"):
        return "application/json"
    if relative_key.endswith(".csv"):
        return "text/csv"
    if relative_key.endswith(".html"):
        return "text/html"
    return "application/octet-stream"


def persist_pipeline_outputs(
    job_id: str,
    bucket: str,
    result: "PipelineResult",
    *,
    s3_client,
    artifact_prefix: Optional[str] = None,
) -> Dict[str, str]:
    """Upload phase payloads and generated artifacts to S3.

    Returns a mapping that includes the manifest key and phase artifact keys.
    """

    uploaded: Dict[str, str] = {}
    for phase, payload in result.phases.items():
        key = phase_key_for(job_id, phase, artifact_prefix)
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=_json_bytes(payload),
            ContentType="application/json",
        )
        uploaded[f"phase:{phase}"] = key

    for relative_key, spec in result.artifact_contents.items():
        key = artifact_key_for(job_id, relative_key, artifact_prefix)
        body = _bytes_for_artifact(spec)
        content_type = _content_type_for_artifact(relative_key, spec)
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType=content_type,
        )

    manifest_key = manifest_key_for(job_id, artifact_prefix)
    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=_json_bytes(result.manifest),
        ContentType="application/json",
    )
    uploaded["manifest"] = manifest_key
    return uploaded


def summarize_phase_payload(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    summary: Dict[str, Any] = {}
    text = payload.get("summary")
    if isinstance(text, str):
        summary["summary"] = text
    metrics = payload.get("metrics")
    if isinstance(metrics, Mapping):
        summary["metrics"] = dict(metrics)
    dq_score = payload.get("datasetCompleteness")
    if dq_score is not None:
        summary["datasetCompleteness"] = dq_score
    if summary:
        return summary
    keys = list(payload.keys())[:5]
    return {"fields": keys}


def _profile_rows_fallback(profile_phase: Mapping[str, Any], schema: List[Dict[str, Any]]) -> Optional[int]:
    for key in ("rowCount", "rows", "nRows", "shapeRows"):
        value = profile_phase.get(key)
        if isinstance(value, int) and value >= 0:
            return value
    shape = profile_phase.get("shape")
    if isinstance(shape, Mapping) and isinstance(shape.get("rows"), int):
        return shape["rows"]
    metrics = profile_phase.get("metrics")
    if isinstance(metrics, Mapping) and isinstance(metrics.get("rows"), int):
        return metrics["rows"]
    return None


def _profile_columns_fallback(profile_phase: Mapping[str, Any], schema: List[Dict[str, Any]]) -> Optional[int]:
    if schema:
        return len(schema)
    for key in ("columnCount", "columns", "nCols", "shapeCols"):
        value = profile_phase.get(key)
        if isinstance(value, int) and value >= 0:
            return value
    shape = profile_phase.get("shape")
    if isinstance(shape, Mapping) and isinstance(shape.get("columns"), int):
        return shape["columns"]
    columns = profile_phase.get("columnProfiles")
    if isinstance(columns, list):
        return len(columns)
    return None


def build_results_payload(
    job_id: str,
    result: "PipelineResult",
    *,
    source_input: Optional[Mapping[str, Any]] = None,
    artifact_bucket: Optional[str] = None,
    analysis_version: str = ANALYSIS_VERSION,
    artifact_prefix: Optional[str] = None,
) -> Dict[str, Any]:
    metrics = dict(result.metrics)
    profile_phase = (
        result.phases.get("profile")
        if isinstance(result.phases, Mapping)
        else None
    ) or {}

    schema: List[Dict[str, Any]] = []
    if isinstance(profile_phase, Mapping):
        columns = profile_phase.get("columnProfiles")
        if isinstance(columns, list):
            for column in columns:
                if not isinstance(column, Mapping):
                    continue
                name = column.get("name")
                if name is None:
                    continue
                entry: Dict[str, Any] = {"name": str(name)}
                inferred = column.get("inferredType")
                if inferred is not None:
                    entry["type"] = inferred
                schema.append(entry)

    rows_value = metrics.get("rows")
    if not isinstance(rows_value, int):
        fallback_rows = _profile_rows_fallback(profile_phase, schema)
        if isinstance(fallback_rows, int):
            rows_value = fallback_rows

    columns_value = metrics.get("columns")
    if not isinstance(columns_value, int):
        fallback_cols = _profile_columns_fallback(profile_phase, schema)
        if isinstance(fallback_cols, int):
            columns_value = fallback_cols

    summary = {
        "rows": rows_value,
        "columns": columns_value,
        "bytesRead": metrics.get("bytesRead"),
        "datasetCompleteness": metrics.get("datasetCompleteness"),
        "dqScore": metrics.get("dqScore"),
    }
    summary = {key: value for key, value in summary.items() if value is not None}

    links: Dict[str, str] = {}
    if source_input:
        bucket = source_input.get("bucket")
        key = source_input.get("key")
        if bucket and key:
            links["input"] = f"s3://{bucket}/{key}"

    if artifact_bucket:
        links["resultsManifest"] = f"s3://{artifact_bucket}/{manifest_key_for(job_id, artifact_prefix)}"
        links["resultsJson"] = f"s3://{artifact_bucket}/{result_key_for(job_id, artifact_prefix)}"

    payload = {
        "jobId": job_id,
        "analysisVersion": analysis_version,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "schema": schema,
        "links": links,
        "phases": result.phases,
        "metrics": metrics,
        "correlations": result.correlations,
        "outliers": result.outliers,
        "mlInference": result.ml_inference,
        "artifactManifest": result.manifest,
        "phaseArtifactKeys": {
            phase: phase_key_for(job_id, phase, artifact_prefix) for phase in result.phases
        },
    }

    return payload


def emit_parse_debug(
    job_id: str,
    bucket: str,
    profile_phase: Mapping[str, Any],
    *,
    s3_client,
    artifact_prefix: Optional[str] = None,
) -> Optional[str]:
    try:
        debug: Dict[str, Any] = {}
        shape = profile_phase.get("shape")
        if isinstance(shape, Mapping):
            debug["shape"] = {
                key: int(value)
                for key, value in shape.items()
                if isinstance(value, int)
            }
        dtypes = profile_phase.get("dtypes")
        if isinstance(dtypes, Mapping):
            debug["dtypes"] = {str(key): str(value) for key, value in dtypes.items()}
        head = profile_phase.get("head")
        if isinstance(head, list):
            debug["head"] = head[:3]
        columns = profile_phase.get("columnProfiles")
        if isinstance(columns, list):
            debug["columnNames"] = [
                str(column.get("name"))
                for column in columns
                if isinstance(column, Mapping) and column.get("name")
            ][:50]

        key = artifact_key_for(job_id, "results/parse_debug.json", artifact_prefix)
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=_json_bytes(debug),
            ContentType="application/json",
        )
        return key
    except Exception:
        return None


def object_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NotFound", "NoSuchKey"}:
            return False
        raise


__all__ = [
    "ANALYSIS_VERSION",
    "artifact_key_for",
    "build_results_payload",
    "emit_parse_debug",
    "error_key_for",
    "manifest_key_for",
    "object_exists",
    "persist_pipeline_outputs",
    "phase_key_for",
    "result_key_for",
    "summarize_phase_payload",
]
