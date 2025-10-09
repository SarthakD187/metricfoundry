"""Helpers for working with MetricFoundry's analytics pipeline outputs."""
from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - import only for static typing
    from services.workers.graph.graph import PipelineResult
else:  # pragma: no cover - at runtime we treat PipelineResult as ``Any``
    PipelineResult = Any  # type: ignore[misc,assignment]


ANALYSIS_VERSION = "2024.05"


def result_key_for(job_id: str) -> str:
    return f"artifacts/{job_id}/results/results.json"


def manifest_key_for(job_id: str) -> str:
    return f"artifacts/{job_id}/results/manifest.json"


def phase_key_for(job_id: str, phase: str) -> str:
    return f"artifacts/{job_id}/phases/{phase}.json"


def artifact_key_for(job_id: str, relative: str) -> str:
    return f"artifacts/{job_id}/{relative}"


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
) -> Dict[str, str]:
    """Upload phase payloads and generated artifacts to S3.

    Returns a mapping that includes the manifest key and phase artifact keys.
    """

    uploaded: Dict[str, str] = {}
    for phase, payload in result.phases.items():
        key = phase_key_for(job_id, phase)
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=_json_bytes(payload),
            ContentType="application/json",
        )
        uploaded[f"phase:{phase}"] = key

    for relative_key, spec in result.artifact_contents.items():
        key = artifact_key_for(job_id, relative_key)
        body = _bytes_for_artifact(spec)
        content_type = _content_type_for_artifact(relative_key, spec)
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType=content_type,
        )

    manifest_key = manifest_key_for(job_id)
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


def build_results_payload(
    job_id: str,
    result: "PipelineResult",
    *,
    source_input: Optional[Mapping[str, Any]] = None,
    artifact_bucket: Optional[str] = None,
    analysis_version: str = ANALYSIS_VERSION,
) -> Dict[str, Any]:
    metrics = dict(result.metrics)
    summary = {
        "rows": metrics.get("rows"),
        "columns": metrics.get("columns"),
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
        links["resultsManifest"] = f"s3://{artifact_bucket}/{manifest_key_for(job_id)}"
        links["resultsJson"] = f"s3://{artifact_bucket}/{result_key_for(job_id)}"

    profile_phase = result.phases.get("profile", {})
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
        "phaseArtifactKeys": {phase: phase_key_for(job_id, phase) for phase in result.phases},
    }

    return payload


__all__ = [
    "ANALYSIS_VERSION",
    "artifact_key_for",
    "build_results_payload",
    "manifest_key_for",
    "persist_pipeline_outputs",
    "phase_key_for",
    "result_key_for",
    "summarize_phase_payload",
]
