import csv
import io
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional

import boto3
from botocore.exceptions import ClientError

from services.workers.graph.graph import PHASE_ORDER, PipelineResult, run_pipeline

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")

TABLE_NAME = os.environ["JOBS_TABLE"]
ARTIFACTS_BUCKET = os.environ.get("ARTIFACTS_BUCKET")

ANALYSIS_VERSION = "TEST-2025-10-09"


STATUS_RUNNING = "RUNNING"
STATUS_SUCCEEDED = "SUCCEEDED"
STATUS_FAILED = "FAILED"

ANALYSIS_VERSION = "2024.05"


def ddb_table():
    return ddb.Table(TABLE_NAME)


def now_epoch() -> int:
    return int(time.time())


def ddb_upsert_status(job_id: str, status: str, **attrs) -> None:
    expr_names = {"#s": "status"}
    expr_vals = {":s": status, ":u": now_epoch()}
    set_clauses = ["#s = :s", "updatedAt = :u"]

    for k, v in attrs.items():
        placeholder = f":{k}"
        expr_vals[placeholder] = v
        set_clauses.append(f"{k} = {placeholder}")

    ddb_table().update_item(
        Key={"pk": f"job#{job_id}", "sk": "meta"},
        UpdateExpression="SET " + ", ".join(set_clauses),
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_vals,
    )


def result_key_for(job_id: str) -> str:
    return f"artifacts/{job_id}/results/results.json"


def manifest_key_for(job_id: str) -> str:
    return f"artifacts/{job_id}/results/manifest.json"


def phase_key_for(job_id: str, phase: str) -> str:
    return f"artifacts/{job_id}/phases/{phase}.json"


def artifact_key_for(job_id: str, relative: str) -> str:
    return f"artifacts/{job_id}/{relative}"


def object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NotFound", "NoSuchKey"):
            return False
        raise


def _json_bytes(data: Any) -> bytes:
    return json.dumps(data, indent=2, default=str).encode("utf-8")


def _csv_bytes(headers: list[str], rows: list[Mapping[str, Any]]) -> bytes:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)
    writer.writeheader()
    for row in rows:
        writer.writerow({h: row.get(h, "") for h in headers})
    return output.getvalue().encode("utf-8")


def _put_object(bucket: str, key: str, body: bytes, content_type: str) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)


def _persist_artifact(bucket: str, key: str, spec: Mapping[str, Any]) -> None:
    kind = spec.get("kind")
    content_type = spec.get("contentType", "application/octet-stream")
    if kind == "json":
        body = _json_bytes(spec.get("data"))
    elif kind == "text":
        text = spec.get("text", "")
        body = text.encode("utf-8")
    elif kind == "csv":
        headers = spec.get("headers", [])
        rows = spec.get("rows", [])
        body = _csv_bytes(list(headers), list(rows))
    elif kind == "html":
        html = spec.get("html", "")
        if isinstance(html, bytes):
            body = html
        else:
            body = str(html).encode("utf-8")
    elif kind == "image":
        data = spec.get("data", b"")
        if isinstance(data, memoryview):  # pragma: no cover - defensive conversion
            data = data.tobytes()
        if not isinstance(data, (bytes, bytearray)):
            raise ValueError("Image artifact data must be bytes-like")
        body = bytes(data)
    elif kind == "binary":
        data = spec.get("data", b"")
        if isinstance(data, memoryview):
            data = data.tobytes()
        if not isinstance(data, (bytes, bytearray)):
            raise ValueError("Binary artifact data must be bytes-like")
        body = bytes(data)
    else:
        raise ValueError(f"Unsupported artifact kind: {kind}")
    _put_object(bucket, key, body, content_type)


def _summarize_phase_payload(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    summary: Dict[str, Any] = {}
    if "summary" in payload and isinstance(payload["summary"], str):
        summary["summary"] = payload["summary"]
    if "metrics" in payload and isinstance(payload["metrics"], Mapping):
        summary["metrics"] = payload["metrics"]
    if "datasetCompleteness" in payload:
        summary["datasetCompleteness"] = payload["datasetCompleteness"]
    if not summary:
        keys = list(payload.keys())[:5]
        summary["fields"] = keys
    return summary


def _default_callback(job_id: str):
    def _callback(phase: str, payload: Mapping[str, Any], index: int, total: int) -> None:
        progress = int(((index + 1) / total) * 100)
        try:
            ddb_upsert_status(
                job_id,
                STATUS_RUNNING,
                currentPhase=phase,
                phaseIndex=index,
                phaseCount=total,
                progress=progress,
                phaseSummary=_summarize_phase_payload(payload),
            )
        except Exception as exc:  # pragma: no cover - DynamoDB errors shouldn't halt job
            print(f"[ProcessorFn] Warning: failed to stream phase status for {phase}: {exc}")

    return _callback


def _persist_pipeline_outputs(job_id: str, bucket: str, result: PipelineResult) -> Dict[str, str]:
    phase_keys: Dict[str, str] = {}
    for phase, payload in result.phases.items():
        key = phase_key_for(job_id, phase)
        _put_object(bucket, key, _json_bytes(payload), "application/json")
        phase_keys[phase] = key

    for relative, spec in result.artifact_contents.items():
        key = artifact_key_for(job_id, relative)
        _persist_artifact(bucket, key, spec)

    manifest_key = manifest_key_for(job_id)
    _put_object(bucket, manifest_key, _json_bytes(result.manifest), "application/json")

    return {"manifest": manifest_key, **{f"phase:{k}": v for k, v in phase_keys.items()}}


def _build_schema_from_profile(profile_phase: Mapping[str, Any]) -> List[Dict[str, Any]]:
    schema: List[Dict[str, Any]] = []
    columns = profile_phase.get("columnProfiles")
    if not isinstance(columns, list):
        return schema
    for item in columns:
        if not isinstance(item, Mapping):
            continue
        name = item.get("name")
        if name is None:
            continue
        entry: Dict[str, Any] = {"name": str(name)}
        inferred = item.get("inferredType")
        if inferred is not None:
            entry["type"] = inferred
        schema.append(entry)
    return schema


def _emit_parse_debug(job_id: str, bucket: str, profile_phase: Mapping[str, Any]) -> Optional[str]:
    """
    Write a tiny debug artifact so we can see what the parser actually produced.
    Looks for common keys emitted by a profiler node.
    """
    try:
        debug: Dict[str, Any] = {}
        # shape / dtypes
        shape = profile_phase.get("shape")
        if isinstance(shape, Mapping):
            debug["shape"] = {k: int(v) for k, v in shape.items() if isinstance(v, int)}
        dtypes = profile_phase.get("dtypes")
        if isinstance(dtypes, Mapping):
            debug["dtypes"] = {str(k): str(v) for k, v in dtypes.items()}
        # head preview
        head = profile_phase.get("head")
        if isinstance(head, list):
            debug["head"] = head[:3]
        # fallback: column names
        cols = profile_phase.get("columnProfiles")
        if isinstance(cols, list):
            debug["columnNames"] = [
                str(c.get("name")) for c in cols if isinstance(c, Mapping) and c.get("name")
            ][:50]
        key = artifact_key_for(job_id, "results/parse_debug.json")
        _put_object(bucket, key, _json_bytes(debug), "application/json")
        return key
    except Exception:
        return None


def build_results_payload(
    job_id: str,
    result: PipelineResult,
    *,
    source_input: Optional[Mapping[str, Any]] = None,
    artifact_bucket: Optional[str] = None,
) -> Dict[str, Any]:
    metrics = dict(result.metrics)
    profile_phase = result.phases.get("profile", {}) or {}

    # Build schema BEFORE summary so we can use it for robust fallbacks
    schema = _build_schema_from_profile(profile_phase)

    # Best-effort fallbacks for rows/columns (avoid numeric-only mistakes)
    def _rows_fallback() -> Optional[int]:
        # common shapes produced by profilers
        for k in ("rowCount", "rows", "nRows", "shapeRows"):
            v = profile_phase.get(k)
            if isinstance(v, int) and v >= 0:
                return v
        # sometimes embedded under "shape" or "metrics"
        shp = profile_phase.get("shape")
        if isinstance(shp, Mapping) and isinstance(shp.get("rows"), int):
            return shp["rows"]
        m = profile_phase.get("metrics")
        if isinstance(m, Mapping) and isinstance(m.get("rows"), int):
            return m["rows"]
        return None

    def _cols_fallback() -> Optional[int]:
        # most reliable: the length of the built schema
        if schema:
            return len(schema)
        for k in ("columnCount", "columns", "nCols", "shapeCols"):
            v = profile_phase.get(k)
            if isinstance(v, int) and v >= 0:
                return v
        shp = profile_phase.get("shape")
        if isinstance(shp, Mapping) and isinstance(shp.get("columns"), int):
            return shp["columns"]
        cols = profile_phase.get("columnProfiles")
        if isinstance(cols, list):
            return len(cols)
        return None

    rows_val = metrics.get("rows")
    cols_val = metrics.get("columns")
    if not isinstance(rows_val, int):
        fb = _rows_fallback()
        if isinstance(fb, int):
            rows_val = fb
    if not isinstance(cols_val, int):
        fb = _cols_fallback()
        if isinstance(fb, int):
            cols_val = fb

    summary = {
        "rows": rows_val,
        "columns": cols_val,
        "bytesRead": metrics.get("bytesRead"),
        "datasetCompleteness": metrics.get("datasetCompleteness"),
        "dqScore": metrics.get("dqScore"),
    }
    summary = {key: value for key, value in summary.items() if value is not None}

    links: Dict[str, str] = {}
    if source_input:
        src_bucket = source_input.get("bucket")
        src_key = source_input.get("key")
        if src_bucket and src_key:
            links["input"] = f"s3://{src_bucket}/{src_key}"

    if artifact_bucket:
        manifest_uri = f"s3://{artifact_bucket}/{manifest_key_for(job_id)}"
        results_uri = f"s3://{artifact_bucket}/{result_key_for(job_id)}"
        links["resultsManifest"] = manifest_uri
        links["resultsJson"] = results_uri

    payload = {
        "jobId": job_id,
        "analysisVersion": ANALYSIS_VERSION,
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


def main(event, _ctx):
    job_id = event.get("jobId")
    payload_input = event.get("input") or {}
    bucket = payload_input.get("bucket")
    key = payload_input.get("key")

    if not job_id or not bucket or not key:
        raise ValueError("jobId, input.bucket, and input.key are required")

    print(f"[ProcessorFn] Processing job {job_id} using s3://{bucket}/{key}")

    try:
        ddb_upsert_status(job_id, STATUS_RUNNING, inputKey=key, currentPhase=PHASE_ORDER[0], progress=0)
    except Exception as e:
        print(f"[ProcessorFn] Warning: failed to upsert initial RUNNING status: {e}")

    artifact_prefix = f"artifacts/{job_id}"
    results_key = result_key_for(job_id)

    try:
        if ARTIFACTS_BUCKET and object_exists(ARTIFACTS_BUCKET, results_key):
            print(
                f"[ProcessorFn] Results already exist at s3://{ARTIFACTS_BUCKET}/{results_key} (idempotent skip)."
            )
            return {"ok": True, "jobId": job_id, "resultKey": results_key, "idempotent": True}
    except Exception as e:
        print(f"[ProcessorFn] Warning: head_object failed for existing results check: {e}")

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"]
        try:
            result = run_pipeline(
                job_id,
                {"bucket": bucket, "key": key},
                body,
                artifact_prefix=artifact_prefix,
                on_phase=_default_callback(job_id),
            )
        finally:
            closer = getattr(body, "close", None)
            if callable(closer):
                try:
                    closer()
                except Exception:
                    pass

        target_bucket = ARTIFACTS_BUCKET or bucket
        artifact_keys = _persist_pipeline_outputs(job_id, target_bucket, result)

        results_payload = build_results_payload(
            job_id,
            result,
            source_input=payload_input,
            artifact_bucket=target_bucket,
        )

        # Best-effort parse debug to aid troubleshooting
        try:
            _emit_parse_debug(job_id, target_bucket, result.phases.get("profile", {}) or {})
        except Exception:
            pass

        _put_object(target_bucket, results_key, _json_bytes(results_payload), "application/json")

        try:
            ddb_upsert_status(
                job_id,
                STATUS_SUCCEEDED,
                resultKey=results_key,
                manifestKey=artifact_keys.get("manifest"),
                completedAt=now_epoch(),
            )
        except Exception as e:
            print(f"[ProcessorFn] Warning: failed to upsert SUCCEEDED status: {e}")

        print(f"[ProcessorFn] Wrote results to s3://{target_bucket}/{results_key}")
        return {"ok": True, "jobId": job_id, "resultKey": results_key, "manifestKey": artifact_keys.get("manifest")}

    except Exception as e:
        err_txt = f"{type(e).__name__}: {e}"
        print(f"[ProcessorFn] ERROR: {err_txt}")

        try:
            ddb_upsert_status(job_id, STATUS_FAILED, error=err_txt[:1000])
        except Exception as e2:
            print(f"[ProcessorFn] Warning: failed to upsert FAILED status: {e2}")

        try:
            target_bucket = ARTIFACTS_BUCKET or bucket
            error_key = results_key.replace("results.json", "error.json")
            _put_object(
                target_bucket,
                error_key,
                _json_bytes({"jobId": job_id, "error": err_txt}),
                "application/json",
            )
        except Exception as e3:
            print(f"[ProcessorFn] Warning: failed to write error artifact: {e3}")

        raise
