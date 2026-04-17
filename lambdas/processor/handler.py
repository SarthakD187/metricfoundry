import base64
import json
import logging
import os
import time
from typing import Any, Callable, Dict, Mapping, Optional
from urllib import error as urllib_error
from urllib import request as urllib_request

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from services.common.pipeline import (
    build_results_payload,
    emit_parse_debug,
    object_exists,
    persist_pipeline_outputs,
    error_key_for,
    result_key_for,
    summarize_phase_payload,
)
from services.workers.graph.graph import (
    PHASE_ORDER,
    PipelineResult,
    decode_pipeline_result,
    run_pipeline,
)

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

TABLE_NAME = os.environ["JOBS_TABLE"]
ARTIFACTS_BUCKET = os.environ.get("ARTIFACTS_BUCKET")
WORKER_INVOKE_MODE = os.environ.get("WORKER_INVOKE_MODE", "embedded").strip().lower()
WORKER_ARN = os.environ.get("WORKER_ARN")
WORKER_URL = os.environ.get("WORKER_URL")
WORKER_AUTH_HEADER = os.environ.get("WORKER_AUTH_HEADER")
WORKER_BEARER_TOKEN = os.environ.get("WORKER_BEARER_TOKEN")

STATUS_RUNNING = "RUNNING"
STATUS_SUCCEEDED = "SUCCEEDED"
STATUS_FAILED = "FAILED"


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


def _default_callback(job_id: str) -> Callable[[str, Mapping[str, Any], int, int], None]:
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
                phaseSummary=summarize_phase_payload(payload),
            )
        except (BotoCoreError, ClientError) as exc:  # pragma: no cover
            logger.warning("failed to stream phase status for %s: %s", phase, exc)

    return _callback


def _is_embedded_mode() -> bool:
    return WORKER_INVOKE_MODE in {"", "embedded", "local"}


def _worker_callback_payload(job_id: str) -> Optional[Dict[str, Any]]:
    if _is_embedded_mode():
        return None

    table_name = os.environ.get("TABLE_NAME") or TABLE_NAME
    if table_name:
        return {"mode": "ddb", "table": table_name, "jobId": job_id}
    return {"mode": "log", "jobId": job_id}


def _ensure_bytes(data: Any) -> bytes:
    if isinstance(data, (bytes, bytearray, memoryview)):
        return bytes(data)

    reader = getattr(data, "read", None)
    if callable(reader):
        chunks = bytearray()
        chunk_size = 8 * 1024 * 1024
        while True:
            try:
                piece = reader(chunk_size)
            except TypeError:
                piece = reader()
            if not piece:
                break
            if isinstance(piece, memoryview):
                piece = piece.tobytes()
            elif isinstance(piece, bytearray):
                piece = bytes(piece)
            if not isinstance(piece, (bytes, bytearray)):
                raise TypeError("Stream produced non-bytes payload")
            chunks.extend(piece)
        return bytes(chunks)

    raise TypeError(f"Unsupported body type: {type(data).__name__}")


def _build_worker_event(
    job_id: str,
    source: Mapping[str, Any],
    artifact_prefix: str,
    *,
    body_bytes: Optional[bytes] = None,
    body_s3: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "jobId": job_id,
        "source": dict(source),
        "artifactPrefix": artifact_prefix,
    }
    if body_bytes is not None:
        payload["body"] = base64.b64encode(body_bytes).decode("ascii")
    if body_s3:
        payload["bodyS3"] = dict(body_s3)
    callback_cfg = _worker_callback_payload(job_id)
    if callback_cfg:
        payload["callback"] = callback_cfg
    return payload


def _invoke_worker_lambda(
    job_id: str,
    source: Mapping[str, Any],
    artifact_prefix: str,
    *,
    body_bytes: Optional[bytes] = None,
    body_s3: Optional[Mapping[str, str]] = None,
) -> PipelineResult:
    if not WORKER_ARN:
        raise RuntimeError("WORKER_ARN must be configured for lambda mode")

    if body_bytes is None and body_s3 is None:
        raise ValueError("Worker invocation requires body_bytes or body_s3")

    event = _build_worker_event(
        job_id,
        source,
        artifact_prefix,
        body_bytes=body_bytes,
        body_s3=body_s3,
    )
    logger.info("invoking LangGraph worker Lambda for job %s", job_id)
    response = lambda_client.invoke(
        FunctionName=WORKER_ARN,
        InvocationType="RequestResponse",
        Payload=json.dumps(event).encode("utf-8"),
    )

    payload_stream = response.get("Payload")
    if payload_stream is None:
        raise RuntimeError("Worker Lambda response missing payload stream")

    try:
        raw = payload_stream.read()
    finally:
        closer = getattr(payload_stream, "close", None)
        if callable(closer):
            try:
                closer()
            except (BotoCoreError, ClientError, OSError):
                pass

    if "FunctionError" in response:
        message = raw.decode("utf-8", errors="ignore") if raw else "(no error payload)"
        raise RuntimeError(f"Worker Lambda execution failed: {message}")

    if not raw:
        raise RuntimeError("Worker Lambda returned empty payload")

    try:
        decoded = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise RuntimeError("Worker Lambda returned invalid JSON") from exc

    return decode_pipeline_result(decoded)


def _invoke_worker_http(
    job_id: str,
    source: Mapping[str, Any],
    artifact_prefix: str,
    *,
    body_bytes: Optional[bytes] = None,
    body_s3: Optional[Mapping[str, str]] = None,
) -> PipelineResult:
    if not WORKER_URL:
        raise RuntimeError("WORKER_URL must be configured for http mode")

    if body_bytes is None and body_s3 is None:
        raise ValueError("Worker invocation requires body_bytes or body_s3")

    event = _build_worker_event(
        job_id,
        source,
        artifact_prefix,
        body_bytes=body_bytes,
        body_s3=body_s3,
    )
    data = json.dumps(event).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if WORKER_BEARER_TOKEN:
        headers["Authorization"] = f"Bearer {WORKER_BEARER_TOKEN.strip()}"
    elif WORKER_AUTH_HEADER:
        try:
            name, value = WORKER_AUTH_HEADER.split(":", 1)
        except ValueError as exc:
            raise RuntimeError(
                "WORKER_AUTH_HEADER must be in the format 'Header-Name: value'"
            ) from exc
        headers[name.strip()] = value.strip()

    request = urllib_request.Request(
        WORKER_URL,
        data=data,
        headers=headers,
        method="POST",
    )
    logger.info("invoking LangGraph worker HTTP endpoint for job %s", job_id)
    try:
        with urllib_request.urlopen(request, timeout=900) as response:
            raw = response.read()
    except urllib_error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else ""
        raise RuntimeError(f"Worker HTTP error {exc.code}: {detail}") from exc
    except urllib_error.URLError as exc:  # pragma: no cover - defensive
        raise RuntimeError(f"Worker HTTP invocation failed: {exc}") from exc

    if not raw:
        raise RuntimeError("Worker HTTP endpoint returned empty payload")

    try:
        decoded = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise RuntimeError("Worker HTTP endpoint returned invalid JSON") from exc

    return decode_pipeline_result(decoded)


def _invoke_pipeline(
    job_id: str,
    source: Mapping[str, Any],
    artifact_prefix: str,
    body: Any,
    *,
    body_s3: Optional[Mapping[str, str]] = None,
    on_phase,
) -> PipelineResult:
    mode = WORKER_INVOKE_MODE or "embedded"
    if mode in {"embedded", "", "local"}:
        if body is None:
            raise ValueError("Embedded mode requires a body stream or bytes")
        return run_pipeline(job_id, source, artifact_prefix, body, on_phase=on_phase)

    body_bytes = _ensure_bytes(body) if body is not None else None
    if mode == "lambda":
        return _invoke_worker_lambda(
            job_id,
            source,
            artifact_prefix,
            body_bytes=body_bytes,
            body_s3=body_s3,
        )
    if mode == "http":
        return _invoke_worker_http(
            job_id,
            source,
            artifact_prefix,
            body_bytes=body_bytes,
            body_s3=body_s3,
        )
    raise ValueError(f"Unsupported WORKER_INVOKE_MODE: {mode}")


def _log_phase_progress(job_id: str, phases: Mapping[str, Any]) -> None:
    ordered = [phase for phase in PHASE_ORDER if phase in phases]
    total = len(ordered) or len(PHASE_ORDER)
    for index, phase in enumerate(ordered):
        logger.info(
            "phase %s completed for job %s (%s/%s)",
            phase,
            job_id,
            index + 1,
            total,
        )


def main(event: Mapping[str, Any], _ctx: Any) -> Dict[str, Any]:
    """Run analytics pipeline for a staged object and persist outputs."""
    job_id = event.get("jobId")
    payload_input = event.get("input") or {}
    bucket = payload_input.get("bucket")
    key = payload_input.get("key")

    if not job_id or not bucket or not key:
        raise ValueError("jobId, input.bucket, and input.key are required")

    logger.info("processing job %s using s3://%s/%s", job_id, bucket, key)

    try:
        ddb_upsert_status(job_id, STATUS_RUNNING, inputKey=key, currentPhase=PHASE_ORDER[0], progress=0)
    except (BotoCoreError, ClientError) as exc:
        logger.warning("failed to upsert initial RUNNING status for %s: %s", job_id, exc)

    artifact_prefix_raw = event.get("artifactPrefix") or event.get("artifact_prefix")
    artifact_prefix = (artifact_prefix_raw or f"artifacts/{job_id}").rstrip("/")
    results_key = result_key_for(job_id, artifact_prefix)

    try:
        if ARTIFACTS_BUCKET and object_exists(s3, ARTIFACTS_BUCKET, results_key):
            logger.info(
                "results already exist at s3://%s/%s (idempotent skip)",
                ARTIFACTS_BUCKET,
                results_key,
            )
            return {"ok": True, "jobId": job_id, "resultKey": results_key, "idempotent": True}
    except (BotoCoreError, ClientError) as exc:
        logger.warning("head_object failed for existing results check: %s", exc)

    try:
        source_descriptor = {"bucket": bucket, "key": key}
        body_stream: Optional[Any] = None
        body_s3_descriptor: Optional[Dict[str, str]] = {"bucket": bucket, "key": key}
        callback = None

        if _is_embedded_mode():
            obj = s3.get_object(Bucket=bucket, Key=key)
            body_stream = obj["Body"]
            callback = _default_callback(job_id)
            body_s3_descriptor = None

        try:
            result = _invoke_pipeline(
                job_id,
                source_descriptor,
                artifact_prefix,
                body_stream,
                body_s3=body_s3_descriptor,
                on_phase=callback,
            )
        finally:
            if body_stream is not None:
                closer = getattr(body_stream, "close", None)
                if callable(closer):
                    try:
                        closer()
                    except (BotoCoreError, ClientError, OSError):
                        pass

        if not _is_embedded_mode():
            _log_phase_progress(job_id, result.phases)

        target_bucket = ARTIFACTS_BUCKET or bucket
        artifact_keys = persist_pipeline_outputs(
            job_id,
            target_bucket,
            result,
            s3_client=s3,
            artifact_prefix=artifact_prefix,
        )

        results_payload = build_results_payload(
            job_id,
            result,
            source_input=source_descriptor,
            artifact_bucket=target_bucket,
            artifact_prefix=artifact_prefix,
        )

        # Best-effort parse debug to aid troubleshooting
        try:
            emit_parse_debug(
                job_id,
                target_bucket,
                result.phases.get("profile", {}) or {},
                s3_client=s3,
                artifact_prefix=artifact_prefix,
            )
        except (BotoCoreError, ClientError):
            pass

        s3.put_object(
            Bucket=target_bucket,
            Key=results_key,
            Body=json.dumps(results_payload, indent=2, default=str).encode("utf-8"),
            ContentType="application/json",
        )

        try:
            ddb_upsert_status(
                job_id,
                STATUS_SUCCEEDED,
                resultKey=results_key,
                manifestKey=artifact_keys.get("manifest"),
                completedAt=now_epoch(),
            )
        except (BotoCoreError, ClientError) as exc:
            logger.warning("failed to upsert SUCCEEDED status: %s", exc)

        logger.info("wrote results to s3://%s/%s", target_bucket, results_key)
        return {"ok": True, "jobId": job_id, "resultKey": results_key, "manifestKey": artifact_keys.get("manifest")}

    except (ValueError, TypeError, RuntimeError, BotoCoreError, ClientError, urllib_error.URLError) as e:
        err_txt = f"{type(e).__name__}: {e}"
        logger.error("processing error: %s", err_txt)

        try:
            ddb_upsert_status(job_id, STATUS_FAILED, error=err_txt[:1000])
        except (BotoCoreError, ClientError) as exc:
            logger.warning("failed to upsert FAILED status: %s", exc)

        try:
            target_bucket = ARTIFACTS_BUCKET or bucket
            error_key = error_key_for(job_id, artifact_prefix)
            s3.put_object(
                Bucket=target_bucket,
                Key=error_key,
                Body=json.dumps({"jobId": job_id, "error": err_txt}, indent=2).encode("utf-8"),
                ContentType="application/json",
            )
        except (BotoCoreError, ClientError) as exc:
            logger.warning("failed to write error artifact: %s", exc)

        raise


def lambda_handler(event: Mapping[str, Any], context: Any) -> Dict[str, Any]:
    """API-shaped wrapper for direct Lambda invocation."""
    try:
        return {"statusCode": 200, "body": main(event, context)}
    except ValueError as exc:
        return {"statusCode": 400, "body": {"error": str(exc)}}
    except (BotoCoreError, ClientError, RuntimeError, urllib_error.URLError):
        return {"statusCode": 502, "body": {"error": "Failed to process staged input"}}


def handler(event: Mapping[str, Any], context: Any) -> Dict[str, Any]:
    """Primary Lambda entrypoint used by Step Functions."""
    return main(event, context)
