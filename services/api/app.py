# services/api/app.py
from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
from pydantic import BaseModel

from services.common.pipeline import (
    build_results_payload,
    persist_pipeline_outputs,
    result_key_for,
    summarize_phase_payload,
)

# ---- Env ----
BUCKET_NAME = os.environ["BUCKET_NAME"]          # artifacts bucket
TABLE_NAME  = os.environ["TABLE_NAME"]           # DynamoDB table
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]

# ---- Logging & Observability ----
logger = logging.getLogger("metricfoundry.api")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)

METRICS_NAMESPACE = os.environ.get("METRICS_NAMESPACE", "MetricFoundry/API")

# ---- AWS ----
s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")
table = ddb.Table(TABLE_NAME)
sfn = boto3.client("stepfunctions")
cloudwatch = boto3.client("cloudwatch")

# ---- App ----
app = FastAPI(title="MetricFoundry API")

# --- CORS for local dashboard ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
    allow_credentials=False,
)

# ---- Models ----
class CreateJob(BaseModel):
    """Request payload for job creation."""
    source_type: str
    s3_path: str | None = None
    source_config: Dict[str, object] | None = None


def _clean_config(config: Dict[str, object] | None) -> Dict[str, object]:
    return {key: value for key, value in (config or {}).items() if value is not None}


def _build_sql_connection(config: Dict[str, object], *, source_label: str) -> Dict[str, object]:
    """Normalise SQL connection metadata for databases and warehouses."""
    connection_meta = config.get("connection")
    secret_field = config.get("secretField")

    if connection_meta is not None:
        if not isinstance(connection_meta, dict):
            raise HTTPException(status_code=400, detail=f"{source_label} connection must be a JSON object")
        cleaned = _clean_config(connection_meta)
        conn_type = str(cleaned.get("type") or "").lower()

        # Infer type when absent for backwards compatibility.
        if not conn_type:
            if "secretArn" in cleaned or "arn" in cleaned:
                conn_type = "secretsmanager"
            elif "parameterName" in cleaned or "name" in cleaned:
                conn_type = "parameterstore"
            elif "url" in cleaned:
                conn_type = "inline"

        if conn_type in {"inline", "url"}:
            url = cleaned.get("url")
            if not url:
                raise HTTPException(status_code=400, detail=f"{source_label} connection.url is required for inline connections")
            return {"type": "inline", "url": url}

        if conn_type in {"secretsmanager", "secret", "secrets"}:
            secret_arn = cleaned.get("secretArn") or cleaned.get("arn")
            if not secret_arn:
                raise HTTPException(status_code=400, detail=f"{source_label} connection.secretArn is required for secrets manager connections")
            payload: Dict[str, object] = {"type": "secretsManager", "secretArn": secret_arn}
            field = cleaned.get("secretField") or secret_field
            if field:
                payload["secretField"] = field
            return payload

        if conn_type in {"parameterstore", "ssm", "systemsmanager"}:
            parameter_name = cleaned.get("parameterName") or cleaned.get("name")
            if not parameter_name:
                raise HTTPException(status_code=400, detail=f"{source_label} connection.parameterName is required for parameter store connections")
            payload = {"type": "parameterStore", "parameterName": parameter_name}
            field = cleaned.get("secretField") or secret_field
            if field:
                payload["secretField"] = field
            return payload

        raise HTTPException(status_code=400, detail=f"Unsupported {source_label} connection.type")

    url = config.get("url")
    secret_arn = config.get("secretArn")
    parameter_name = config.get("parameterName")

    provided_connections = [value for value in [url, secret_arn, parameter_name] if value]
    if not provided_connections:
        raise HTTPException(status_code=400, detail=f"{source_label} jobs require a connection reference (url, secretArn, or parameterName)")
    if len(provided_connections) > 1:
        raise HTTPException(status_code=400, detail=f"{source_label} jobs must specify only one connection reference")

    if url:
        return {"type": "inline", "url": url}
    if secret_arn:
        payload = {"type": "secretsManager", "secretArn": secret_arn}
    else:
        payload = {"type": "parameterStore", "parameterName": parameter_name}
    if secret_field:
        payload["secretField"] = secret_field
    return payload


_FILENAME_PATTERN = re.compile(r"[^A-Za-z0-9._-]")


def _sanitize_upload_filename(filename: str) -> str:
    base = os.path.basename(filename)
    candidate = _FILENAME_PATTERN.sub("_", base.strip())[:128]
    candidate = candidate.lstrip("._")
    while ".." in candidate:
        candidate = candidate.replace("..", ".")
    if not candidate or set(candidate) == {"."}:
        return "upload.csv"
    return candidate


def _build_source(body: CreateJob, job_id: str) -> Tuple[dict, Optional[str]]:
    """Return (source_metadata, upload_url?)."""
    source_type = body.source_type
    config = _clean_config(body.source_config)

    if source_type == "upload":
        filename_raw = str(config.get("filename") or config.get("fileName") or "upload.csv")
        filename = _sanitize_upload_filename(filename_raw)
        key = f"artifacts/{job_id}/input/{filename}"
        content_type = config.get("contentType") or config.get("content_type")
        params: Dict[str, Any] = {"Bucket": BUCKET_NAME, "Key": key}
        if content_type:
            params["ContentType"] = str(content_type)
        upload_url = s3.generate_presigned_url(
            ClientMethod="put_object",
            Params=params,
            ExpiresIn=900,
        )
        source = {"type": "upload", "bucket": BUCKET_NAME, "key": key, "filename": filename}
        if content_type:
            source["contentType"] = str(content_type)
        return source, upload_url

    if source_type == "s3":
        path = body.s3_path or config.get("uri") or config.get("path")
        if not path or not path.startswith("s3://"):
            raise HTTPException(status_code=400, detail="s3_path must be like s3://bucket/key")
        return {"type": "s3", "uri": path}, None

    if source_type in {"http", "https"}:
        url = config.get("url") or body.s3_path
        if not url or not url.startswith("http"):
            raise HTTPException(status_code=400, detail="source_config.url is required for http jobs")
        method = (config.get("method") or "GET").upper()
        if method not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
            raise HTTPException(status_code=400, detail="Unsupported HTTP method")
        headers = config.get("headers") or {}
        if not isinstance(headers, dict):
            raise HTTPException(status_code=400, detail="headers must be a JSON object")
        source = {
            "type": "http",
            "protocol": source_type,
            "url": url,
            "method": method,
            "headers": headers,
            "body": config.get("body"),
            "filename": config.get("filename"),
            "timeout": config.get("timeout"),
        }
        return _clean_config(source), None

    if source_type == "database":
        query = config.get("query")
        if not query:
            raise HTTPException(status_code=400, detail="database jobs require query in source_config")
        connection = _build_sql_connection(config, source_label="database")
        source = {
            "type": "database",
            "query": query,
            "params": config.get("params"),
            "filename": config.get("filename"),
            "format": config.get("format", "csv"),
            "connection": connection,
        }
        return _clean_config(source), None

    if source_type == "warehouse":
        warehouse_type = (config.get("warehouseType") or config.get("type") or "").lower()
        if warehouse_type not in {"redshift", "snowflake", "bigquery", "databricks"}:
            raise HTTPException(status_code=400, detail="warehouseType must be redshift, snowflake, bigquery, or databricks")
        query = config.get("query")
        if not query:
            raise HTTPException(status_code=400, detail="warehouse jobs require query in source_config")
        connection = _build_sql_connection(config, source_label="warehouse")
        source = {
            "type": "warehouse",
            "warehouseType": warehouse_type,
            "query": query,
            "params": config.get("params"),
            "filename": config.get("filename"),
            "format": config.get("format", "csv"),
            "connection": connection,
        }
        return _clean_config(source), None

    raise HTTPException(status_code=400, detail="Unsupported source_type")

# ---- Helpers ----
def record_metric(name: str, value: float = 1, unit: str = "Count", dimensions: Optional[Dict[str, str]] | None = None) -> None:
    metric = {"MetricName": name, "Value": value, "Unit": unit}
    if dimensions:
        metric["Dimensions"] = [{"Name": key, "Value": val} for key, val in dimensions.items()]
    try:
        cloudwatch.put_metric_data(Namespace=METRICS_NAMESPACE, MetricData=[metric])
    except Exception as exc:  # pragma: no cover
        logger.debug("failed to emit metric", extra={"metric": name, "error": str(exc)})


def epoch() -> int:
    return int(time.time())


def ddb_put_job(job_id: str, status: str, source: dict, created: int):
    item = {
        "pk": f"job#{job_id}",
        "sk": "meta",
        "status": status,
        "createdAt": created,
        "updatedAt": created,
        "source": source,
    }
    table.put_item(Item=item, ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)")
    return item


def ddb_update_status(job_id: str, status: str, **attrs):
    expr_names = {"#s": "status", "#u": "updatedAt"}
    expr_vals = {":s": status, ":u": epoch()}
    set_parts = ["#s = :s", "#u = :u"]

    for key, value in attrs.items():
        placeholder = f":{key}"
        expr_vals[placeholder] = value
        expr_name = f"#{key}"
        expr_names[expr_name] = key
        set_parts.append(f"{expr_name} = {placeholder}")

    table.update_item(
        Key={"pk": f"job#{job_id}", "sk": "meta"},
        UpdateExpression="SET " + ", ".join(set_parts),
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_vals,
    )


def ensure_job(job_id: str) -> dict:
    res = table.get_item(Key={"pk": f"job#{job_id}", "sk": "meta"})
    item = res.get("Item")
    if not item:
        raise HTTPException(status_code=404, detail="Job not found")
    return item


def serialize_objects(objects: Iterable[dict]) -> List[dict]:
    serialized: List[dict] = []
    for obj in objects:
        modified = obj.get("LastModified")
        if isinstance(modified, datetime):
            if modified.tzinfo is None:
                modified = modified.replace(tzinfo=timezone.utc)
            modified_str = modified.astimezone(timezone.utc).isoformat()
        else:
            modified_str = None if modified is None else str(modified)
        serialized.append(
            {
                "key": obj.get("Key"),
                "size": obj.get("Size"),
                "lastModified": modified_str,
                "etag": obj.get("ETag"),
                "storageClass": obj.get("StorageClass"),
            }
        )
    return serialized


def _normalize_s3_path(value: str) -> str:
    """Return a canonical representation of an S3 key or prefix."""
    if value == "":
        return value
    segments: List[str] = []
    for segment in value.split("/"):
        if segment in ("", "."):
            continue
        if segment == "..":
            if segments:
                segments.pop()
            continue
        segments.append(segment)
    normalised = "/".join(segments)
    if value.endswith("/") and normalised:
        normalised += "/"
    return normalised


def validate_prefix(job_id: str, prefix: Optional[str], *, base: str) -> str:
    target = prefix or base
    base_normalised = _normalize_s3_path(base)
    target_normalised = _normalize_s3_path(target)
    base_compare = base_normalised.rstrip("/")
    target_compare = target_normalised.rstrip("/")
    if target_compare != base_compare and not target_compare.startswith(f"{base_compare}/"):
        raise HTTPException(status_code=400, detail="prefix must target this job's artifacts")
    if target_compare == base_compare:
        return base_normalised
    return target_normalised


def ddb_item_to_job(job_id: str, item: dict) -> dict:
    return {
        "jobId": job_id,
        "status": item.get("status"),
        "createdAt": item.get("createdAt"),
        "updatedAt": item.get("updatedAt"),
        "resultKey": item.get("resultKey"),
        "source": item.get("source"),
        "error": item.get("error"),
    }


# ---- Routes ----
@app.get("/health")
def health():
    return {"ok": True}


@app.post("/jobs")
def create_job(body: CreateJob):
    job_id = str(uuid.uuid4())
    now = epoch()
    dimensions = {"SourceType": body.source_type}

    try:
        source, upload_url = _build_source(body, job_id)
    except HTTPException:
        record_metric("JobValidationError", dimensions=dimensions)
        raise
    except Exception as exc:  # pragma: no cover
        record_metric("JobValidationError", dimensions=dimensions)
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    logger.info("creating job", extra={"job_id": job_id, **dimensions})

    try:
        ddb_put_job(job_id, status="CREATED", source=source, created=now)
    except ClientError as e:
        logger.exception("failed to persist job", extra={"job_id": job_id})
        record_metric("JobPersistenceFailed", dimensions=dimensions)
        raise HTTPException(status_code=502, detail="Unable to persist job") from e

    execution_input = json.dumps({"jobId": job_id})
    execution_name = f"job-{job_id}".replace("/", "-")

    try:
        # Start the workflow (can be disabled in local dev if desired)
        sfn.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=execution_name,
            input=execution_input,
        )
        ddb_update_status(job_id, "QUEUED")
        record_metric("JobQueued", dimensions=dimensions)
    except ClientError as e:
        error = e.response.get("Error", {})
        message = error.get("Message") or str(e)
        ddb_update_status(job_id, "FAILED", error=message[:1000])
        record_metric("JobWorkflowStartFailed", dimensions=dimensions)
        logger.exception("failed to start workflow", extra={"job_id": job_id})
        raise HTTPException(status_code=502, detail="Failed to start job workflow") from e
    except Exception as e:
        ddb_update_status(job_id, "FAILED", error=str(e)[:1000])
        record_metric("JobWorkflowStartFailed", dimensions=dimensions)
        logger.exception("unexpected error starting workflow", extra={"job_id": job_id})
        raise HTTPException(status_code=502, detail="Failed to start job workflow") from e

    record_metric("JobCreated", dimensions=dimensions)
    logger.info("job queued", extra={"job_id": job_id, **dimensions})
    return {"jobId": job_id, "uploadUrl": upload_url, "source": source}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    item = ensure_job(job_id)
    return ddb_item_to_job(job_id, item)


@app.get("/jobs/{job_id}/manifest")
def get_job_manifest(job_id: str):
    ensure_job(job_id)
    key = f"artifacts/{job_id}/manifest.json"
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NotFound", "NoSuchKey"):
            raise HTTPException(status_code=404, detail="Manifest not available")
        raise HTTPException(status_code=502, detail="Unable to load manifest")

    try:
        manifest = json.loads(obj["Body"].read().decode("utf-8"))
    except json.JSONDecodeError as exc:
        logger.exception("manifest is not valid JSON", extra={"job_id": job_id})
        raise HTTPException(status_code=502, detail="Manifest is not valid JSON") from exc

    return {
        "jobId": job_id,
        "manifest": manifest,
        "etag": obj.get("ETag"),
        "contentLength": obj.get("ContentLength"),
    }


@app.get("/jobs/{job_id}/artifacts")
def list_job_artifacts(
    job_id: str,
    prefix: Optional[str] = Query(default=None, description="Prefix filter within the job's artifact directory"),
    continuation_token: Optional[str] = Query(default=None, alias="continuationToken"),
    page_size: int = Query(default=100, alias="pageSize", ge=1, le=1000),
    delimiter: Optional[str] = Query(default="/", description="Delimiter used for common prefixes; empty to disable"),
):
    ensure_job(job_id)
    base_prefix = f"artifacts/{job_id}/"
    target_prefix = validate_prefix(job_id, prefix, base=base_prefix)

    kwargs = {
        "Bucket": BUCKET_NAME,
        "Prefix": target_prefix,
        "MaxKeys": page_size,
    }
    if continuation_token:
        kwargs["ContinuationToken"] = continuation_token
    if delimiter:
        kwargs["Delimiter"] = delimiter

    response = s3.list_objects_v2(**kwargs)
    objects = serialize_objects(response.get("Contents", []))
    common = [item.get("Prefix") for item in response.get("CommonPrefixes", [])]

    return {
        "jobId": job_id,
        "prefix": target_prefix,
        "objects": objects,
        "commonPrefixes": common,
        "isTruncated": response.get("IsTruncated", False),
        "nextToken": response.get("NextContinuationToken"),
    }


@app.get("/jobs/{job_id}/results/files")
def list_job_result_files(
    job_id: str,
    continuation_token: Optional[str] = Query(default=None, alias="continuationToken"),
    page_size: int = Query(default=100, alias="pageSize", ge=1, le=1000),
):
    ensure_job(job_id)
    prefix = f"artifacts/{job_id}/results/"
    kwargs = {
        "Bucket": BUCKET_NAME,
        "Prefix": prefix,
        "MaxKeys": page_size,
        "Delimiter": "/",
    }
    if continuation_token:
        kwargs["ContinuationToken"] = continuation_token

    response = s3.list_objects_v2(**kwargs)
    contents = response.get("Contents", [])
    if not contents and not response.get("CommonPrefixes"):
        raise HTTPException(status_code=404, detail="No result files available")

    return {
        "jobId": job_id,
        "prefix": prefix,
        "objects": serialize_objects(contents),
        "commonPrefixes": [item.get("Prefix") for item in response.get("CommonPrefixes", [])],
        "isTruncated": response.get("IsTruncated", False),
        "nextToken": response.get("NextContinuationToken"),
    }


@app.get("/jobs/{job_id}/results")
def get_job_results(job_id: str, path: Optional[str] = Query(default=None, description="Relative path within the job's results directory")):
    """Return a presigned download URL for a results file.
    Defaults to artifacts/{job_id}/results/results.json
    """
    ensure_job(job_id)
    prefix = _normalize_s3_path(f"artifacts/{job_id}/results/")
    relative = path.lstrip("/") if path else "results.json"
    prefix_no_trailing = prefix[:-1] if prefix.endswith("/") else prefix
    key = _normalize_s3_path(f"{prefix_no_trailing}/{relative}")

    if not key.startswith(prefix):
        raise HTTPException(status_code=400, detail="path must resolve within the results directory")

    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NotFound", "NoSuchKey"):
            raise HTTPException(status_code=404, detail="Results not available")
        raise HTTPException(status_code=502, detail="Unable to verify results availability")

    url = s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": BUCKET_NAME, "Key": key},
        ExpiresIn=600,
    )
    return {"jobId": job_id, "downloadUrl": url, "key": key}


@app.get("/jobs/{job_id}/download")
def presign_any(job_id: str, key: str = Query(..., description="Full S3 key inside artifacts/{job_id}/...")):
    ensure_job(job_id)
    base_prefix = f"artifacts/{job_id}/"
    norm = _normalize_s3_path(key)
    if not norm.startswith(base_prefix):
        raise HTTPException(status_code=400, detail="key must be within artifacts/{job_id}/")
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=norm)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NotFound", "NoSuchKey"):
            raise HTTPException(status_code=404, detail="Object not found")
        raise HTTPException(status_code=502, detail="Unable to verify object")
    url = s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": BUCKET_NAME, "Key": norm},
        ExpiresIn=600,
    )
    return {"jobId": job_id, "downloadUrl": url, "key": norm}


# --- Dev/manual processing endpoint to emit the full artifact layout ---
@app.post("/jobs/{job_id}/process")
def process_now(job_id: str):
    """Run the LangGraph analytics pipeline immediately for the given job."""

    job = ensure_job(job_id)
    source = job.get("source") or {}
    bucket = source.get("bucket")
    key = source.get("key")
    if not (bucket and key):
        raise HTTPException(status_code=400, detail="job has no source.bucket/key")

    try:
        from services.workers.graph.graph import PHASE_ORDER, run_pipeline  # type: ignore import
    except Exception as exc:  # pragma: no cover - dependency missing in unusual setups
        logger.exception("analytics pipeline unavailable", extra={"job_id": job_id})
        raise HTTPException(status_code=500, detail="Analytics pipeline is not available") from exc

    dimensions = {"SourceType": source.get("type", "unknown")}
    result_key = result_key_for(job_id)

    try:
        phase_count = len(PHASE_ORDER)
        try:
            ddb_update_status(
                job_id,
                "RUNNING",
                currentPhase=PHASE_ORDER[0] if PHASE_ORDER else "ingest",
                phaseIndex=0,
                phaseCount=phase_count,
                progress=0,
            )
        except Exception as exc:
            logger.warning("failed to mark job running", extra={"job_id": job_id, "error": str(exc)})

        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in ("404", "NotFound", "NoSuchKey"):
                raise HTTPException(status_code=404, detail="Input object not found")
            raise HTTPException(status_code=502, detail="Unable to read job input") from exc

        body = obj.get("Body")
        if body is None:
            raise HTTPException(status_code=502, detail="Job input body missing")

        def _on_phase(phase: str, payload: Mapping[str, Any], index: int, total: int) -> None:
            progress = int(((index + 1) / max(total, 1)) * 100)
            summary = summarize_phase_payload(payload)
            update: Dict[str, Any] = {
                "currentPhase": phase,
                "phaseIndex": index,
                "phaseCount": total,
                "progress": progress,
            }
            if summary:
                update["phaseSummary"] = summary
            try:
                ddb_update_status(job_id, "RUNNING", **update)
            except Exception as exc:
                logger.warning(
                    "failed to stream phase update",
                    extra={"job_id": job_id, "phase": phase, "error": str(exc)},
                )

        try:
            result = run_pipeline(
                job_id,
                {"bucket": bucket, "key": key},
                body,
                artifact_prefix=f"artifacts/{job_id}",
                on_phase=_on_phase,
            )
        finally:
            closer = getattr(body, "close", None)
            if callable(closer):
                try:
                    closer()
                except Exception:
                    pass

        artifact_keys = persist_pipeline_outputs(job_id, BUCKET_NAME, result, s3_client=s3)
        results_payload = build_results_payload(
            job_id,
            result,
            source_input={"bucket": bucket, "key": key},
            artifact_bucket=BUCKET_NAME,
        )
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=result_key,
            Body=json.dumps(results_payload, indent=2, default=str).encode("utf-8"),
            ContentType="application/json",
        )

        try:
            ddb_update_status(
                job_id,
                "SUCCEEDED",
                resultKey=result_key,
                manifestKey=artifact_keys.get("manifest"),
                completedAt=epoch(),
            )
        except Exception as exc:
            logger.warning("failed to persist SUCCEEDED status", extra={"job_id": job_id, "error": str(exc)})

        record_metric("JobProcessed", dimensions=dimensions)
        return {
            "jobId": job_id,
            "resultKey": result_key,
            "manifestKey": artifact_keys.get("manifest"),
            "results": results_payload,
        }

    except HTTPException:
        record_metric("JobProcessingFailed", dimensions=dimensions)
        raise
    except Exception as exc:
        logger.exception("analytics pipeline failed", extra={"job_id": job_id})
        err_txt = f"{type(exc).__name__}: {exc}"
        try:
            ddb_update_status(job_id, "FAILED", error=err_txt[:1000])
        except Exception as update_exc:
            logger.warning("failed to persist FAILED status", extra={"job_id": job_id, "error": str(update_exc)})

        try:
            error_key = result_key.replace("results.json", "error.json")
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=error_key,
                Body=json.dumps({"jobId": job_id, "error": err_txt}, indent=2, default=str).encode("utf-8"),
                ContentType="application/json",
            )
        except Exception as write_exc:
            logger.warning("failed to write error artifact", extra={"job_id": job_id, "error": str(write_exc)})

        record_metric("JobProcessingFailed", dimensions=dimensions)
        raise HTTPException(status_code=500, detail="Failed to process job") from exc

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    try:
        response = await call_next(request)
        duration_ms = int((time.time() - start) * 1000)
        logger.info(
            "request completed",
            extra={
                "path": request.url.path,
                "method": request.method,
                "status_code": response.status_code,
                "duration_ms": duration_ms,
                "request_id": request_id,
            },
        )
        response.headers.setdefault("x-request-id", request_id)
        return response
    except Exception:
        duration_ms = int((time.time() - start) * 1000)
        logger.exception(
            "request failed",
            extra={
                "path": request.url.path,
                "method": request.method,
                "duration_ms": duration_ms,
                "request_id": request_id,
            },
        )
        raise


# ✅ GLOBAL Lambda handler (must be at module scope)
handler = Mangum(app)
