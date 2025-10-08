# services/api/app.py
from __future__ import annotations

import base64
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from functools import lru_cache
from typing import Dict, Iterable, List, Optional

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from mangum import Mangum
from pydantic import BaseModel

# ---- Env ----
BUCKET_NAME = os.environ["BUCKET_NAME"]          # artifacts bucket
TABLE_NAME  = os.environ["TABLE_NAME"]           # DynamoDB table
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
API_KEY_SECRET_ARN = os.environ.get("API_KEY_SECRET_ARN")
STATIC_API_KEYS = [key.strip() for key in os.environ.get("API_KEYS", "").split(",") if key.strip()]

def _load_rate_limit_setting(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, default))
    except (TypeError, ValueError):  # pragma: no cover - defensive parsing
        return default
    return max(0, value)


RATE_LIMIT_CONFIG = {
    "per_minute": _load_rate_limit_setting("RATE_LIMIT_PER_MINUTE", 120),
    "burst": _load_rate_limit_setting("RATE_LIMIT_BURST", _load_rate_limit_setting("RATE_LIMIT_PER_MINUTE", 120)),
}
if RATE_LIMIT_CONFIG["burst"] == 0 and RATE_LIMIT_CONFIG["per_minute"] > 0:
    RATE_LIMIT_CONFIG["burst"] = RATE_LIMIT_CONFIG["per_minute"]

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
secrets_manager = boto3.client("secretsmanager")

app = FastAPI(title="MetricFoundry API")

_RATE_LIMIT_STATE: dict[str, dict[str, float]] = {}


def reset_rate_limiter():
    """Utility used in tests to reset the in-memory limiter state."""

    _RATE_LIMIT_STATE.clear()

# ---- Models ----
class CreateJob(BaseModel):
    """Request payload for job creation.

    ``source_type`` determines which connector will be used.  Historically the
    API only accepted ``upload`` and ``s3`` jobs – we now expose a ``source_config``
    payload so that richer connectors (HTTP endpoints, databases, data
    warehouses, etc.) can pass the metadata they need without changing the core
    contract for existing clients.
    """

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


def _build_source(body: CreateJob, job_id: str) -> tuple[dict, Optional[str]]:
    """Normalise the user's request into an internal ``source`` descriptor.

    Returns a tuple of ``(source_metadata, upload_url)`` where the latter is only
    populated for ``upload`` workflows that expect the client to PUT data
    directly to S3.
    """

    source_type = body.source_type
    config = _clean_config(body.source_config)

    if source_type == "upload":
        key = f"artifacts/{job_id}/input/upload.csv"
        upload_url = s3.generate_presigned_url(
            ClientMethod="put_object",
            Params={"Bucket": BUCKET_NAME, "Key": key, "ContentType": "text/csv"},
            ExpiresIn=900,
        )
        source = {"type": "upload", "bucket": BUCKET_NAME, "key": key}
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
    metric = {
        "MetricName": name,
        "Value": value,
        "Unit": unit,
    }
    if dimensions:
        metric["Dimensions"] = [{"Name": key, "Value": val} for key, val in dimensions.items()]

    try:
        cloudwatch.put_metric_data(Namespace=METRICS_NAMESPACE, MetricData=[metric])
    except Exception as exc:  # pragma: no cover - observability must never block requests
        logger.debug("failed to emit metric", extra={"metric": name, "error": str(exc)})


def epoch() -> int:
    return int(time.time())


@lru_cache(maxsize=1)
def load_api_keys() -> frozenset[str]:
    keys = {key for key in STATIC_API_KEYS}

    if API_KEY_SECRET_ARN:
        try:
            secret_value = secrets_manager.get_secret_value(SecretId=API_KEY_SECRET_ARN)
        except ClientError as exc:
            logger.error("failed to load API key secret", extra={"error": str(exc)})
        else:
            secret_string = secret_value.get("SecretString")
            if secret_string:
                try:
                    payload = json.loads(secret_string)
                except json.JSONDecodeError:
                    keys.add(secret_string)
                else:
                    if isinstance(payload, dict):
                        candidate = payload.get("apiKey")
                        if isinstance(candidate, str):
                            keys.add(candidate)
                        else:
                            for value in payload.values():
                                if isinstance(value, str):
                                    keys.add(value)
                    elif isinstance(payload, list):
                        keys.update(str(item) for item in payload if isinstance(item, str))
                    elif isinstance(payload, str):
                        keys.add(payload)
            secret_binary = secret_value.get("SecretBinary")
            if secret_binary:
                try:
                    decoded = base64.b64decode(secret_binary).decode("utf-8")
                    keys.add(decoded)
                except Exception as exc:  # pragma: no cover - defensive guard for decoding issues
                    logger.warning("failed to decode binary API key secret", extra={"error": str(exc)})

    return frozenset(keys)


def _rate_limit_allow(api_key: str) -> bool:
    if RATE_LIMIT_CONFIG["per_minute"] <= 0:
        return True

    now = time.time()
    state = _RATE_LIMIT_STATE.get(api_key, {"tokens": RATE_LIMIT_CONFIG["burst"], "updated": now})

    elapsed = now - state["updated"]
    refill_rate = RATE_LIMIT_CONFIG["per_minute"] / 60 if RATE_LIMIT_CONFIG["per_minute"] > 0 else 0
    tokens = min(RATE_LIMIT_CONFIG["burst"], state["tokens"] + elapsed * refill_rate)

    if tokens < 1:
        _RATE_LIMIT_STATE[api_key] = {"tokens": tokens, "updated": now}
        return False

    _RATE_LIMIT_STATE[api_key] = {"tokens": tokens - 1, "updated": now}
    return True


def ddb_put_job(job_id: str, status: str, source: dict, created: int):
    item = {
        "pk": f"job#{job_id}",
        "sk": "meta",
        "status": status,      # CREATED -> (worker sets RUNNING/SUCCEEDED/FAILED)
        "createdAt": created,
        "updatedAt": created,
        "source": source,
    }
    table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)"
    )
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
    """Return a canonical representation of an S3 key or prefix.

    S3 treats keys as POSIX-like paths.  This helper collapses ``.`` and ``..``
    segments so callers can safely compare prefixes without being tricked by
    directory traversal payloads like ``artifacts/{job}/../other``.
    """

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
    except Exception as exc:  # pragma: no cover - defensive guard
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
    return {"jobId": job_id, "uploadUrl": upload_url}

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

# ---- Middleware ----
@app.middleware("http")
async def enforce_authentication(request: Request, call_next):
    if request.url.path == "/health":
        return await call_next(request)

    api_key = request.headers.get("x-api-key")
    if not api_key:
        record_metric("MissingApiKey", dimensions={"Path": request.url.path})
        return JSONResponse({"detail": "Missing API key"}, status_code=401)

    keys = load_api_keys()
    if not keys:
        logger.error("API authentication is not configured")
        return JSONResponse({"detail": "Server configuration error"}, status_code=500)

    if api_key not in keys:
        record_metric("InvalidApiKey", dimensions={"Path": request.url.path})
        return JSONResponse({"detail": "Invalid API key"}, status_code=403)

    if not _rate_limit_allow(api_key):
        record_metric("RateLimitExceeded", dimensions={"Path": request.url.path})
        return JSONResponse({"detail": "Rate limit exceeded"}, status_code=429)

    return await call_next(request)


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

