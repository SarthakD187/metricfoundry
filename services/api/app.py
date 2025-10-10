# services/api/app.py
from __future__ import annotations

import io
import json
import logging
import os
import time
import uuid
import zipfile
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
from pydantic import BaseModel

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


def _build_source(body: CreateJob, job_id: str) -> Tuple[dict, Optional[str]]:
    """Return (source_metadata, upload_url?)."""
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
    """
    Dev/manual processor that infers CSV shape, writes phases/*, and results/*.
    Produces correct rows/columns/schema and real descriptive stats/correlations/outliers.
    """
    import csv
    from collections import Counter

    job = ensure_job(job_id)
    src = job.get("source") or {}
    bucket = src.get("bucket")
    key    = src.get("key")
    if not (bucket and key):
        raise HTTPException(status_code=400, detail="job has no source.bucket/key")

    ddb_update_status(job_id, "RUNNING")
    now_iso = datetime.now(timezone.utc).isoformat()
    base = f"artifacts/{job_id}"

    # --- Read entire object (ok for small CSVs; switch to chunking if needed)
    obj = s3.get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read()
    data_bytes = len(raw)

    # --- Decode (handle BOM) ---
    text = raw.decode("utf-8-sig", errors="ignore")

    rows = 0
    columns = 0
    header: List[str] = []
    data_rows: List[List[str]] = []
    data_rows_preview: List[List[str]] = []
    inferred_types: List[str] = []
    delimiter: Optional[str] = None

    if text:
        # --- Sniff delimiter & parse header ---
        sample = text[:64 * 1024]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[",",";","\t","|"])
            delimiter = dialect.delimiter
        except Exception:
            # fallback: choose the most common likely delimiter in the sample
            cand = Counter([c for c in sample if c in ",;\t|"]).most_common(1)
            delimiter = cand[0][0] if cand else ","

        reader = csv.reader(io.StringIO(text), delimiter=delimiter)
        rows_list = list(reader)

        if rows_list:
            header = rows_list[0]
            data_rows = rows_list[1:]
        else:
            header = []
            data_rows = []

        columns = len(header)

        # Trim empty trailing lines that parsed as []
        data_rows = [r for r in data_rows if any((cell or "").strip() != "" for cell in r)]
        rows = len(data_rows)

        # --- normalize ragged rows & header padding ---
        max_len = max([columns] + [len(r) for r in data_rows]) if data_rows else columns
        if columns < max_len and header:
            header = header + [f"col_{i}" for i in range(columns+1, max_len+1)]
            columns = max_len
            # also pad each data row to the same length (for safe indexing below)
            data_rows = [r + [""] * (columns - len(r)) for r in data_rows]

        # Preview a few rows
        data_rows_preview = data_rows[:3]

        # --- Light type inference per column (sample up to 200 rows) ---
        def infer_type(values: List[str]) -> str:
            num_hits = 0
            non_empty = 0
            for v in values:
                v = (v or "").strip()
                if not v:
                    continue
                non_empty += 1
                try:
                    float(v.replace(",", ""))  # tolerate 1,234.5
                    num_hits += 1
                except Exception:
                    pass
            if non_empty > 0 and num_hits / non_empty >= 0.8:
                return "number"
            return "string"

        sample_n = min(200, len(data_rows))
        col_samples: List[List[str]] = [[] for _ in range(columns)]
        for r in data_rows[:sample_n]:
            for i in range(columns):
                col_samples[i].append(r[i] if i < len(r) else "")

        inferred_types = [infer_type(vals) for vals in col_samples] if columns else []

    # --- Build phases/* payloads ---
    profiling_payload = {
        "phase": "profiling",
        "status": "completed",
        "at": now_iso,
        "shape": {"rows": rows, "columns": columns},
        "delimiter": delimiter if text else None,
        "head": [dict(zip(header, r + [""] * (len(header) - len(r)))) for r in data_rows_preview] if header else [],
        "columnProfiles": [
            {
                "name": (name if (name and str(name).strip()) else f"col_{i+1}"),
                "inferredType": inferred_types[i] if i < len(inferred_types) else "string",
            }
            for i, name in enumerate(header)
        ],
    }

    ingest_payload = {
        "phase": "ingest",
        "input": {"bucket": bucket, "key": key, "bytes": data_bytes},
        "status": "completed",
        "at": now_iso,
    }
    quality_payload = {
        "phase": "data_quality",
        "checks": [{"name": "non_empty", "status": "pass" if rows > 0 and columns > 0 else "fail"}],
        "status": "completed",
        "at": now_iso,
    }
    descriptives_payload = {
        "phase": "descriptive_stats",
        "tables": [{"name": "descriptive_stats", "path": f"s3://{BUCKET_NAME}/{base}/results/descriptive_stats.csv"}],
        "status": "completed",
        "at": now_iso,
    }
    narrative_payload = {
        "phase": "nl_report",
        "highlights": [
            f"Dataset has {rows} rows and {columns} columns.",
            "Basic profile generated from header, delimiter inference, and sampling.",
        ],
        "status": "completed",
        "at": now_iso,
    }
    finalize_payload = {"phase": "finalization", "status": "completed", "at": now_iso}

    # --- Write phases/*
    phases = [
        ("01_ingest.json", ingest_payload),
        ("02_profiling.json", profiling_payload),
        ("03_quality.json", quality_payload),
        ("04_descriptives.json", descriptives_payload),
        ("05_narrative.json", narrative_payload),
        ("99_finalize.json", finalize_payload),
    ]
    for fname, payload in phases:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{base}/phases/{fname}",
            Body=json.dumps(payload, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

    # --- Top-level manifest
    manifest = {
        "jobId": job_id,
        "generatedAt": now_iso,
        "input": {"bucket": bucket, "key": key, "bytes": data_bytes},
        "phases": [f"s3://{BUCKET_NAME}/{base}/phases/{n}" for (n, _) in phases],
    }
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"{base}/manifest.json",
        Body=json.dumps(manifest, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    # =========================
    # REAL DESCRIPTIVE STATS
    # =========================
    def _to_float(s: str) -> Optional[float]:
        if s is None:
            return None
        s = s.strip()
        if s == "":
            return None
        try:
            return float(s.replace(",", ""))
        except Exception:
            return None

    # Choose numeric columns based on inferred types
    col_names = [c["name"] for c in profiling_payload.get("columnProfiles", [])]
    num_indices = [i for i, c in enumerate(profiling_payload.get("columnProfiles", []))
                   if (isinstance(c, dict) and c.get("inferredType") == "number")]
    num_names = [col_names[i] for i in num_indices]

    # Materialize numeric columns
    col_values: List[List[float]] = [[] for _ in num_indices]
    for r in data_rows:
        for j, col_idx in enumerate(num_indices):
            v = _to_float(r[col_idx] if col_idx < len(r) else None)
            if v is not None:
                col_values[j].append(v)

    def _quantile(sorted_vals: List[float], q: float) -> Optional[float]:
        if not sorted_vals:
            return None
        if q <= 0: return sorted_vals[0]
        if q >= 1: return sorted_vals[-1]
        pos = q * (len(sorted_vals) - 1)
        lo = int(pos)
        hi = min(lo + 1, len(sorted_vals) - 1)
        frac = pos - lo
        return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac

    def _stats(vals: List[float]) -> dict:
        n = len(vals)
        if n == 0:
            return {"count": 0, "mean": None, "std": None, "min": None, "p5": None, "p25": None,
                    "p50": None, "p75": None, "p95": None, "max": None}
        s = sum(vals)
        mean = s / n
        if n > 1:
            var = sum((x - mean) ** 2 for x in vals) / (n - 1)  # sample variance
            std = var ** 0.5
        else:
            std = 0.0
        sv = sorted(vals)
        return {
            "count": n,
            "mean": mean,
            "std": std,
            "min": sv[0],
            "p5":  _quantile(sv, 0.05),
            "p25": _quantile(sv, 0.25),
            "p50": _quantile(sv, 0.50),
            "p75": _quantile(sv, 0.75),
            "p95": _quantile(sv, 0.95),
            "max": sv[-1],
        }

    # Build descriptive table rows
    desc_rows = []
    for name, vals in zip(num_names, col_values):
        st = _stats(vals)
        desc_rows.append({"column": name, **st})

    # Write descriptive_stats.csv
    desc_header = ["column","count","mean","std","min","p5","p25","p50","p75","p95","max"]
    def _fmt(x):
        if x is None: return ""
        if isinstance(x, float):
            return f"{x:.6g}"
        return str(x)
    out_lines = [",".join(desc_header)]
    for row in desc_rows:
        out_lines.append(",".join(_fmt(row[h]) for h in desc_header))
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"{base}/results/descriptive_stats.csv",
        Body=("\n".join(out_lines) + "\n").encode("utf-8"),
        ContentType="text/csv",
    )

    # JSON version for UI
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"{base}/results/descriptiveTable.json",
        Body=json.dumps({"columns": desc_header, "rows": desc_rows}, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    # --- Real correlations (top pairs by |r|)
    def _pearson(x: List[float], y: List[float]) -> Optional[float]:
        n = min(len(x), len(y))
        if n < 2:
            return None
        mx = sum(x[:n]) / n
        my = sum(y[:n]) / n
        num = sum((x[i]-mx)*(y[i]-my) for i in range(n))
        denx = sum((x[i]-mx)**2 for i in range(n)) ** 0.5
        deny = sum((y[i]-my)**2 for i in range(n)) ** 0.5
        if denx == 0 or deny == 0:
            return None
        return num / (denx * deny)

    corr_rows: List[dict] = []
    for a_i in range(len(num_names)):
        for b_i in range(a_i + 1, len(num_names)):
            A, B = [], []
            col_a = num_indices[a_i]
            col_b = num_indices[b_i]
            for r in data_rows:
                va = _to_float(r[col_a] if col_a < len(r) else None)
                vb = _to_float(r[col_b] if col_b < len(r) else None)
                if va is not None and vb is not None:
                    A.append(va); B.append(vb)
            r_val = _pearson(A, B)
            if r_val is not None:
                corr_rows.append({"feature_x": num_names[a_i], "feature_y": num_names[b_i], "pearson_r": r_val, "n": len(A)})

    corr_rows.sort(key=lambda d: abs(d["pearson_r"]), reverse=True)
    corr_rows = corr_rows[:100]
    corr_header = ["feature_x","feature_y","pearson_r","n"]
    corr_lines = [",".join(corr_header)]
    for row in corr_rows:
        corr_lines.append(",".join([row["feature_x"], row["feature_y"], _fmt(row["pearson_r"]), str(row["n"])]))
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"{base}/results/correlations.csv",
        Body=("\n".join(corr_lines) + "\n").encode("utf-8"),
        ContentType="text/csv",
    )

    # --- Outliers: simple z-score >= 3 per numeric column
    out_findings = []
    for name, vals in zip(num_names, col_values):
        st = _stats(vals)
        n = st["count"]
        mu = st["mean"]; sd = st["std"]
        if n >= 3 and sd and sd > 0:
            for v in vals:
                z = (v - mu) / sd
                if abs(z) >= 3:
                    out_findings.append({"column": name, "value": v, "z": z})
    out_findings = sorted(out_findings, key=lambda d: abs(d["z"]), reverse=True)[:50]
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"{base}/results/outliers.json",
        Body=json.dumps({"method":"zscore","threshold":3.0,"findings": out_findings}, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    # --- Report
    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>MetricFoundry Report</title>
<style>body{{font-family:ui-sans-serif,system-ui;max-width:920px;margin:24px auto;padding:0 16px}}
h1{{font-size:22px}} .kpi{{display:inline-block;margin-right:16px;padding:8px 12px;border:1px solid #ddd;border-radius:10px}}</style></head>
<body>
  <h1>Descriptive report — Job {job_id}</h1>
  <div class="kpi">Rows: <b>{rows}</b></div>
  <div class="kpi">Columns: <b>{columns}</b></div>
  <p>Generated: {now_iso}</p>
  <p><b>Highlights:</b> Real stats computed from numeric columns; correlations and potential outliers included.</p>
</body></html>"""
    s3.put_object(Bucket=BUCKET_NAME, Key=f"{base}/results/report.html", Body=html.encode("utf-8"), ContentType="text/html")
    s3.put_object(Bucket=BUCKET_NAME, Key=f"{base}/results/report.txt", Body=f"Job {job_id}\nRows: {rows}\nColumns: {columns}\nGenerated: {now_iso}\n".encode("utf-8"), ContentType="text/plain")

    # --- Graphs (PNG stubs kept)
    png_stub = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x05\x00\x00\x00\x05\x08\x06\x00\x00\x00\x8d\x89\x1d\r\x00\x00\x00\x0cIDATx\x9ccddbf\xa0\x040\x00\x00\x05\x00\x01\x9b\xc7\x19\xd3\x00\x00\x00\x00IEND\xaeB`\x82"
    s3.put_object(Bucket=BUCKET_NAME, Key=f"{base}/results/graphs/hist_1.png", Body=png_stub, ContentType="image/png")
    s3.put_object(Bucket=BUCKET_NAME, Key=f"{base}/results/graphs/scatter_1.png", Body=png_stub, ContentType="image/png")

    # --- Results manifest
    results_manifest = {
        "jobId": job_id,
        "generatedAt": now_iso,
        "artifacts": [
            {"key": f"{base}/results/descriptive_stats.csv", "contentType":"text/csv"},
            {"key": f"{base}/results/descriptiveTable.json", "contentType":"application/json"},
            {"key": f"{base}/results/correlations.csv", "contentType":"text/csv"},
            {"key": f"{base}/results/outliers.json", "contentType":"application/json"},
            {"key": f"{base}/results/report.html", "contentType":"text/html"},
            {"key": f"{base}/results/report.txt", "contentType":"text/plain"},
            {"key": f"{base}/results/graphs/hist_1.png", "contentType":"image/png"},
            {"key": f"{base}/results/graphs/scatter_1.png", "contentType":"image/png"},
        ]
    }
    s3.put_object(Bucket=BUCKET_NAME, Key=f"{base}/results/manifest.json", Body=json.dumps(results_manifest, indent=2).encode("utf-8"), ContentType="application/json")

    # --- parse_debug.json (for easy troubleshooting)
    try:
        parse_debug = {
            "shape": {"rows": rows, "columns": columns},
            "delimiter": profiling_payload.get("delimiter"),
            "firstHeader": header[:20] if text else [],
            "head": profiling_payload.get("head", []),
        }
        s3.put_object(Bucket=BUCKET_NAME, Key=f"{base}/results/parse_debug.json", Body=json.dumps(parse_debug, indent=2).encode("utf-8"), ContentType="application/json")
    except Exception:
        pass

    # --- results.json (canonical API summary) with schema + analysisVersion
    schema = profiling_payload.get("columnProfiles") or []
    results_json = {
        "jobId": job_id,
        "analysisVersion": "2025.10-dev",
        "generatedAt": now_iso,
        "summary": {"rows": rows, "columns": columns},
        "schema": [{"name": c["name"], "type": c.get("inferredType")} for c in schema],
        "links": {
            "manifest": f"s3://{BUCKET_NAME}/{base}/manifest.json",
            "resultsManifest": f"s3://{BUCKET_NAME}/{base}/results/manifest.json",
            "resultsJson": f"s3://{BUCKET_NAME}/{base}/results/results.json",
            "input": f"s3://{bucket}/{key}",
            "reportHtml": f"s3://{BUCKET_NAME}/{base}/results/report.html",
        },
    }
    s3.put_object(Bucket=BUCKET_NAME, Key=f"{base}/results/results.json", Body=json.dumps(results_json, indent=2).encode("utf-8"), ContentType="application/json")

    # success
    results_key = f"{base}/results/results.json"
    ddb_update_status(job_id, "SUCCEEDED", resultKey=results_key)
    return {"ok": True, "resultKey": results_key}


# ---- Middleware ----
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
                "request_id": request_id,
                "duration_ms": duration_ms,
            },
        )
        raise


# ✅ GLOBAL Lambda handler (must be at module scope)
handler = Mangum(app)
