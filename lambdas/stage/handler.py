"""Lambda to stage job source data into the artifacts bucket.

The staging task is responsible for normalising every supported source into a
consistent layout within the artifacts bucket.  As new connectors are added we
only need to plug them into the dispatcher below – the rest of the orchestration
remains unchanged.
"""

import base64
import csv
import io
import json
import os
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError, NoSuchModuleError

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")
secretsmanager = boto3.client("secretsmanager")
ssm = boto3.client("ssm")

TABLE_NAME = os.environ["JOBS_TABLE"]
ARTIFACTS_BUCKET = os.environ["ARTIFACTS_BUCKET"]

STATUS_STAGING = "STAGING"
STATUS_STAGED = "STAGED"
STATUS_FAILED = "FAILED"


class FileNotReadyError(Exception):
    """Raised when an expected upload has not arrived yet."""


def _now() -> int:
    return int(time.time())


def _table():
    return ddb.Table(TABLE_NAME)


def _ddb_update(job_id: str, status: str, **attrs) -> None:
    expr_names = {"#s": "status"}
    expr_vals = {":s": status, ":u": _now()}
    set_expr = ["#s = :s", "updatedAt = :u"]

    for key, value in attrs.items():
        placeholder = f":{key}"
        expr_vals[placeholder] = value
        set_expr.append(f"{key} = {placeholder}")

    _table().update_item(
        Key={"pk": f"job#{job_id}", "sk": "meta"},
        UpdateExpression="SET " + ", ".join(set_expr),
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_vals,
    )


@dataclass
class SourceRef:
    bucket: str
    key: str

    @property
    def filename(self) -> str:
        return self.key.rsplit("/", 1)[-1]


@dataclass
class StagedArtifact:
    bucket: str
    key: str
    size: Optional[int]
    content_type: Optional[str]

    @property
    def path(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


def _safe_filename(name: Optional[str], fallback: str) -> str:
    candidate = (name or fallback).strip() or fallback
    candidate = os.path.basename(candidate)
    candidate = candidate.replace("..", "_")
    return candidate or fallback


def _filename_from_headers(response, url: str) -> str:
    disposition = response.headers.get("Content-Disposition")
    if disposition:
        parts = disposition.split(";")
        for part in parts[1:]:
            if "=" not in part:
                continue
            key, value = [segment.strip() for segment in part.split("=", 1)]
            if key.lower() == "filename":
                return _safe_filename(value.strip('"'), "http-download")

    parsed = urlparse(url)
    return _safe_filename(os.path.basename(parsed.path), "http-download")


def _download_http_source(job_id: str, source: Dict[str, object]) -> SourceRef:
    url = source.get("url")
    if not isinstance(url, str):
        raise ValueError("HTTP source missing url")

    method = str(source.get("method") or "GET").upper()
    headers = source.get("headers") or {}
    if not isinstance(headers, dict):
        raise ValueError("HTTP headers must be a mapping")
    body = source.get("body")
    timeout = source.get("timeout") or 30

    response = requests.request(method, url, headers=headers, data=body, timeout=timeout)
    if response.status_code >= 400:
        raise ValueError(f"HTTP connector received status {response.status_code} from {url}")

    filename = _safe_filename(source.get("filename"), _filename_from_headers(response, url))
    key = f"artifacts/{job_id}/input/{filename}"

    s3.put_object(
        Bucket=ARTIFACTS_BUCKET,
        Key=key,
        Body=response.content,
        ContentType=response.headers.get("Content-Type"),
    )

    return SourceRef(ARTIFACTS_BUCKET, key)


def _json_default(value):
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="ignore")
    return str(value)


def _secret_payload_value(raw_value: str, *, field: Optional[str]) -> str:
    if field:
        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError as exc:  # pragma: no cover - validated via calling code
            raise ValueError("Secret payload must be JSON when secretField is provided") from exc
        if field not in parsed:
            raise ValueError(f"Secret payload missing field '{field}'")
        value = parsed[field]
    else:
        value = raw_value

    if not isinstance(value, str):
        raise ValueError("Database connection secret must resolve to a string")

    value = value.strip()
    if not value:
        raise ValueError("Database connection string cannot be empty")

    return value


def _resolve_sql_connection(source: Dict[str, object]) -> str:
    """Return a SQLAlchemy connection URL for the provided source."""

    connection_meta = source.get("connection")
    if connection_meta is not None:
        if not isinstance(connection_meta, dict):
            raise ValueError("SQL connection metadata must be an object")

        conn_type = str(connection_meta.get("type") or "inline").lower()
        secret_field = connection_meta.get("secretField")

        if conn_type in {"inline", "url"}:
            url = connection_meta.get("url")
            if not isinstance(url, str) or not url.strip():
                raise ValueError("Inline SQL connection requires a non-empty url")
            return url.strip()

        if conn_type in {"secretsmanager", "secret", "secrets"}:
            secret_arn = connection_meta.get("secretArn") or connection_meta.get("arn")
            if not isinstance(secret_arn, str) or not secret_arn:
                raise ValueError("Secrets Manager connection requires secretArn")
            try:
                response = secretsmanager.get_secret_value(SecretId=secret_arn)
            except ClientError as exc:
                raise ValueError(f"Failed to retrieve secret {secret_arn}: {exc}") from exc
            if "SecretString" in response and response["SecretString"] is not None:
                raw_value = response["SecretString"]
            elif "SecretBinary" in response and response["SecretBinary"] is not None:
                raw_value = base64.b64decode(response["SecretBinary"]).decode("utf-8")
            else:
                raise ValueError(f"Secret {secret_arn} does not contain a value")
            return _secret_payload_value(raw_value, field=secret_field)

        if conn_type in {"parameterstore", "ssm", "systemsmanager"}:
            parameter_name = connection_meta.get("parameterName") or connection_meta.get("name")
            if not isinstance(parameter_name, str) or not parameter_name:
                raise ValueError("Parameter Store connection requires parameterName")
            try:
                response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
            except ClientError as exc:
                raise ValueError(f"Failed to retrieve parameter {parameter_name}: {exc}") from exc
            parameter = response.get("Parameter") or {}
            raw_value = parameter.get("Value")
            if raw_value is None:
                raise ValueError(f"Parameter {parameter_name} does not contain a value")
            return _secret_payload_value(raw_value, field=secret_field)

        raise ValueError(f"Unsupported SQL connection type: {conn_type}")

    url = source.get("url")
    if isinstance(url, str) and url.strip():
        return url.strip()

    secret_arn = source.get("secretArn")
    parameter_name = source.get("parameterName")
    secret_field = source.get("secretField")

    if secret_arn:
        try:
            response = secretsmanager.get_secret_value(SecretId=secret_arn)
        except ClientError as exc:
            raise ValueError(f"Failed to retrieve secret {secret_arn}: {exc}") from exc
        if "SecretString" in response and response["SecretString"] is not None:
            raw_value = response["SecretString"]
        elif "SecretBinary" in response and response["SecretBinary"] is not None:
            raw_value = base64.b64decode(response["SecretBinary"]).decode("utf-8")
        else:
            raise ValueError(f"Secret {secret_arn} does not contain a value")
        return _secret_payload_value(raw_value, field=secret_field)

    if parameter_name:
        try:
            response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
        except ClientError as exc:
            raise ValueError(f"Failed to retrieve parameter {parameter_name}: {exc}") from exc
        parameter = response.get("Parameter") or {}
        raw_value = parameter.get("Value")
        if raw_value is None:
            raise ValueError(f"Parameter {parameter_name} does not contain a value")
        return _secret_payload_value(raw_value, field=secret_field)

    raise ValueError(
        "SQL source requires a connection string via connection metadata, url, secretArn, or parameterName"
    )


def _extract_sql_source(job_id: str, source: Dict[str, object], *, default_name: str) -> SourceRef:
    query = source.get("query")
    if not isinstance(query, str):
        raise ValueError("SQL source requires a query")

    url = _resolve_sql_connection(source)

    params = source.get("params") or {}
    if params and not isinstance(params, dict):
        raise ValueError("SQL params must be a mapping")

    filename = _safe_filename(source.get("filename"), default_name)
    export_format = str(source.get("format") or "csv").lower()
    if export_format not in {"csv", "jsonl"}:
        raise ValueError("SQL export format must be 'csv' or 'jsonl'")

    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query), params)

            if export_format == "csv":
                buffer = io.StringIO()
                writer = csv.writer(buffer)
                writer.writerow(result.keys())
                for row in result:
                    writer.writerow([str(value) if value is not None else "" for value in row])
                payload = buffer.getvalue().encode("utf-8")
                content_type = "text/csv"
            else:
                lines = []
                for mapping in result.mappings():
                    lines.append(json.dumps(dict(mapping), default=_json_default))
                payload = "\n".join(lines).encode("utf-8")
                content_type = "application/json"
    except NoSuchModuleError as exc:
        raise ValueError("SQL driver for the provided URL is not installed") from exc
    except DBAPIError as exc:
        raise ValueError(f"Failed to execute SQL query: {exc}") from exc
    finally:
        engine.dispose()

    key = f"artifacts/{job_id}/input/{filename}"
    s3.put_object(Bucket=ARTIFACTS_BUCKET, Key=key, Body=payload, ContentType=content_type)

    return SourceRef(ARTIFACTS_BUCKET, key)


def _resolve_source(job_id: str, item: Dict[str, Dict]) -> Tuple[SourceRef, str]:
    source = item.get("source") or {}
    source_type = source.get("type")

    if source_type == "upload":
        bucket = source.get("bucket")
        key = source.get("key")
        if not bucket or not key:
            raise ValueError("Upload job missing bucket/key metadata")
        return SourceRef(bucket, key), source_type

    if source_type == "s3":
        uri = source.get("uri")
        if not uri or not uri.startswith("s3://"):
            raise ValueError("S3 job missing uri metadata")
        without_scheme = uri[5:]
        parts = without_scheme.split("/", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError("Invalid S3 URI")
        return SourceRef(parts[0], parts[1]), source_type

    if source_type == "http":
        protocol = source.get("protocol") or "http"
        return _download_http_source(job_id, source), protocol

    if source_type == "database":
        ref = _extract_sql_source(job_id, source, default_name="database-export.csv")
        return ref, source_type

    if source_type == "warehouse":
        warehouse_type = source.get("warehouseType") or "warehouse"
        ref = _extract_sql_source(job_id, source, default_name=f"{warehouse_type}-export.csv")
        return ref, f"warehouse:{warehouse_type}"

    raise ValueError(f"Unsupported source type: {source_type}")


def _wait_for_upload(src: SourceRef) -> None:
    try:
        s3.head_object(Bucket=src.bucket, Key=src.key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            raise FileNotReadyError(f"Upload not found at s3://{src.bucket}/{src.key}") from exc
        raise


def _copy_object(src: SourceRef, dest_key: str) -> None:
    if src.bucket == ARTIFACTS_BUCKET and src.key == dest_key:
        # Already staged in the correct location.
        return
    s3.copy_object(
        Bucket=ARTIFACTS_BUCKET,
        Key=dest_key,
        CopySource={"Bucket": src.bucket, "Key": src.key},
    )


def _detect_format(key: str, content_type: Optional[str]) -> str:
    name = key.lower()
    if name.endswith(".csv"):
        return "csv"
    if name.endswith(".tsv") or name.endswith(".tab"):
        return "tsv"
    if name.endswith(".jsonl") or name.endswith(".ndjson"):
        return "jsonl"
    if name.endswith(".json"):
        return "json"
    if name.endswith(".xlsx") or name.endswith(".xls") or name.endswith(".xlsm"):
        return "excel"
    if name.endswith(".parquet") or name.endswith(".pq") or name.endswith(".pqt") or name.endswith(".parq"):
        return "parquet"
    if name.endswith(".sqlite") or name.endswith(".sqlite3") or name.endswith(".db"):
        return "sqlite"
    if name.endswith(".zip"):
        return "zip"
    if name.endswith(".tar.gz") or name.endswith(".tgz"):
        return "tar"
    if name.endswith(".tar"):
        return "tar"
    if name.endswith(".gz") or name.endswith(".gzip"):
        return "gzip"

    if content_type:
        ct = content_type.lower()
        if "csv" in ct:
            return "csv"
        if "tsv" in ct or "tab-separated" in ct:
            return "tsv"
        if "json" in ct:
            # Handles generic JSON content types as a fallback.
            return "json"
        if "spreadsheet" in ct or "ms-excel" in ct:
            return "excel"
        if "parquet" in ct:
            return "parquet"
        if "tar" in ct:
            return "tar"
        if "zip" in ct:
            return "zip"
        if "gzip" in ct:
            return "gzip"

    return "unknown"


def _stage_source(job_id: str, src: SourceRef) -> StagedArtifact:
    filename = src.filename or "source"
    dest_key = f"artifacts/{job_id}/input/{filename}"

    _copy_object(src, dest_key)

    head = s3.head_object(Bucket=ARTIFACTS_BUCKET, Key=dest_key)
    size = head.get("ContentLength")
    content_type = head.get("ContentType")

    return StagedArtifact(
        bucket=ARTIFACTS_BUCKET,
        key=dest_key,
        size=size,
        content_type=content_type,
    )


def handler(event, _context):
    job_id = event.get("jobId")
    if not job_id:
        raise ValueError("jobId is required")

    print(f"[Stage] Starting staging for job {job_id}")

    # Fetch job metadata
    res = _table().get_item(Key={"pk": f"job#{job_id}", "sk": "meta"})
    item = res.get("Item")
    if not item:
        raise ValueError(f"Job {job_id} not found")

    src, source_type = _resolve_source(job_id, item)

    # Mark job as staging (idempotent)
    _ddb_update(job_id, STATUS_STAGING)

    if source_type == "upload":
        _wait_for_upload(src)

    try:
        staged = _stage_source(job_id, src)
    except FileNotReadyError:
        # Should never reach here due to early check, but propagate just in case.
        raise
    except Exception as exc:
        print(f"[Stage] ERROR copying object: {exc}")
        _ddb_update(job_id, STATUS_FAILED, error=str(exc))
        raise

    fmt = _detect_format(staged.key, staged.content_type)

    metadata = {
        "size": staged.size,
        "format": fmt,
        "contentType": staged.content_type,
        "sourceType": source_type,
    }

    _ddb_update(
        job_id,
        STATUS_STAGED,
        inputKey=staged.key,
        inputMetadata=metadata,
    )

    print(f"[Stage] Staged data at {staged.path} (format={fmt}, size={staged.size})")

    return {
        "jobId": job_id,
        "input": {"bucket": staged.bucket, "key": staged.key},
        "metadata": metadata,
    }
