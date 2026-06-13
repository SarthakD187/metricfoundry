"""Lambda to stage job source data into the artifacts bucket.

The staging task is responsible for normalising every supported source into a
consistent layout within the artifacts bucket.  As new connectors are added we
only need to plug them into the dispatcher below – the rest of the orchestration
remains unchanged.
"""

import base64
import csv
import io
import ipaddress
import json
import logging
import os
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import import_module
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union
from urllib.parse import parse_qs, unquote, urlparse

import boto3
from botocore.exceptions import ClientError
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.dialects import registry
from sqlalchemy.exc import DBAPIError, NoSuchModuleError

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")
secretsmanager = boto3.client("secretsmanager")
ssm = boto3.client("ssm")
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

TABLE_NAME = os.environ["JOBS_TABLE"]
ARTIFACTS_BUCKET = os.environ["ARTIFACTS_BUCKET"]
DEFAULT_MAX_HTTP_BYTES = 10 * 1024 * 1024


def _load_max_http_bytes() -> int:
    raw_value = os.environ.get("MAX_HTTP_BYTES")
    if raw_value is None or not str(raw_value).strip():
        return DEFAULT_MAX_HTTP_BYTES

    try:
        parsed = int(str(raw_value).strip())
    except ValueError as exc:  # pragma: no cover - configuration error
        raise ValueError("MAX_HTTP_BYTES must be an integer") from exc

    if parsed <= 0:
        raise ValueError("MAX_HTTP_BYTES must be greater than zero")

    return parsed


MAX_HTTP_BYTES = _load_max_http_bytes()

STATUS_STAGING = "STAGING"
STATUS_STAGED = "STAGED"
STATUS_FAILED = "FAILED"


def _register_sqlalchemy_dialects() -> None:
    """Register optional SQLAlchemy dialects that still use the generic SQL path."""

    targets = [
        (
            "databricks.sqlalchemy",
            "databricks.sqlalchemy",
            "DatabricksDialect",
            ["databricks", "databricks+connector"],
        ),
    ]

    for module_name, target_module, dialect_cls, aliases in targets:
        try:
            import_module(module_name)
        except ImportError:
            continue

        for alias in aliases:
            registry.register(alias, target_module, dialect_cls)


_register_sqlalchemy_dialects()


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
        alias = f"#{key}"
        expr_names[alias] = key
        set_expr.append(f"{alias} = {placeholder}")

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
class HttpDownload:
    url: str
    filename: str
    content: bytes
    content_type: Optional[str]


@dataclass
class HttpStagingResult:
    original: "StagedArtifact"
    normalized: "StagedArtifact"
    manifest: Dict[str, object]
    manifest_key: str
    format: str
    schema_sample: List[str]
    row_count: Optional[int]


def _job_artifact_prefix(item: Dict[str, object], job_id: str) -> str:
    prefix = item.get("artifactPrefix")
    if isinstance(prefix, str) and prefix.strip():
        return prefix.rstrip("/")
    return f"artifacts/{job_id}"


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


def _resolve_host_ips(hostname: str) -> List[str]:
    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        try:
            infos = socket.getaddrinfo(hostname, None)
        except socket.gaierror as exc:
            raise ValueError(f"Unable to resolve hostname {hostname}: {exc}") from exc

        addresses: List[str] = []
        for info in infos:
            sockaddr = info[4]
            if not sockaddr:
                continue
            ip = sockaddr[0]
            if ip not in addresses:
                addresses.append(ip)
        if not addresses:
            raise ValueError(f"Unable to resolve hostname {hostname}")
        return addresses

    return [hostname]


def _ensure_public_destination(hostname: str) -> None:
    for ip in _resolve_host_ips(hostname):
        addr = ipaddress.ip_address(ip)
        if not addr.is_global:
            raise ValueError(
                f"HTTP connector disallows private or link-local address {ip} for host {hostname}"
            )


def _validate_http_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("HTTP connector only supports http and https URLs")
    if not parsed.hostname:
        raise ValueError("HTTP connector requires a hostname")

    _ensure_public_destination(parsed.hostname)

    return url


def _coerce_timeout(value: Any) -> Tuple[float, float]:
    if value is None:
        return (5.0, 20.0)

    if isinstance(value, (int, float)):
        timeout = float(value)
        if timeout <= 0:
            raise ValueError("HTTP connector timeout must be positive")
        return (timeout, timeout)

    if isinstance(value, (list, tuple)) and len(value) == 2:
        connect, read = value
        connect_timeout = float(connect)
        read_timeout = float(read)
        if connect_timeout <= 0 or read_timeout <= 0:
            raise ValueError("HTTP connector timeout must be positive")
        return (connect_timeout, read_timeout)

    raise ValueError("HTTP connector timeout must be a number or [connect, read] tuple")


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


def _download_http_source(job_id: str, _artifact_prefix: str, source: Dict[str, object]) -> HttpDownload:
    url = source.get("url")
    if not isinstance(url, str):
        raise ValueError("HTTP source missing url")

    url = _validate_http_url(url.strip())

    method = str(source.get("method") or "GET").upper()
    headers = source.get("headers") or {}
    if not isinstance(headers, dict):
        raise ValueError("HTTP headers must be a mapping")
    body = source.get("body")
    timeout = _coerce_timeout(source.get("timeout"))

    response = requests.request(
        method,
        url,
        headers=headers,
        data=body,
        timeout=timeout,
        stream=True,
        allow_redirects=False,
    )

    try:
        if response.status_code >= 400 or 300 <= response.status_code < 400:
            raise ValueError(f"HTTP connector received status {response.status_code} from {url}")

        content_length = response.headers.get("Content-Length")
        if content_length:
            try:
                length_value = int(content_length)
            except ValueError:
                raise ValueError("HTTP connector received invalid Content-Length header")
            if length_value > MAX_HTTP_BYTES:
                raise ValueError("HTTP connector download exceeds configured MAX_HTTP_BYTES")

        buffer = bytearray()
        for chunk in response.iter_content(chunk_size=1024 * 64):
            if not chunk:
                continue
            buffer.extend(chunk)
            if len(buffer) > MAX_HTTP_BYTES:
                raise ValueError("HTTP connector download exceeds configured MAX_HTTP_BYTES")

        filename = _safe_filename(source.get("filename"), _filename_from_headers(response, url))
        content_type = response.headers.get("Content-Type")

        return HttpDownload(url=url, filename=filename, content=bytes(buffer), content_type=content_type)
    finally:
        response.close()


def _extension_from_content_type(content_type: Optional[str]) -> str:
    if not content_type:
        return ""
    ct = content_type.split(";", 1)[0].strip().lower()
    if "json" in ct:
        return ".json"
    if "csv" in ct:
        return ".csv"
    if "tsv" in ct or "tab-separated" in ct:
        return ".tsv"
    return ""


def _infer_original_extension(filename: str, content_type: Optional[str]) -> str:
    name = filename.lower()
    if "." in name and not name.endswith("."):
        _, ext = os.path.splitext(name)
        if ext:
            return ext
    return _extension_from_content_type(content_type)


def _content_type_for_extension(extension: str) -> Optional[str]:
    ext = extension.lower().lstrip(".")
    if ext in {"json", "jsonl", "ndjson"}:
        return "application/json"
    if ext == "csv":
        return "text/csv"
    if ext == "tsv":
        return "text/tab-separated-values"
    return None


def _schema_sample_from_mappings(records: Sequence[object], limit: int = 10) -> List[str]:
    seen: List[str] = []
    for entry in records[:limit]:
        if isinstance(entry, dict):
            for key in entry.keys():
                key_str = str(key)
                if key_str not in seen:
                    seen.append(key_str)
    return seen


def _normalise_http_download(download: HttpDownload) -> Tuple[bytes, str, List[str], Optional[int], str, str]:
    """Return (normalized_bytes, format, schema_sample, row_count, normalized_ext, content_type)."""

    filename = download.filename.lower()
    content_type = (download.content_type or "").lower()

    is_json = filename.endswith((".json", ".jsonl", ".ndjson")) or "json" in content_type
    is_tsv = filename.endswith(".tsv") or "tsv" in content_type or "tab-separated" in content_type
    is_csv = filename.endswith(".csv") or ("csv" in content_type and not is_tsv)

    if not is_json and not is_csv and not is_tsv:
        sample = download.content[:1024].decode("utf-8", errors="ignore")
        stripped = sample.lstrip()
        if stripped.startswith("{") or stripped.startswith("["):
            is_json = True
        else:
            first_line = sample.splitlines()[0] if sample else ""
            if "\t" in first_line:
                is_tsv = True
            elif "," in first_line:
                is_csv = True

    if is_json:
        text = download.content.decode("utf-8-sig", errors="replace")
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            # Treat as JSON Lines
            records: List[object] = []
            for line in text.splitlines():
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    records.append(json.loads(stripped))
                except json.JSONDecodeError as exc:
                    raise ValueError("JSON Lines payload contains invalid JSON record") from exc

            schema_sample = _schema_sample_from_mappings(records)
            row_count = len(records)
            lines = [json.dumps(item, default=_json_default) for item in records]
            normalized = "\n".join(lines).encode("utf-8")
            return normalized, "jsonl", schema_sample, row_count, ".jsonl", "application/json"

        if isinstance(parsed, dict) and isinstance(parsed.get("data"), list):
            records = parsed["data"]
        else:
            records = parsed

        if not isinstance(records, list):
            raise ValueError("JSON payload must be an array or object with data array")

        schema_sample = _schema_sample_from_mappings(records)
        row_count = len(records)
        lines = [json.dumps(item, default=_json_default) for item in records]
        normalized = "\n".join(lines).encode("utf-8")
        return normalized, "jsonl", schema_sample, row_count, ".jsonl", "application/json"

    if is_csv or is_tsv:
        text = download.content.decode("utf-8-sig", errors="replace")
        reader = list(csv.reader(io.StringIO(text), delimiter="\t" if is_tsv else ","))
        header: List[str] = []
        if reader:
            header = [str(col) for col in reader[0]]
        schema_sample = header
        row_count = max(len(reader) - 1, 0) if reader else 0
        ext = ".tsv" if is_tsv else ".csv"
        ctype = "text/tab-separated-values" if is_tsv else "text/csv"
        fmt = "tsv" if is_tsv else "csv"
        return download.content, fmt, schema_sample, row_count, ext, ctype

    raise ValueError("HTTP connector only supports CSV or JSON responses")


def _build_http_manifest(
    job_id: str,
    artifact_prefix: str,
    download: HttpDownload,
    original: "StagedArtifact",
    normalized: "StagedArtifact",
    *,
    fmt: str,
    schema_sample: Sequence[str],
    row_count: Optional[int],
) -> Tuple[Dict[str, object], str]:
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    manifest = {
        "jobId": job_id,
        "generatedAt": generated_at,
        "source": {"type": "http", "url": download.url},
        "format": fmt,
        "schemaSample": list(schema_sample),
        "rowCountEstimate": row_count,
        "artifacts": [
            {
                "name": "original",
                "bucket": original.bucket,
                "key": original.key,
                "bytes": original.size,
                "contentType": original.content_type,
            },
            {
                "name": "normalized",
                "bucket": normalized.bucket,
                "key": normalized.key,
                "bytes": normalized.size,
                "contentType": normalized.content_type,
                "format": fmt,
                "schemaSample": list(schema_sample),
                "rowCountEstimate": row_count,
            },
        ],
    }

    key = f"{artifact_prefix}/manifest.json"
    s3.put_object(
        Bucket=ARTIFACTS_BUCKET,
        Key=key,
        Body=json.dumps(manifest, indent=2, default=_json_default).encode("utf-8"),
        ContentType="application/json",
    )

    return manifest, key


def _stage_http_source(
    job_id: str,
    artifact_prefix: str,
    download: HttpDownload,
) -> HttpStagingResult:
    original_ext = _infer_original_extension(download.filename, download.content_type) or ".bin"
    normalized_bytes, fmt, schema_sample, row_count, normalized_ext, normalized_content_type = _normalise_http_download(
        download
    )

    original_key = f"{artifact_prefix}/staged/original{original_ext}"
    normalized_key = f"{artifact_prefix}/staged/normalized{normalized_ext}"

    original_content_type = (
        download.content_type or _content_type_for_extension(original_ext) or "application/octet-stream"
    )
    s3.put_object(
        Bucket=ARTIFACTS_BUCKET,
        Key=original_key,
        Body=download.content,
        ContentType=original_content_type,
    )
    s3.put_object(
        Bucket=ARTIFACTS_BUCKET,
        Key=normalized_key,
        Body=normalized_bytes,
        ContentType=normalized_content_type,
    )

    original_artifact = StagedArtifact(
        bucket=ARTIFACTS_BUCKET,
        key=original_key,
        size=len(download.content),
        content_type=original_content_type,
    )
    normalized_artifact = StagedArtifact(
        bucket=ARTIFACTS_BUCKET,
        key=normalized_key,
        size=len(normalized_bytes),
        content_type=normalized_content_type,
    )

    manifest, manifest_key = _build_http_manifest(
        job_id,
        artifact_prefix,
        download,
        original_artifact,
        normalized_artifact,
        fmt=fmt,
        schema_sample=schema_sample,
        row_count=row_count,
    )

    return HttpStagingResult(
        original=original_artifact,
        normalized=normalized_artifact,
        manifest=manifest,
        manifest_key=manifest_key,
        format=fmt,
        schema_sample=list(schema_sample),
        row_count=row_count,
    )


def _json_default(value: Any) -> str:
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


ConnectionPayload = Union[str, Dict[str, Any]]


def _decode_connection_value(raw_value: str, *, field: Optional[str]) -> ConnectionPayload:
    if field:
        return _secret_payload_value(raw_value, field=field)

    stripped = raw_value.strip()
    if not stripped:
        raise ValueError("Connection secret cannot be empty")

    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return stripped

    if not isinstance(parsed, dict):
        raise ValueError("Connection secret JSON must be an object")
    return parsed


def _resolve_connection_payload(source: Dict[str, object]) -> ConnectionPayload:
    """Return inline or secret-backed connection data as either URL string or dict."""

    connection_meta = source.get("connection")
    if connection_meta is not None:
        if not isinstance(connection_meta, dict):
            raise ValueError("SQL connection metadata must be an object")

        conn_type = str(connection_meta.get("type") or "inline").lower()
        secret_field = connection_meta.get("secretField")

        if conn_type in {"inline", "url"}:
            if "url" in connection_meta:
                url = connection_meta.get("url")
                if not isinstance(url, str) or not url.strip():
                    raise ValueError("Inline SQL connection requires a non-empty url")
                return url.strip()
            details = connection_meta.get("details") or connection_meta.get("config")
            if isinstance(details, dict):
                return dict(details)
            raise ValueError("Inline SQL connection requires url or details")

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
            return _decode_connection_value(raw_value, field=secret_field)

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
            return _decode_connection_value(raw_value, field=secret_field)

        raise ValueError(f"Unsupported SQL connection type: {conn_type}")

    details = source.get("connectionDetails") or source.get("credentials")
    if isinstance(details, dict):
        return dict(details)

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
        return _decode_connection_value(raw_value, field=secret_field)

    if parameter_name:
        try:
            response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
        except ClientError as exc:
            raise ValueError(f"Failed to retrieve parameter {parameter_name}: {exc}") from exc
        parameter = response.get("Parameter") or {}
        raw_value = parameter.get("Value")
        if raw_value is None:
            raise ValueError(f"Parameter {parameter_name} does not contain a value")
        return _decode_connection_value(raw_value, field=secret_field)

    raise ValueError(
        "SQL source requires connection data via connection metadata, url, secretArn, or parameterName"
    )


def _resolve_sql_connection(source: Dict[str, object]) -> str:
    """Return a SQLAlchemy connection URL for the provided source."""

    payload = _resolve_connection_payload(source)
    if isinstance(payload, str):
        return payload

    url = payload.get("url") or payload.get("connectionString")
    if isinstance(url, str) and url.strip():
        return url.strip()

    raise ValueError("SQLAlchemy sources require a connection URL")


def _extract_sql_source(
    job_id: str,
    artifact_prefix: str,
    source: Dict[str, object],
    *,
    default_name: str,
) -> SourceRef:
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

    key = f"{artifact_prefix}/input/{filename}"
    s3.put_object(Bucket=ARTIFACTS_BUCKET, Key=key, Body=payload, ContentType=content_type)

    return SourceRef(ARTIFACTS_BUCKET, key)


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _connection_dict(source: Dict[str, object], *, scheme: str) -> Dict[str, Any]:
    payload = _resolve_connection_payload(source)
    if isinstance(payload, dict):
        return dict(payload)

    parsed = urlparse(payload)
    if parsed.scheme and parsed.scheme.split("+", 1)[0] != scheme:
        raise ValueError(f"Expected {scheme} connection URL")

    params = {key: values[-1] for key, values in parse_qs(parsed.query).items() if values}
    database = parsed.path.lstrip("/").split("/", 1)[0] if parsed.path else ""
    schema = parsed.path.lstrip("/").split("/", 1)[1] if "/" in parsed.path.lstrip("/") else ""
    details: Dict[str, Any] = dict(params)
    if parsed.hostname:
        details["host"] = parsed.hostname
    if parsed.port:
        details["port"] = parsed.port
    if parsed.username:
        details["user"] = unquote(parsed.username)
    if parsed.password:
        details["password"] = unquote(parsed.password)
    if database:
        details["database"] = unquote(database)
    if schema:
        details["schema"] = unquote(schema)
    return details


def _connect_redshift(**kwargs):
    try:
        redshift_connector = import_module("redshift_connector")
    except ImportError as exc:
        raise ValueError("redshift_connector is required for Redshift warehouse exports") from exc
    return redshift_connector.connect(**kwargs)


def _connect_snowflake(**kwargs):
    try:
        snowflake_connector = import_module("snowflake.connector")
    except ImportError as exc:
        raise ValueError("snowflake-connector-python is required for Snowflake warehouse exports") from exc
    return snowflake_connector.connect(**kwargs)


def _bigquery_clients(source: Dict[str, object], connection: Dict[str, Any]):
    try:
        bigquery = import_module("google.cloud.bigquery")
        bigquery_storage = import_module("google.cloud.bigquery_storage")
    except ImportError as exc:
        raise ValueError(
            "google-cloud-bigquery and google-cloud-bigquery-storage are required for BigQuery warehouse reads"
        ) from exc

    project = (
        source.get("project")
        or source.get("projectId")
        or connection.get("project")
        or connection.get("project_id")
    )
    client_kwargs = {"project": project} if project else {}
    return bigquery.Client(**client_kwargs), bigquery_storage.BigQueryReadClient(), bigquery, bigquery_storage


def _list_s3_keys(bucket: str, prefix: str) -> List[str]:
    if hasattr(s3, "list_keys"):
        return list(s3.list_keys(Bucket=bucket, Prefix=prefix))

    keys: List[str] = []
    continuation: Dict[str, Any] = {}
    while True:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, **continuation)
        keys.extend(entry["Key"] for entry in response.get("Contents", []))
        if not response.get("IsTruncated"):
            break
        continuation = {"ContinuationToken": response["NextContinuationToken"]}
    return sorted(keys)


def _copy_first_native_export(prefix: str, final_key: str) -> SourceRef:
    candidates = [
        key
        for key in _list_s3_keys(ARTIFACTS_BUCKET, prefix)
        if not key.endswith("/") and not key.endswith("manifest") and not key.endswith("manifest.json")
    ]
    if not candidates:
        raise ValueError("Warehouse native export completed without producing an S3 object")

    s3.copy_object(
        Bucket=ARTIFACTS_BUCKET,
        Key=final_key,
        CopySource={"Bucket": ARTIFACTS_BUCKET, "Key": candidates[0]},
    )
    return SourceRef(ARTIFACTS_BUCKET, final_key)


def _redshift_unload_source(job_id: str, artifact_prefix: str, source: Dict[str, object]) -> SourceRef:
    query = source.get("query")
    if not isinstance(query, str) or not query.strip():
        raise ValueError("Redshift warehouse source requires a query")

    filename = _safe_filename(source.get("filename"), "redshift-export.csv")
    final_key = f"{artifact_prefix}/input/{filename}"
    export_prefix = f"{artifact_prefix}/native/redshift/{job_id}-"
    role = source.get("unloadIamRole") or source.get("iamRole") or source.get("roleArn")
    if not isinstance(role, str) or not role.strip():
        raise ValueError("Redshift UNLOAD requires unloadIamRole, iamRole, or roleArn")

    options = str(source.get("unloadOptions") or "FORMAT AS CSV HEADER ALLOWOVERWRITE PARALLEL OFF")
    statement = (
        f"UNLOAD ('{_escape_sql_literal(query)}') "
        f"TO 's3://{ARTIFACTS_BUCKET}/{export_prefix}' "
        f"IAM_ROLE '{_escape_sql_literal(role)}' {options}"
    )

    connection = _connect_redshift(**_connection_dict(source, scheme="redshift"))
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(statement)
            if hasattr(connection, "commit"):
                connection.commit()
        finally:
            if hasattr(cursor, "close"):
                cursor.close()
    finally:
        if hasattr(connection, "close"):
            connection.close()

    return _copy_first_native_export(export_prefix, final_key)


def _snowflake_copy_into_source(job_id: str, artifact_prefix: str, source: Dict[str, object]) -> SourceRef:
    query = source.get("query")
    if not isinstance(query, str) or not query.strip():
        raise ValueError("Snowflake warehouse source requires a query")

    filename = _safe_filename(source.get("filename"), "snowflake-export.csv")
    final_key = f"{artifact_prefix}/input/{filename}"
    export_prefix = f"{artifact_prefix}/native/snowflake/{job_id}"
    connection_details = _connection_dict(source, scheme="snowflake")

    integration = source.get("storageIntegration") or connection_details.pop("storageIntegration", None)
    credentials = source.get("s3Credentials") or connection_details.pop("s3Credentials", None)
    stage_auth = ""
    if isinstance(integration, str) and integration.strip():
        stage_auth = f" STORAGE_INTEGRATION = {_escape_sql_literal(integration).upper()}"
    elif isinstance(credentials, dict):
        access_key = credentials.get("awsKeyId") or credentials.get("aws_access_key_id")
        secret_key = credentials.get("awsSecretKey") or credentials.get("aws_secret_access_key")
        token = credentials.get("awsToken") or credentials.get("aws_session_token")
        if not access_key or not secret_key:
            raise ValueError("Snowflake s3Credentials require awsKeyId and awsSecretKey")
        token_fragment = f" AWS_TOKEN='{_escape_sql_literal(str(token))}'" if token else ""
        stage_auth = (
            " CREDENTIALS=("
            f"AWS_KEY_ID='{_escape_sql_literal(str(access_key))}' "
            f"AWS_SECRET_KEY='{_escape_sql_literal(str(secret_key))}'"
            f"{token_fragment})"
        )
    else:
        raise ValueError("Snowflake COPY INTO S3 requires storageIntegration or s3Credentials")

    stage_name = f"metricfoundry_{job_id.replace('-', '_')}_stage"
    stage_url = f"s3://{ARTIFACTS_BUCKET}/{export_prefix}/"
    file_format = str(
        source.get("copyFileFormat")
        or "TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"' COMPRESSION = NONE"
    )
    copy_options = str(source.get("copyOptions") or "HEADER = TRUE SINGLE = TRUE OVERWRITE = TRUE")

    connection = _connect_snowflake(**connection_details)
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(
                f"CREATE OR REPLACE TEMPORARY STAGE {stage_name} URL='{_escape_sql_literal(stage_url)}'{stage_auth}"
            )
            cursor.execute(
                f"COPY INTO @{stage_name}/{_escape_sql_literal(filename)} "
                f"FROM ({query}) FILE_FORMAT = ({file_format}) {copy_options}"
            )
        finally:
            if hasattr(cursor, "close"):
                cursor.close()
    finally:
        if hasattr(connection, "close"):
            connection.close()

    return _copy_first_native_export(export_prefix, final_key)


def _write_bigquery_rows(
    rows: Iterable[Any],
    schema_fields: Sequence[Any],
    key: str,
    export_format: str,
) -> SourceRef:
    if export_format == "jsonl":
        lines = []
        for row in rows:
            if hasattr(row, "items"):
                payload = dict(row.items())
            elif isinstance(row, dict):
                payload = row
            else:
                payload = dict(row)
            lines.append(json.dumps(payload, default=_json_default))
        body = "\n".join(lines).encode("utf-8")
        content_type = "application/json"
    else:
        columns = [field.name for field in schema_fields]
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        for row in rows:
            if hasattr(row, "get"):
                writer.writerow([row.get(column) for column in columns])
            elif isinstance(row, dict):
                writer.writerow([row.get(column) for column in columns])
            else:
                writer.writerow([getattr(row, column, "") for column in columns])
        body = buffer.getvalue().encode("utf-8")
        content_type = "text/csv"

    s3.put_object(Bucket=ARTIFACTS_BUCKET, Key=key, Body=body, ContentType=content_type)
    return SourceRef(ARTIFACTS_BUCKET, key)


def _bigquery_storage_read_source(job_id: str, artifact_prefix: str, source: Dict[str, object]) -> SourceRef:
    connection_payload = _resolve_connection_payload(source)
    connection = connection_payload if isinstance(connection_payload, dict) else {}
    bq_client, read_client, bigquery, bigquery_storage = _bigquery_clients(source, connection)

    filename = _safe_filename(source.get("filename"), "bigquery-export.csv")
    export_format = str(source.get("format") or "csv").lower()
    if export_format not in {"csv", "jsonl"}:
        raise ValueError("BigQuery export format must be 'csv' or 'jsonl'")
    final_key = f"{artifact_prefix}/input/{filename}"

    table_ref = source.get("table") or source.get("tableId")
    query = source.get("query")
    if isinstance(query, str) and query.strip():
        job_config = getattr(bigquery, "QueryJobConfig", lambda **kwargs: kwargs)(
            use_legacy_sql=bool(source.get("useLegacySql", False))
        )
        query_job = bq_client.query(query, job_config=job_config)
        query_job.result()
        table_ref = query_job.destination

    if not table_ref:
        dataset = source.get("dataset") or source.get("datasetId")
        table = source.get("tableName") or source.get("table")
        project = source.get("project") or source.get("projectId") or connection.get("project") or connection.get("project_id")
        if not dataset or not table:
            raise ValueError("BigQuery warehouse source requires query or table metadata")
        table_ref = f"{project}.{dataset}.{table}" if project else f"{dataset}.{table}"

    table = bq_client.get_table(table_ref)
    parent = f"projects/{bq_client.project}"
    table_path = f"projects/{table.project}/datasets/{table.dataset_id}/tables/{table.table_id}"
    read_options = bigquery_storage.types.ReadSession.TableReadOptions()
    selected_fields = source.get("selectedFields")
    if isinstance(selected_fields, list):
        read_options.selected_fields = [str(field) for field in selected_fields]
    row_restriction = source.get("rowRestriction")
    if isinstance(row_restriction, str) and row_restriction.strip():
        read_options.row_restriction = row_restriction

    read_session = bigquery_storage.types.ReadSession(
        table=table_path,
        data_format=bigquery_storage.types.DataFormat.ARROW,
        read_options=read_options,
    )
    session = read_client.create_read_session(
        parent=parent,
        read_session=read_session,
        max_stream_count=int(source.get("maxStreamCount") or 1),
    )

    rows: List[Any] = []
    for stream in session.streams:
        reader = read_client.read_rows(stream.name)
        rows.extend(reader.rows(session))

    return _write_bigquery_rows(rows, table.schema, final_key, export_format)


def _extract_warehouse_source(job_id: str, artifact_prefix: str, source: Dict[str, object]) -> Tuple[SourceRef, str]:
    warehouse_type = str(source.get("warehouseType") or "warehouse").lower()
    if warehouse_type == "redshift":
        return _redshift_unload_source(job_id, artifact_prefix, source), "warehouse:redshift"
    if warehouse_type == "snowflake":
        return _snowflake_copy_into_source(job_id, artifact_prefix, source), "warehouse:snowflake"
    if warehouse_type == "bigquery":
        return _bigquery_storage_read_source(job_id, artifact_prefix, source), "warehouse:bigquery"

    ref = _extract_sql_source(job_id, artifact_prefix, source, default_name=f"{warehouse_type}-export.csv")
    return ref, f"warehouse:{warehouse_type}"


def _resolve_source(job_id: str, artifact_prefix: str, item: Dict[str, Dict]) -> Tuple[object, str]:
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
        return _download_http_source(job_id, artifact_prefix, source), "http"

    if source_type == "database":
        ref = _extract_sql_source(job_id, artifact_prefix, source, default_name="database-export.csv")
        return ref, source_type

    if source_type == "warehouse":
        return _extract_warehouse_source(job_id, artifact_prefix, source)

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


def _stage_source(job_id: str, artifact_prefix: str, src: SourceRef) -> StagedArtifact:
    filename = src.filename or "source"
    dest_key = f"{artifact_prefix}/input/{filename}"

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


def handler(event: Mapping[str, Any], _context: Any) -> Dict[str, Any]:
    """Stage source input into the artifacts bucket and persist staging metadata."""
    job_id = event.get("jobId")
    if not job_id:
        raise ValueError("jobId is required")

    logger.info("starting staging for job %s", job_id)

    # Fetch job metadata
    res = _table().get_item(Key={"pk": f"job#{job_id}", "sk": "meta"})
    item = res.get("Item")
    if not item:
        raise ValueError(f"Job {job_id} not found")

    artifact_prefix = _job_artifact_prefix(item, job_id)
    try:
        src, source_type = _resolve_source(job_id, artifact_prefix, item)
    except (ValueError, ClientError, DBAPIError, NoSuchModuleError, requests.RequestException) as exc:
        logger.error("failed resolving source for job %s: %s", job_id, exc)
        _ddb_update(job_id, STATUS_FAILED, error=str(exc))
        raise

    # Mark job as staging (idempotent)
    _ddb_update(job_id, STATUS_STAGING)

    if source_type == "upload":
        _wait_for_upload(src)

    try:
        if source_type == "http":
            if not isinstance(src, HttpDownload):
                raise ValueError("HTTP source resolution failed")
            http_result = _stage_http_source(job_id, artifact_prefix, src)
            staged = http_result.normalized
        else:
            http_result = None
            staged = _stage_source(job_id, artifact_prefix, src)
    except FileNotReadyError:
        # Should never reach here due to early check, but propagate just in case.
        raise
    except (ValueError, ClientError, DBAPIError, NoSuchModuleError, requests.RequestException) as exc:
        logger.error("failed staging source object for job %s: %s", job_id, exc)
        _ddb_update(job_id, STATUS_FAILED, error=str(exc))
        raise

    if source_type == "http" and http_result is not None:
        metadata = {
            "size": staged.size,
            "format": http_result.format,
            "contentType": staged.content_type,
            "sourceType": source_type,
            "schemaSample": http_result.schema_sample,
            "rowCountEstimate": http_result.row_count,
            "originalKey": http_result.original.key,
            "normalizedKey": http_result.normalized.key,
            "manifestKey": http_result.manifest_key,
        }

        _ddb_update(
            job_id,
            STATUS_STAGED,
            inputKey=staged.key,
            inputMetadata=metadata,
            manifestKey=http_result.manifest_key,
        )

        logger.info(
            "staged HTTP data at %s (format=%s size=%s rows=%s)",
            staged.path,
            http_result.format,
            staged.size,
            http_result.row_count,
        )

        return {
            "jobId": job_id,
            "input": {"bucket": staged.bucket, "key": staged.key},
            "metadata": metadata,
            "artifactPrefix": artifact_prefix,
            "manifest": http_result.manifest,
            "manifestKey": http_result.manifest_key,
        }

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

    logger.info("staged data at %s (format=%s size=%s)", staged.path, fmt, staged.size)

    return {
        "jobId": job_id,
        "input": {"bucket": staged.bucket, "key": staged.key},
        "metadata": metadata,
        "artifactPrefix": artifact_prefix,
    }


def lambda_handler(event: Mapping[str, Any], context: Any) -> Dict[str, Any]:
    """API-shaped wrapper for direct Lambda invocation."""
    try:
        return {"statusCode": 200, "body": handler(event, context)}
    except FileNotReadyError as exc:
        return {"statusCode": 409, "body": {"error": str(exc)}}
    except ValueError as exc:
        return {"statusCode": 400, "body": {"error": str(exc)}}
    except (ClientError, DBAPIError, NoSuchModuleError, requests.RequestException):
        return {"statusCode": 502, "body": {"error": "Failed to stage source data"}}
