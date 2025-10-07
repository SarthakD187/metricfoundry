"""Lambda to stage job source data into the artifacts bucket."""
import os
import time
from dataclasses import dataclass
from typing import Dict, Tuple

import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")

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


def _parse_source(item: Dict[str, Dict]) -> Tuple[SourceRef, str]:
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

    src, source_type = _parse_source(item)

    # Mark job as staging (idempotent)
    _ddb_update(job_id, STATUS_STAGING)

    if source_type == "upload":
        _wait_for_upload(src)

    filename = src.filename or "source"
    dest_key = f"artifacts/{job_id}/input/{filename}"

    try:
        _copy_object(src, dest_key)
    except FileNotReadyError:
        # Should never reach here due to early check, but propagate just in case.
        raise
    except Exception as exc:
        print(f"[Stage] ERROR copying object: {exc}")
        _ddb_update(job_id, STATUS_FAILED, error=str(exc))
        raise

    _ddb_update(job_id, STATUS_STAGED, inputKey=dest_key)

    print(f"[Stage] Staged data at s3://{ARTIFACTS_BUCKET}/{dest_key}")

    return {
        "jobId": job_id,
        "input": {"bucket": ARTIFACTS_BUCKET, "key": dest_key},
    }
