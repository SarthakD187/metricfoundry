import os
import json
import io
import csv
import time
from typing import Dict, Any, Optional, Tuple
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError

# ---- AWS clients/resources ----
s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")

# ---- Env ----
TABLE_NAME = os.environ["JOBS_TABLE"]                     # DynamoDB table name
ARTIFACTS_BUCKET = os.environ.get("ARTIFACTS_BUCKET")     # Bucket results will be written to

# ---- Constants ----
STATUS_RUNNING = "RUNNING"
STATUS_SUCCEEDED = "SUCCEEDED"
STATUS_FAILED = "FAILED"
DDB_PK_PREFIX = "job#"
DDB_SK_META = "meta"

# ---- Helpers: DDB ----
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
        Key={"pk": f"{DDB_PK_PREFIX}{job_id}", "sk": DDB_SK_META},
        UpdateExpression="SET " + ", ".join(set_clauses),
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_vals,
    )

# ---- Helpers: S3 path parsing ----
def parse_job_from_key(key: str) -> Optional[str]:
    """
    Accepts either:
      - artifacts/<jobId>/input/...
      - jobs/<jobId>/raw/...
    Returns jobId or None.
    """
    parts = key.split("/")
    if len(parts) >= 4:
        if parts[0] == "artifacts" and parts[2] == "input":
            return parts[1]
        if parts[0] == "jobs" and parts[2] == "raw":
            return parts[1]
    return None

def result_key_for(job_id: str) -> str:
    return f"artifacts/{job_id}/results/results.json"

def object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NotFound", "NoSuchKey"):
            return False
        raise

# ---- Metrics ----
_NULL_SENTINELS = {None, "", "null", "NULL", "NaN", "nan"}

def compute_metrics_from_csv(body: bytes) -> Dict[str, Any]:
    text = body.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    cols = reader.fieldnames or []
    rows = 0
    nulls = {c: 0 for c in cols}
    for row in reader:
        rows += 1
        for c in cols:
            if row.get(c) in _NULL_SENTINELS:
                nulls[c] += 1
    return {"rows": rows, "columns": cols, "null_counts": nulls}

def compute_metrics_from_json(body: bytes) -> Dict[str, Any]:
    data = json.loads(body)
    records = data if isinstance(data, list) else [data]
    cols = sorted({k for r in records if isinstance(r, dict) for k in r.keys()})
    return {"rows": len(records), "columns": cols, "null_counts": {}}

def compute_metrics_from_jsonl(body: bytes) -> Dict[str, Any]:
    text = body.decode("utf-8", errors="replace")
    cols_set = set()
    rows = 0
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            # skip malformed lines instead of failing the whole job
            continue
        rows += 1
        if isinstance(obj, dict):
            cols_set.update(obj.keys())
    return {"rows": rows, "columns": sorted(cols_set), "null_counts": {}}

def compute_metrics(key: str, body: bytes) -> Dict[str, Any]:
    k = key.lower()
    if k.endswith(".csv"):
        return compute_metrics_from_csv(body)
    if k.endswith(".jsonl") or k.endswith(".ndjson"):
        return compute_metrics_from_jsonl(body)
    if k.endswith(".json"):
        return compute_metrics_from_json(body)
    # Default: unknown type
    return {"rows": None, "columns": [], "null_counts": {}}

# ---- Lambda entrypoint ----
def main(event, _ctx):
    # Extract S3 info from event
    rec = event["Records"][0]["s3"]
    bucket = rec["bucket"]["name"]
    key = unquote_plus(rec["object"]["key"])  # handle URL-encoded keys

    print(f"[ProcessorFn] Received event for s3://{bucket}/{key}")

    # Determine jobId
    job_id = parse_job_from_key(key)
    if not job_id:
        print(f"[ProcessorFn] Skip: unexpected key layout: {key}")
        return {"skip": True, "key": key}

    # Mark as RUNNING (idempotent upsert)
    try:
        ddb_upsert_status(job_id, STATUS_RUNNING, inputKey=key)
    except Exception as e:
        # Do not fail the entire run if DDB write has a transient issue; log & continue
        print(f"[ProcessorFn] Warning: failed to upsert RUNNING status: {e}")

    # Idempotency: if results already exist, return success
    rkey = result_key_for(job_id)
    try:
        if ARTIFACTS_BUCKET and object_exists(ARTIFACTS_BUCKET, rkey):
            print(f"[ProcessorFn] Results already exist at s3://{ARTIFACTS_BUCKET}/{rkey} (idempotent skip).")
            return {"ok": True, "jobId": job_id, "resultKey": rkey, "idempotent": True}
    except Exception as e:
        print(f"[ProcessorFn] Warning: head_object failed for existing results check: {e}")

    # Read the uploaded object, compute metrics, write results
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()

        metrics = compute_metrics(key, body)

        target_bucket = ARTIFACTS_BUCKET or bucket  # default to source bucket if env not set
        s3.put_object(
            Bucket=target_bucket,
            Key=rkey,
            Body=json.dumps({"jobId": job_id, "metrics": metrics}, indent=2).encode(),
            ContentType="application/json",
        )

        # Mark SUCCEEDED
        try:
            ddb_upsert_status(job_id, STATUS_SUCCEEDED, resultKey=rkey)
        except Exception as e:
            print(f"[ProcessorFn] Warning: failed to upsert SUCCEEDED status: {e}")

        print(f"[ProcessorFn] Wrote results to s3://{target_bucket}/{rkey}")
        return {"ok": True, "jobId": job_id, "resultKey": rkey}

    except Exception as e:
        # On any failure, update FAILED with error text (truncated)
        err_txt = f"{type(e).__name__}: {e}"
        print(f"[ProcessorFn] ERROR: {err_txt}")

        try:
            ddb_upsert_status(job_id, STATUS_FAILED, error=err_txt[:1000])
        except Exception as e2:
            print(f"[ProcessorFn] Warning: failed to upsert FAILED status: {e2}")

        # Best-effort error artifact (helps debugging from the UI)
        try:
            target_bucket = ARTIFACTS_BUCKET or bucket
            s3.put_object(
                Bucket=target_bucket,
                Key=rkey.replace("results.json", "error.json"),
                Body=json.dumps({"jobId": job_id, "error": err_txt}, indent=2).encode(),
                ContentType="application/json",
            )
        except Exception as e3:
            print(f"[ProcessorFn] Warning: failed to write error artifact: {e3}")

        # Re-raise so the Lambda invocation is marked as a failure (visible in CW logs/metrics)
        raise
