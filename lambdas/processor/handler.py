"""Analytics processor Lambda entrypoint."""
from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Mapping, Optional

import boto3
from botocore.exceptions import ClientError

from services.common.pipeline import (
    ANALYSIS_VERSION,
    build_results_payload,
    persist_pipeline_outputs,
    result_key_for,
    summarize_phase_payload,
)
from services.workers.graph.graph import PHASE_ORDER, run_pipeline

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")

TABLE_NAME = os.environ["JOBS_TABLE"]
ARTIFACTS_BUCKET = os.environ.get("ARTIFACTS_BUCKET")

STATUS_RUNNING = "RUNNING"
STATUS_SUCCEEDED = "SUCCEEDED"
STATUS_FAILED = "FAILED"


def ddb_table():
    return ddb.Table(TABLE_NAME)


def now_epoch() -> int:
    return int(time.time())


def ddb_upsert_status(job_id: str, status: str, **attrs) -> None:
    expr_names = {"#s": "status", "#u": "updatedAt"}
    expr_vals: Dict[str, Any] = {":s": status, ":u": now_epoch()}
    set_clauses = ["#s = :s", "#u = :u"]

    for key, value in attrs.items():
        placeholder = f":{key}"
        expr_vals[placeholder] = value
        name_alias = f"#{key}"
        expr_names[name_alias] = key
        set_clauses.append(f"{name_alias} = {placeholder}")

    ddb_table().update_item(
        Key={"pk": f"job#{job_id}", "sk": "meta"},
        UpdateExpression="SET " + ", ".join(set_clauses),
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_vals,
    )


def object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in ("404", "NotFound", "NoSuchKey"):
            return False
        raise


def _put_object(bucket: str, key: str, body: bytes, content_type: str) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)


def _default_callback(job_id: str):
    def _callback(phase: str, payload: Mapping[str, Any], index: int, total: int) -> None:
        progress = int(((index + 1) / total) * 100)
        summary = summarize_phase_payload(payload)
        try:
            ddb_upsert_status(
                job_id,
                STATUS_RUNNING,
                currentPhase=phase,
                phaseIndex=index,
                phaseCount=total,
                progress=progress,
                phaseSummary=summary,
            )
        except Exception as exc:  # pragma: no cover - DynamoDB errors shouldn't halt job
            print(f"[ProcessorFn] Warning: failed to stream phase status for {phase}: {exc}")

    return _callback


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
    except Exception as exc:
        print(f"[ProcessorFn] Warning: failed to upsert initial RUNNING status: {exc}")

    artifact_prefix = f"artifacts/{job_id}"
    results_key = result_key_for(job_id)

    try:
        if ARTIFACTS_BUCKET and object_exists(ARTIFACTS_BUCKET, results_key):
            print(
                f"[ProcessorFn] Results already exist at s3://{ARTIFACTS_BUCKET}/{results_key} (idempotent skip)."
            )
            return {"ok": True, "jobId": job_id, "resultKey": results_key, "idempotent": True}
    except Exception as exc:
        print(f"[ProcessorFn] Warning: head_object failed for existing results check: {exc}")

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
        artifact_keys = persist_pipeline_outputs(job_id, target_bucket, result, s3_client=s3)

        results_payload = build_results_payload(
            job_id,
            result,
            source_input=payload_input,
            artifact_bucket=target_bucket,
            analysis_version=ANALYSIS_VERSION,
        )
        _put_object(
            target_bucket,
            results_key,
            json.dumps(results_payload, indent=2, default=str).encode("utf-8"),
            "application/json",
        )

        try:
            ddb_upsert_status(
                job_id,
                STATUS_SUCCEEDED,
                resultKey=results_key,
                manifestKey=artifact_keys.get("manifest"),
                completedAt=now_epoch(),
            )
        except Exception as exc:
            print(f"[ProcessorFn] Warning: failed to upsert SUCCEEDED status: {exc}")

        print(f"[ProcessorFn] Wrote results to s3://{target_bucket}/{results_key}")
        return {"ok": True, "jobId": job_id, "resultKey": results_key, "manifestKey": artifact_keys.get("manifest")}

    except Exception as exc:
        err_txt = f"{type(exc).__name__}: {exc}"
        print(f"[ProcessorFn] ERROR: {err_txt}")

        try:
            ddb_upsert_status(job_id, STATUS_FAILED, error=err_txt[:1000])
        except Exception as update_exc:
            print(f"[ProcessorFn] Warning: failed to upsert FAILED status: {update_exc}")

        try:
            target_bucket = ARTIFACTS_BUCKET or bucket
            error_key = results_key.replace("results.json", "error.json")
            _put_object(
                target_bucket,
                error_key,
                json.dumps({"jobId": job_id, "error": err_txt}, indent=2, default=str).encode("utf-8"),
                "application/json",
            )
        except Exception as write_exc:
            print(f"[ProcessorFn] Warning: failed to write error artifact: {write_exc}")

        raise
