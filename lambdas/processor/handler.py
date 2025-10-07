import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Protocol

import boto3
from botocore.exceptions import ClientError

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


def object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NotFound", "NoSuchKey"):
            return False
        raise


_NULL_SENTINELS = {None, "", "null", "NULL", "NaN", "nan"}

ANALYSIS_VERSION = "2024.05"


@dataclass
class DatasetDescriptor:
    """Lightweight summary of the ingested dataset."""

    rows: int | None
    columns: List[str]
    null_counts: Dict[str, int]


class AnalyticsContext:
    """Shared state that steps can use to communicate results."""

    def __init__(self, job_id: str, source_key: str, dataset: DatasetDescriptor):
        self.job_id = job_id
        self.source_key = source_key
        self.dataset = dataset
        self.results: Dict[str, Dict[str, Any]] = {}

    def record(self, step_name: str, payload: Mapping[str, Any]) -> Dict[str, Any]:
        result = dict(payload)
        self.results[step_name] = result
        return result


class AnalyticsStep(Protocol):
    name: str

    def run(self, context: AnalyticsContext) -> Mapping[str, Any]:
        ...


class ProfilingStep:
    name = "profiling"

    def run(self, context: AnalyticsContext) -> Mapping[str, Any]:
        dataset = context.dataset
        return context.record(
            self.name,
            {
                "rows": dataset.rows,
                "columns": dataset.columns,
                "source": context.source_key,
            },
        )


class StatisticsStep:
    name = "statistics"

    def run(self, context: AnalyticsContext) -> Mapping[str, Any]:
        dataset = context.dataset
        return context.record(
            self.name,
            {
                "null_counts": dataset.null_counts,
            },
        )


class VisualizationStep:
    name = "visualizations"

    def run(self, context: AnalyticsContext) -> Mapping[str, Any]:
        return context.record(
            self.name,
            {
                "artifacts": [],
                "status": "pending",
                "message": "Visualization rendering not yet implemented.",
            },
        )


class MLInferenceStep:
    name = "ml_inference"

    def run(self, context: AnalyticsContext) -> Mapping[str, Any]:
        return context.record(
            self.name,
            {
                "models": [],
                "status": "pending",
                "message": "ML inference pipeline not yet implemented.",
            },
        )


class AnalyticsWorkflow:
    """Coordinator that executes analytics steps sequentially."""

    def __init__(self, steps: List[AnalyticsStep]):
        self.steps = steps

    def run(self, context: AnalyticsContext) -> Dict[str, Dict[str, Any]]:
        for step in self.steps:
            try:
                step.run(context)
            except Exception as exc:  # pragma: no cover - defensive logging
                context.record(
                    step.name,
                    {
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                )
                raise
        return context.results


def _compute_descriptor_from_csv(body: bytes) -> DatasetDescriptor:
    import csv
    import io

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
    return DatasetDescriptor(rows=rows, columns=cols, null_counts=nulls)


def _compute_descriptor_from_json(body: bytes) -> DatasetDescriptor:
    data = json.loads(body)
    records = data if isinstance(data, list) else [data]
    cols = sorted({k for r in records if isinstance(r, dict) for k in r.keys()})
    return DatasetDescriptor(rows=len(records), columns=cols, null_counts={})


def _compute_descriptor_from_jsonl(body: bytes) -> DatasetDescriptor:
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
            continue
        rows += 1
        if isinstance(obj, dict):
            cols_set.update(obj.keys())
    return DatasetDescriptor(rows=rows, columns=sorted(cols_set), null_counts={})


def compute_dataset_descriptor(key: str, body: bytes) -> DatasetDescriptor:
    k = key.lower()
    if k.endswith(".csv"):
        return _compute_descriptor_from_csv(body)
    if k.endswith(".jsonl") or k.endswith(".ndjson"):
        return _compute_descriptor_from_jsonl(body)
    if k.endswith(".json"):
        return _compute_descriptor_from_json(body)
    return DatasetDescriptor(rows=None, columns=[], null_counts={})


def run_workflow(job_id: str, key: str, body: bytes) -> Dict[str, Any]:
    descriptor = compute_dataset_descriptor(key, body)
    context = AnalyticsContext(job_id=job_id, source_key=key, dataset=descriptor)
    workflow = AnalyticsWorkflow(
        [
            ProfilingStep(),
            StatisticsStep(),
            VisualizationStep(),
            MLInferenceStep(),
        ]
    )
    phases = workflow.run(context)

    metrics: Dict[str, Any] = {}
    profiling = phases.get("profiling", {})
    statistics = phases.get("statistics", {})
    if profiling:
        metrics.update({k: profiling.get(k) for k in ("rows", "columns")})
    if statistics:
        metrics["null_counts"] = statistics.get("null_counts", {})

    return {
        "jobId": job_id,
        "analysisVersion": ANALYSIS_VERSION,
        "phases": phases,
        "metrics": metrics,
    }


def main(event, _ctx):
    job_id = event.get("jobId")
    payload_input = event.get("input") or {}
    bucket = payload_input.get("bucket")
    key = payload_input.get("key")

    if not job_id or not bucket or not key:
        raise ValueError("jobId, input.bucket, and input.key are required")

    print(f"[ProcessorFn] Processing job {job_id} using s3://{bucket}/{key}")

    try:
        ddb_upsert_status(job_id, STATUS_RUNNING, inputKey=key)
    except Exception as e:
        print(f"[ProcessorFn] Warning: failed to upsert RUNNING status: {e}")

    rkey = result_key_for(job_id)

    try:
        if ARTIFACTS_BUCKET and object_exists(ARTIFACTS_BUCKET, rkey):
            print(
                f"[ProcessorFn] Results already exist at s3://{ARTIFACTS_BUCKET}/{rkey} (idempotent skip)."
            )
            return {"ok": True, "jobId": job_id, "resultKey": rkey, "idempotent": True}
    except Exception as e:
        print(f"[ProcessorFn] Warning: head_object failed for existing results check: {e}")

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()

        results = run_workflow(job_id, key, body)

        target_bucket = ARTIFACTS_BUCKET or bucket
        s3.put_object(
            Bucket=target_bucket,
            Key=rkey,
            Body=json.dumps(results, indent=2).encode(),
            ContentType="application/json",
        )

        try:
            ddb_upsert_status(job_id, STATUS_SUCCEEDED, resultKey=rkey)
        except Exception as e:
            print(f"[ProcessorFn] Warning: failed to upsert SUCCEEDED status: {e}")

        print(f"[ProcessorFn] Wrote results to s3://{target_bucket}/{rkey}")
        return {"ok": True, "jobId": job_id, "resultKey": rkey}

    except Exception as e:
        err_txt = f"{type(e).__name__}: {e}"
        print(f"[ProcessorFn] ERROR: {err_txt}")

        try:
            ddb_upsert_status(job_id, STATUS_FAILED, error=err_txt[:1000])
        except Exception as e2:
            print(f"[ProcessorFn] Warning: failed to upsert FAILED status: {e2}")

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

        raise
