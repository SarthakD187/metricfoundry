import csv
import importlib
import io
import json
import sys
import zipfile
from pathlib import Path
from typing import Callable

import pytest

from .utils.aws import (
    FakeDynamoResource,
    FakeDynamoTable,
    FakeS3,
    FakeSSM,
    FakeSecretsManager,
)
from .utils.langgraph import ensure_langgraph_stub


SAMPLE_ROWS = [
    {"id": index, "value": index * 2}
    for index in range(1, 6)
]

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def _csv_bytes(rows) -> bytes:
    buffer = io.StringIO()
    fieldnames = list(rows[0].keys())
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue().encode("utf-8")


def _zip_bytes(name: str, payload: bytes) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name, payload)
    return buffer.getvalue()


@pytest.fixture()
def stage_and_processor(monkeypatch):
    ensure_langgraph_stub()
    monkeypatch.setenv("JOBS_TABLE", "jobs-table")
    monkeypatch.setenv("ARTIFACTS_BUCKET", "artifacts-bucket")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")

    stage_module = importlib.import_module("lambdas.stage.handler")
    importlib.reload(stage_module)
    processor_module = importlib.import_module("lambdas.processor.handler")
    importlib.reload(processor_module)

    fake_s3 = FakeS3()
    fake_table = FakeDynamoTable()
    fake_secrets = FakeSecretsManager()
    fake_ssm = FakeSSM()

    stage_module.s3 = fake_s3
    stage_module.ddb = FakeDynamoResource(fake_table)
    stage_module.secretsmanager = fake_secrets
    stage_module.ssm = fake_ssm

    processor_module.s3 = fake_s3
    processor_module.ddb = FakeDynamoResource(fake_table)

    return stage_module, processor_module, fake_s3, fake_table


@pytest.mark.parametrize(
    "filename, body_factory, content_type, expected_format, patch_parquet",
    [
        (
            "dataset.csv",
            lambda: _csv_bytes(SAMPLE_ROWS),
            "text/csv",
            "csv",
            False,
        ),
        (
            "dataset.parquet",
            lambda: _csv_bytes(SAMPLE_ROWS),
            "application/octet-stream",
            "parquet",
            True,
        ),
        (
            "dataset.zip",
            lambda: _zip_bytes("dataset.csv", _csv_bytes(SAMPLE_ROWS)),
            "application/zip",
            "zip",
            False,
        ),
    ],
)
def test_stage_and_processor_pipeline(
    stage_and_processor,
    monkeypatch,
    filename: str,
    body_factory: Callable[[], bytes],
    content_type: str,
    expected_format: str,
    patch_parquet: bool,
):
    stage_module, processor_module, fake_s3, fake_table = stage_and_processor
    job_id = f"job-{expected_format}"
    source_bucket = "incoming"
    source_key = f"uploads/{filename}"
    body = body_factory()

    fake_table.put_item(
        {
            "pk": f"job#{job_id}",
            "sk": "meta",
            "status": "QUEUED",
            "source": {"type": "upload", "bucket": source_bucket, "key": source_key},
        }
    )

    fake_s3.put_object(Bucket=source_bucket, Key=source_key, Body=body, ContentType=content_type)

    stage_result = stage_module.handler({"jobId": job_id}, None)
    assert stage_result["metadata"]["format"] == expected_format

    if patch_parquet:
        graph_module = importlib.import_module("services.workers.graph.graph")

        def _parquet_as_csv(data: bytes):
            return graph_module._ingest_csv("dataset.parquet", data)

        monkeypatch.setattr(graph_module, "_ingest_parquet", _parquet_as_csv)

    response = processor_module.main({"jobId": job_id, "input": stage_result["input"]}, None)

    assert response["ok"] is True
    assert response["resultKey"].endswith("results.json")
    assert response["manifestKey"].endswith("manifest.json")

    record = fake_table.get_item({"pk": f"job#{job_id}", "sk": "meta"}).get("Item")
    assert record is not None
    assert record["status"] == processor_module.STATUS_SUCCEEDED
    assert record["resultKey"] == response["resultKey"]
    assert record["manifestKey"] == response["manifestKey"]

    results_obj = fake_s3.get_object(Bucket=processor_module.ARTIFACTS_BUCKET, Key=response["resultKey"])
    results_payload = json.loads(results_obj["Body"].read().decode("utf-8"))
    assert results_payload["metrics"]["rows"] == len(SAMPLE_ROWS)
    assert results_payload["metrics"]["columns"] == len(SAMPLE_ROWS[0])
    assert results_payload["mlInference"]["status"] in {"failed", "skipped", "completed"}

    manifest_obj = fake_s3.get_object(Bucket=processor_module.ARTIFACTS_BUCKET, Key=response["manifestKey"])
    manifest = json.loads(manifest_obj["Body"].read().decode("utf-8"))
    artifact_keys = {entry["key"] for entry in manifest["artifacts"]}
    assert f"artifacts/{job_id}/results/results.json" in artifact_keys
    assert f"artifacts/{job_id}/results/manifest.json" in artifact_keys
    assert any(key.endswith("analytics_bundle.zip") for key in artifact_keys)

    all_keys = list(fake_s3.list_keys(processor_module.ARTIFACTS_BUCKET, Prefix=f"artifacts/{job_id}/"))
    assert f"artifacts/{job_id}/phases/ingest.json" in all_keys
    assert response["resultKey"] in all_keys
    assert response["manifestKey"] in all_keys
