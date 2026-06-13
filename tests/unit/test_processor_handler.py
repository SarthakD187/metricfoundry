"""Unit tests for lambdas/processor/handler.py using moto + pipeline stub."""
from __future__ import annotations

import importlib
import io
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Mapping
from urllib import error as urllib_error

import boto3
import pytest
from moto import mock_aws

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Provide a langgraph stub so the processor module can be imported
from tests.integration.utils.langgraph import ensure_langgraph_stub

ensure_langgraph_stub()

TABLE = "test-jobs"
BUCKET = "test-artifacts"
REGION = "us-east-1"
JOB_ID = "job-processor-test"


# ---------------------------------------------------------------------------
# Pipeline result stub
# ---------------------------------------------------------------------------


def _stub_result(job_id: str = JOB_ID, rows: int = 5, cols: int = 2) -> Any:
    return SimpleNamespace(
        phases={
            "ingest": {"summary": "ingested", "sourceFormat": "csv"},
            "profile": {
                "columnProfiles": [{"name": "id", "inferredType": "integer"}, {"name": "value", "inferredType": "float"}],
                "shape": {"rows": rows, "columns": cols},
            },
            "descriptive_stats": {"metrics": {"rows": rows}},
            "nl_report": {"summary": "done"},
            "finalize": {"summary": "finalized"},
        },
        metrics={"rows": rows, "columns": cols, "bytesRead": 200},
        manifest={
            "jobId": job_id,
            "artifacts": [
                {"key": f"artifacts/{job_id}/results/results.json"},
                {"key": f"artifacts/{job_id}/results/manifest.json"},
            ],
        },
        artifact_contents={
            "results/report.html": {"kind": "html", "html": "<p>test</p>"},
            "results/report.txt": {"kind": "text", "text": "analysis report"},
        },
        correlations=[],
        outliers=[],
        ml_inference={"status": "skipped"},
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_resources(ddb_resource, s3_client):
    table = ddb_resource.create_table(
        TableName=TABLE,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    s3_client.create_bucket(Bucket=BUCKET)
    return table


def _seed_job(table, job_id: str, status: str = "STAGED") -> None:
    table.put_item(
        Item={
            "pk": f"job#{job_id}",
            "sk": "meta",
            "status": status,
            "createdAt": 0,
            "updatedAt": 0,
        }
    )


def _put_csv(s3_client, bucket: str, key: str, body: bytes = b"id,value\n1,10\n2,20\n") -> None:
    s3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="text/csv")


@pytest.fixture()
def processor_module(monkeypatch):
    monkeypatch.setenv("JOBS_TABLE", TABLE)
    monkeypatch.setenv("ARTIFACTS_BUCKET", BUCKET)
    monkeypatch.setenv("WORKER_INVOKE_MODE", "embedded")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.delenv("WORKER_ARN", raising=False)
    monkeypatch.delenv("WORKER_URL", raising=False)

    mod = importlib.import_module("lambdas.processor.handler")
    return importlib.reload(mod)


# ---------------------------------------------------------------------------
# main() — embedded mode
# ---------------------------------------------------------------------------


@mock_aws
def test_main_embedded_mode_succeeds(processor_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_resources(ddb, s3)
    _seed_job(table, JOB_ID)

    source_key = f"artifacts/{JOB_ID}/input/data.csv"
    _put_csv(s3, BUCKET, source_key)

    # Patch run_pipeline on the handler module (it imports it by name)
    stub = _stub_result(JOB_ID)
    monkeypatch.setattr(processor_module, "run_pipeline", lambda *a, **kw: stub)
    monkeypatch.setattr(processor_module, "s3", s3)
    monkeypatch.setattr(processor_module, "ddb", ddb)

    event = {"jobId": JOB_ID, "input": {"bucket": BUCKET, "key": source_key}, "artifactPrefix": f"artifacts/{JOB_ID}"}
    result = processor_module.main(event, None)

    assert result["ok"] is True
    assert result["jobId"] == JOB_ID
    assert result["resultKey"].endswith("results.json")
    assert result["manifestKey"].endswith("manifest.json")

    # DynamoDB should be SUCCEEDED
    item = table.get_item(Key={"pk": f"job#{JOB_ID}", "sk": "meta"})["Item"]
    assert item["status"] == "SUCCEEDED"
    assert item["resultKey"] == result["resultKey"]


@mock_aws
def test_main_writes_results_to_s3(processor_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_resources(ddb, s3)
    _seed_job(table, JOB_ID)

    source_key = f"artifacts/{JOB_ID}/input/data.csv"
    _put_csv(s3, BUCKET, source_key)

    stub = _stub_result(JOB_ID, rows=10, cols=3)
    monkeypatch.setattr(processor_module, "run_pipeline", lambda *a, **kw: stub)
    monkeypatch.setattr(processor_module, "s3", s3)
    monkeypatch.setattr(processor_module, "ddb", ddb)

    event = {"jobId": JOB_ID, "input": {"bucket": BUCKET, "key": source_key}}
    result = processor_module.main(event, None)

    # results.json must exist in S3
    results_obj = s3.get_object(Bucket=BUCKET, Key=result["resultKey"])
    results_data = json.loads(results_obj["Body"].read())
    assert results_data["jobId"] == JOB_ID
    assert results_data["summary"]["rows"] == 10
    assert results_data["summary"]["columns"] == 3
    assert results_data["schema"][0]["name"] == "id"
    assert results_data["links"]["input"].startswith("s3://")


@mock_aws
def test_main_writes_phase_artifacts_to_s3(processor_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_resources(ddb, s3)
    _seed_job(table, JOB_ID)

    source_key = f"artifacts/{JOB_ID}/input/data.csv"
    _put_csv(s3, BUCKET, source_key)

    stub = _stub_result(JOB_ID)
    monkeypatch.setattr(processor_module, "run_pipeline", lambda *a, **kw: stub)
    monkeypatch.setattr(processor_module, "s3", s3)
    monkeypatch.setattr(processor_module, "ddb", ddb)

    event = {"jobId": JOB_ID, "input": {"bucket": BUCKET, "key": source_key}, "artifactPrefix": f"artifacts/{JOB_ID}"}
    result = processor_module.main(event, None)

    # Phase artifacts should exist
    for phase in ("ingest", "profile"):
        phase_key = f"artifacts/{JOB_ID}/phases/{phase}.json"
        resp = s3.get_object(Bucket=BUCKET, Key=phase_key)
        assert json.loads(resp["Body"].read()) is not None

    # Manifest should exist
    manifest_key = f"artifacts/{JOB_ID}/results/manifest.json"
    mresp = s3.get_object(Bucket=BUCKET, Key=manifest_key)
    manifest = json.loads(mresp["Body"].read())
    assert manifest["jobId"] == JOB_ID


@mock_aws
def test_main_phase_callback_updates_ddb(processor_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_resources(ddb, s3)
    _seed_job(table, JOB_ID)

    source_key = f"artifacts/{JOB_ID}/input/data.csv"
    _put_csv(s3, BUCKET, source_key)

    stub_result = _stub_result(JOB_ID)

    def _fake_pipeline(job_id, source, artifact_prefix, body, *, on_phase=None):
        if on_phase:
            for i, phase in enumerate(stub_result.phases):
                on_phase(phase, stub_result.phases[phase], i, len(stub_result.phases))
        return stub_result

    # Patch on handler module where the name was imported
    monkeypatch.setattr(processor_module, "run_pipeline", _fake_pipeline)
    monkeypatch.setattr(processor_module, "s3", s3)
    monkeypatch.setattr(processor_module, "ddb", ddb)

    event = {"jobId": JOB_ID, "input": {"bucket": BUCKET, "key": source_key}}
    processor_module.main(event, None)

    item = table.get_item(Key={"pk": f"job#{JOB_ID}", "sk": "meta"})["Item"]
    assert item["status"] == "SUCCEEDED"


# ---------------------------------------------------------------------------
# main() — idempotency
# ---------------------------------------------------------------------------


@mock_aws
def test_main_skips_if_results_already_exist(processor_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_resources(ddb, s3)
    _seed_job(table, JOB_ID)

    source_key = f"artifacts/{JOB_ID}/input/data.csv"
    results_key = f"artifacts/{JOB_ID}/results/results.json"
    _put_csv(s3, BUCKET, source_key)
    s3.put_object(Bucket=BUCKET, Key=results_key, Body=b"{}")

    pipeline_called = []
    monkeypatch.setattr(processor_module, "run_pipeline", lambda *a, **kw: pipeline_called.append(1) or _stub_result())

    monkeypatch.setattr(processor_module, "s3", s3)
    monkeypatch.setattr(processor_module, "ddb", ddb)

    event = {"jobId": JOB_ID, "input": {"bucket": BUCKET, "key": source_key}, "artifactPrefix": f"artifacts/{JOB_ID}"}
    result = processor_module.main(event, None)

    assert result.get("idempotent") is True
    assert len(pipeline_called) == 0


# ---------------------------------------------------------------------------
# main() — missing required params
# ---------------------------------------------------------------------------


@mock_aws
def test_main_raises_on_missing_job_id(processor_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    _create_resources(ddb, s3)
    monkeypatch.setattr(processor_module, "s3", s3)
    monkeypatch.setattr(processor_module, "ddb", ddb)

    with pytest.raises(ValueError, match="jobId"):
        processor_module.main({}, None)


@mock_aws
def test_main_raises_on_missing_bucket(processor_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    _create_resources(ddb, s3)
    monkeypatch.setattr(processor_module, "s3", s3)
    monkeypatch.setattr(processor_module, "ddb", ddb)

    with pytest.raises(ValueError, match="bucket"):
        processor_module.main({"jobId": JOB_ID, "input": {"key": "data.csv"}}, None)


# ---------------------------------------------------------------------------
# main() — pipeline error writes error artifact
# ---------------------------------------------------------------------------


@mock_aws
def test_main_pipeline_error_writes_error_artifact(processor_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_resources(ddb, s3)
    _seed_job(table, JOB_ID)

    source_key = f"artifacts/{JOB_ID}/input/data.csv"
    _put_csv(s3, BUCKET, source_key)

    monkeypatch.setattr(processor_module, "run_pipeline", lambda *a, **kw: (_ for _ in ()).throw(ValueError("pipeline broke")))
    monkeypatch.setattr(processor_module, "s3", s3)
    monkeypatch.setattr(processor_module, "ddb", ddb)

    event = {"jobId": JOB_ID, "input": {"bucket": BUCKET, "key": source_key}}
    with pytest.raises(ValueError, match="pipeline broke"):
        processor_module.main(event, None)

    # DynamoDB should be FAILED
    item = table.get_item(Key={"pk": f"job#{JOB_ID}", "sk": "meta"})["Item"]
    assert item["status"] == "FAILED"
    assert "pipeline broke" in item.get("error", "")

    # Error artifact should be written
    error_key = f"artifacts/{JOB_ID}/results/error.json"
    err_obj = s3.get_object(Bucket=BUCKET, Key=error_key)
    err_data = json.loads(err_obj["Body"].read())
    assert err_data["jobId"] == JOB_ID
    assert "pipeline broke" in err_data["error"]


# ---------------------------------------------------------------------------
# lambda_handler() — wrapper
# ---------------------------------------------------------------------------


@mock_aws
def test_lambda_handler_returns_200_on_success(processor_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_resources(ddb, s3)
    _seed_job(table, JOB_ID)

    source_key = f"artifacts/{JOB_ID}/input/data.csv"
    _put_csv(s3, BUCKET, source_key)

    stub = _stub_result(JOB_ID)
    monkeypatch.setattr(processor_module, "run_pipeline", lambda *a, **kw: stub)
    monkeypatch.setattr(processor_module, "s3", s3)
    monkeypatch.setattr(processor_module, "ddb", ddb)

    event = {"jobId": JOB_ID, "input": {"bucket": BUCKET, "key": source_key}}
    resp = processor_module.lambda_handler(event, None)
    assert resp["statusCode"] == 200
    assert resp["body"]["ok"] is True


@mock_aws
def test_lambda_handler_returns_400_on_missing_params(processor_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    _create_resources(ddb, s3)
    monkeypatch.setattr(processor_module, "s3", s3)
    monkeypatch.setattr(processor_module, "ddb", ddb)

    resp = processor_module.lambda_handler({}, None)
    assert resp["statusCode"] == 400


# ---------------------------------------------------------------------------
# _build_worker_event
# ---------------------------------------------------------------------------


def test_build_worker_event_embedded_mode_no_callback(processor_module, monkeypatch):
    monkeypatch.setattr(processor_module, "WORKER_INVOKE_MODE", "embedded")
    event = processor_module._build_worker_event(
        "job-1", {"bucket": "b", "key": "k"}, "artifacts/job-1"
    )
    assert event["jobId"] == "job-1"
    assert "callback" not in event


def test_build_worker_event_lambda_mode_includes_callback(processor_module, monkeypatch):
    monkeypatch.setattr(processor_module, "WORKER_INVOKE_MODE", "lambda")
    monkeypatch.setenv("TABLE_NAME", TABLE)
    event = processor_module._build_worker_event(
        "job-1", {"bucket": "b", "key": "k"}, "artifacts/job-1"
    )
    assert "callback" in event
    assert event["callback"]["mode"] == "ddb"


def test_build_worker_event_with_body_bytes(processor_module, monkeypatch):
    monkeypatch.setattr(processor_module, "WORKER_INVOKE_MODE", "embedded")
    body = b"csv data here"
    event = processor_module._build_worker_event(
        "job-1", {}, "artifacts/job-1", body_bytes=body
    )
    import base64
    assert base64.b64decode(event["body"]) == body


def test_build_worker_event_with_body_s3(processor_module, monkeypatch):
    monkeypatch.setattr(processor_module, "WORKER_INVOKE_MODE", "embedded")
    event = processor_module._build_worker_event(
        "job-1", {}, "artifacts/job-1", body_s3={"bucket": "b", "key": "k"}
    )
    assert event["bodyS3"] == {"bucket": "b", "key": "k"}


# ---------------------------------------------------------------------------
# _ensure_bytes
# ---------------------------------------------------------------------------


def test_ensure_bytes_from_bytes(processor_module):
    assert processor_module._ensure_bytes(b"hello") == b"hello"


def test_ensure_bytes_from_bytearray(processor_module):
    assert processor_module._ensure_bytes(bytearray(b"world")) == b"world"


def test_ensure_bytes_from_stream(processor_module):
    stream = io.BytesIO(b"stream data")
    assert processor_module._ensure_bytes(stream) == b"stream data"


def test_ensure_bytes_from_memoryview(processor_module):
    mv = memoryview(b"view data")
    assert processor_module._ensure_bytes(mv) == b"view data"


def test_ensure_bytes_unsupported_raises(processor_module):
    with pytest.raises(TypeError, match="Unsupported body type"):
        processor_module._ensure_bytes(42)


# ---------------------------------------------------------------------------
# _invoke_pipeline — lambda mode
# ---------------------------------------------------------------------------


@mock_aws
def test_invoke_pipeline_lambda_mode(processor_module, monkeypatch):
    from services.workers.graph.graph import PipelineResult, encode_pipeline_result

    monkeypatch.setattr(processor_module, "WORKER_INVOKE_MODE", "lambda")
    monkeypatch.setattr(processor_module, "WORKER_ARN", "arn:aws:lambda:us-east-1:123:function:worker")

    stub = _stub_result()
    encoded = encode_pipeline_result(stub)
    payload_bytes = json.dumps(encoded).encode()

    class _StubLambdaClient:
        def invoke(self, FunctionName, InvocationType, Payload):
            return {"StatusCode": 200, "Payload": io.BytesIO(payload_bytes)}

    monkeypatch.setattr(processor_module, "lambda_client", _StubLambdaClient())

    result = processor_module._invoke_pipeline(
        "job-1",
        {"bucket": "b", "key": "k"},
        "artifacts/job-1",
        None,
        body_s3={"bucket": "b", "key": "k"},
        on_phase=None,
    )
    assert result.phases is not None
    assert "ingest" in result.phases


@mock_aws
def test_invoke_pipeline_lambda_mode_function_error_raises(processor_module, monkeypatch):
    monkeypatch.setattr(processor_module, "WORKER_INVOKE_MODE", "lambda")
    monkeypatch.setattr(processor_module, "WORKER_ARN", "arn:aws:lambda:us-east-1:123:function:worker")

    class _StubLambdaClient:
        def invoke(self, FunctionName, InvocationType, Payload):
            return {
                "StatusCode": 200,
                "FunctionError": "Unhandled",
                "Payload": io.BytesIO(b'{"errorMessage": "timeout"}'),
            }

    monkeypatch.setattr(processor_module, "lambda_client", _StubLambdaClient())

    with pytest.raises(RuntimeError, match="execution failed"):
        processor_module._invoke_pipeline(
            "job-1",
            {},
            "artifacts/job-1",
            None,
            body_s3={"bucket": "b", "key": "k"},
            on_phase=None,
        )


# ---------------------------------------------------------------------------
# _invoke_pipeline — HTTP mode
# ---------------------------------------------------------------------------


@mock_aws
def test_invoke_pipeline_http_mode(processor_module, monkeypatch):
    from services.workers.graph.graph import PipelineResult, encode_pipeline_result

    monkeypatch.setattr(processor_module, "WORKER_INVOKE_MODE", "http")
    monkeypatch.setattr(processor_module, "WORKER_URL", "https://worker.example.com/invoke")
    monkeypatch.setattr(processor_module, "WORKER_BEARER_TOKEN", "my-secret-token")

    stub = _stub_result()
    encoded = encode_pipeline_result(stub)
    payload_bytes = json.dumps(encoded).encode()

    class _FakeHTTPResponse:
        def __init__(self):
            self._data = payload_bytes

        def read(self):
            return self._data

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    def _fake_urlopen(request, timeout=None):
        assert "Bearer my-secret-token" in request.get_header("Authorization")
        return _FakeHTTPResponse()

    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)

    result = processor_module._invoke_pipeline(
        "job-1",
        {},
        "artifacts/job-1",
        None,
        body_s3={"bucket": "b", "key": "k"},
        on_phase=None,
    )
    assert "ingest" in result.phases


@mock_aws
def test_invoke_pipeline_http_mode_http_error_raises(processor_module, monkeypatch):
    monkeypatch.setattr(processor_module, "WORKER_INVOKE_MODE", "http")
    monkeypatch.setattr(processor_module, "WORKER_URL", "https://worker.example.com/invoke")

    import urllib.request
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda *a, **kw: (_ for _ in ()).throw(
            urllib_error.HTTPError("url", 500, "Internal Server Error", {}, None)
        ),
    )

    with pytest.raises(RuntimeError, match="HTTP error 500"):
        processor_module._invoke_pipeline(
            "job-1",
            {},
            "artifacts/job-1",
            None,
            body_s3={"bucket": "b", "key": "k"},
            on_phase=None,
        )


def test_invoke_pipeline_unsupported_mode_raises(processor_module, monkeypatch):
    monkeypatch.setattr(processor_module, "WORKER_INVOKE_MODE", "ftp")

    with pytest.raises(ValueError, match="Unsupported WORKER_INVOKE_MODE"):
        processor_module._invoke_pipeline("j", {}, "p", None, on_phase=None)


def test_invoke_pipeline_lambda_mode_no_arn_raises(processor_module, monkeypatch):
    monkeypatch.setattr(processor_module, "WORKER_INVOKE_MODE", "lambda")
    monkeypatch.setattr(processor_module, "WORKER_ARN", None)

    with pytest.raises(RuntimeError, match="WORKER_ARN"):
        processor_module._invoke_pipeline(
            "j", {}, "p", None, body_s3={"bucket": "b", "key": "k"}, on_phase=None
        )


def test_invoke_pipeline_http_mode_no_url_raises(processor_module, monkeypatch):
    monkeypatch.setattr(processor_module, "WORKER_INVOKE_MODE", "http")
    monkeypatch.setattr(processor_module, "WORKER_URL", None)

    with pytest.raises(RuntimeError, match="WORKER_URL"):
        processor_module._invoke_pipeline(
            "j", {}, "p", None, body_s3={"bucket": "b", "key": "k"}, on_phase=None
        )


# ---------------------------------------------------------------------------
# handler() (primary Step Functions entrypoint)
# ---------------------------------------------------------------------------


@mock_aws
def test_handler_delegates_to_main(processor_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_resources(ddb, s3)
    _seed_job(table, JOB_ID)

    source_key = f"artifacts/{JOB_ID}/input/data.csv"
    _put_csv(s3, BUCKET, source_key)

    stub = _stub_result(JOB_ID)
    monkeypatch.setattr(processor_module, "run_pipeline", lambda *a, **kw: stub)
    monkeypatch.setattr(processor_module, "s3", s3)
    monkeypatch.setattr(processor_module, "ddb", ddb)

    event = {"jobId": JOB_ID, "input": {"bucket": BUCKET, "key": source_key}}
    result = processor_module.handler(event, None)
    assert result["ok"] is True
