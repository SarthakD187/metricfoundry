import importlib
import io
import json
from typing import Optional

import pytest

from tests.integration.utils.langgraph import ensure_langgraph_stub

ensure_langgraph_stub()

from services.workers.graph.graph import PipelineResult, encode_pipeline_result


@pytest.mark.parametrize("mode", ["lambda", "http"])
def test_remote_worker_uses_s3_pointer(monkeypatch, mode):
    monkeypatch.setenv("JOBS_TABLE", "jobs-table")
    monkeypatch.setenv("ARTIFACTS_BUCKET", "artifacts")
    monkeypatch.setenv("WORKER_INVOKE_MODE", mode)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_REGION", "us-east-1")

    if mode == "lambda":
        monkeypatch.setenv("WORKER_ARN", "arn:aws:lambda:us-east-1:123:function:worker")
        monkeypatch.delenv("WORKER_URL", raising=False)
    else:
        monkeypatch.setenv("WORKER_URL", "https://worker.example.com")
        monkeypatch.delenv("WORKER_ARN", raising=False)

    module = importlib.import_module("lambdas.processor.handler")
    module = importlib.reload(module)

    # ensure we don't make real network calls
    monkeypatch.setattr(module, "s3", object())

    result = PipelineResult(
        phases={"ingest": {"summary": "ok"}},
        metrics={},
        manifest={},
        artifact_contents={},
        correlations=[],
        outliers=[],
        ml_inference={},
    )

    body_pointer = {"bucket": "artifacts", "key": "artifacts/job-123/input/data.csv"}

    if mode == "lambda":
        class StubLambdaClient:
            def __init__(self):
                self.payloads = []

            def invoke(self, FunctionName, InvocationType, Payload):
                event = json.loads(Payload.decode("utf-8"))
                self.payloads.append(event)
                payload_bytes = json.dumps(encode_pipeline_result(result)).encode("utf-8")
                return {"StatusCode": 200, "Payload": io.BytesIO(payload_bytes)}

        stub = StubLambdaClient()
        monkeypatch.setattr(module, "lambda_client", stub)
        returned = module._invoke_worker_lambda(
            "job-123",
            body_pointer,
            "artifacts/job-123",
            body_s3=body_pointer,
        )
        assert returned == result
        assert stub.payloads and stub.payloads[0]["bodyS3"] == body_pointer
        assert "body" not in stub.payloads[0]
    else:
        class StubHTTPResponse(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                self.close()

        def fake_urlopen(request, timeout):
            event = json.loads(request.data.decode("utf-8"))
            fake_urlopen.last_payload = event
            fake_urlopen.last_headers = dict(request.headers)
            payload_bytes = json.dumps(encode_pipeline_result(result)).encode("utf-8")
            return StubHTTPResponse(payload_bytes)

        fake_urlopen.last_payload = None
        fake_urlopen.last_headers = {}
        monkeypatch.setattr(module.urllib_request, "urlopen", fake_urlopen)
        returned = module._invoke_worker_http(
            "job-123",
            body_pointer,
            "artifacts/job-123",
            body_s3=body_pointer,
        )
        assert returned == result
        assert fake_urlopen.last_payload is not None
        assert fake_urlopen.last_payload["bodyS3"] == body_pointer
        assert "body" not in fake_urlopen.last_payload
        header_map = {k.lower(): v for k, v in fake_urlopen.last_headers.items()}
        assert header_map.get("content-type") == "application/json"


def _stub_http_worker(monkeypatch, bearer: Optional[str] = None, header: Optional[str] = None):
    monkeypatch.setenv("JOBS_TABLE", "jobs-table")
    monkeypatch.setenv("ARTIFACTS_BUCKET", "artifacts")
    monkeypatch.setenv("WORKER_INVOKE_MODE", "http")
    monkeypatch.setenv("WORKER_URL", "https://worker.example.com")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    if bearer is not None:
        monkeypatch.setenv("WORKER_BEARER_TOKEN", bearer)
    else:
        monkeypatch.delenv("WORKER_BEARER_TOKEN", raising=False)
    if header is not None:
        monkeypatch.setenv("WORKER_AUTH_HEADER", header)
    else:
        monkeypatch.delenv("WORKER_AUTH_HEADER", raising=False)

    module = importlib.import_module("lambdas.processor.handler")
    return importlib.reload(module)


def test_http_worker_uses_bearer_token(monkeypatch):
    module = _stub_http_worker(monkeypatch, bearer="super-secret")

    class StubHTTPResponse(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.close()

    result = PipelineResult(
        phases={},
        metrics={},
        manifest={},
        artifact_contents={},
        correlations=[],
        outliers=[],
        ml_inference={},
    )

    def fake_urlopen(request, timeout):
        fake_urlopen.last_headers = dict(request.headers)
        payload_bytes = json.dumps(encode_pipeline_result(result)).encode("utf-8")
        return StubHTTPResponse(payload_bytes)

    fake_urlopen.last_headers = {}
    monkeypatch.setattr(module.urllib_request, "urlopen", fake_urlopen)

    module._invoke_worker_http(
        "job-123",
        {"bucket": "artifacts", "key": "artifacts/job-123/input/data.csv"},
        "artifacts/job-123",
        body_s3={"bucket": "artifacts", "key": "input"},
    )

    header_map = {k.lower(): v for k, v in fake_urlopen.last_headers.items()}
    assert header_map.get("authorization") == "Bearer super-secret"


def test_http_worker_uses_custom_header(monkeypatch):
    module = _stub_http_worker(monkeypatch, header="X-Api-Key: abc123")

    class StubHTTPResponse(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.close()

    result = PipelineResult(
        phases={},
        metrics={},
        manifest={},
        artifact_contents={},
        correlations=[],
        outliers=[],
        ml_inference={},
    )

    def fake_urlopen(request, timeout):
        fake_urlopen.last_headers = dict(request.headers)
        payload_bytes = json.dumps(encode_pipeline_result(result)).encode("utf-8")
        return StubHTTPResponse(payload_bytes)

    fake_urlopen.last_headers = {}
    monkeypatch.setattr(module.urllib_request, "urlopen", fake_urlopen)

    module._invoke_worker_http(
        "job-456",
        {"bucket": "artifacts", "key": "artifacts/job-456/input/data.csv"},
        "artifacts/job-456",
        body_s3={"bucket": "artifacts", "key": "input"},
    )

    header_map = {k.lower(): v for k, v in fake_urlopen.last_headers.items()}
    assert header_map.get("x-api-key") == "abc123"
    assert "authorization" not in header_map
