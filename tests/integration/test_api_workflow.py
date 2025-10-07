import asyncio
import importlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import httpx
import pytest
from botocore.exceptions import ClientError


def _client_error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


class FakeCloudWatch:
    def __init__(self) -> None:
        self.metrics: List[Dict[str, object]] = []

    def put_metric_data(self, Namespace: str, MetricData: List[Dict[str, object]]):  # noqa: N803 - mimic boto3 casing
        for metric in MetricData:
            record = dict(metric)
            record["Namespace"] = Namespace
            self.metrics.append(record)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


class FakeStepFunctions:
    def __init__(self) -> None:
        self.executions: List[Dict[str, object]] = []
        self._error: Optional[Exception] = None

    def fail_with(self, error: Exception) -> None:
        self._error = error

    def start_execution(self, **kwargs):
        if self._error is not None:
            error = self._error
            self._error = None
            raise error
        execution = dict(kwargs)
        self.executions.append(execution)
        return {
            "executionArn": f"arn:aws:states:us-east-1:123456789012:execution:stateMachine:{kwargs['name']}",
            "startDate": datetime.now(timezone.utc),
        }


class FakeTable:
    def __init__(self) -> None:
        self._items: Dict[tuple[str, str], Dict[str, object]] = {}

    def put_item(self, Item: Dict[str, object], ConditionExpression: Optional[str] = None, **_: object):
        key = (Item["pk"], Item["sk"])
        if ConditionExpression and key in self._items:
            raise _client_error("ConditionalCheckFailedException", "PutItem")
        self._items[key] = dict(Item)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def get_item(self, Key: Dict[str, str]):
        key = (Key["pk"], Key["sk"])
        item = self._items.get(key)
        return {"Item": dict(item)} if item is not None else {}

    def update_item(
        self,
        Key: Dict[str, str],
        UpdateExpression: str,
        ExpressionAttributeNames: Dict[str, str],
        ExpressionAttributeValues: Dict[str, object],
    ):
        key = (Key["pk"], Key["sk"])
        if key not in self._items:
            raise _client_error("ResourceNotFoundException", "UpdateItem")
        expression = UpdateExpression.replace("SET", "").strip()
        item = dict(self._items[key])
        for part in expression.split(","):
            name_alias, value_alias = [segment.strip() for segment in part.split("=", 1)]
            attr_name = ExpressionAttributeNames.get(name_alias, name_alias)
            value = ExpressionAttributeValues[value_alias]
            item[attr_name] = value
        self._items[key] = item
        return {"Attributes": dict(item)}


class FakeS3:
    def __init__(self) -> None:
        self._objects: Dict[str, Dict[str, Dict[str, object]]] = {}

    def _bucket(self, name: str) -> Dict[str, Dict[str, object]]:
        return self._objects.setdefault(name, {})

    def put_object(self, Bucket: str, Key: str, Body=b"", **_: object):
        if isinstance(Body, str):
            payload = Body.encode("utf-8")
        elif hasattr(Body, "read"):
            payload = Body.read()
        else:
            payload = bytes(Body)
        self._bucket(Bucket)[Key] = {
            "Body": payload,
            "Size": len(payload),
            "ETag": "\"etag\"",
            "LastModified": datetime.now(timezone.utc),
            "StorageClass": "STANDARD",
        }
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def generate_presigned_url(self, ClientMethod: str, Params: Dict[str, str], ExpiresIn: int):  # noqa: N803 - mimic boto3 casing
        return f"https://example.com/{Params['Key']}?expires={ExpiresIn}&method={ClientMethod}"

    def list_objects_v2(self, Bucket: str, Prefix: str, MaxKeys: int, Delimiter: Optional[str] = "/", **_: object):  # noqa: N803
        bucket = self._bucket(Bucket)
        contents: List[Dict[str, object]] = []
        common: Dict[str, Dict[str, str]] = {}
        for key in sorted(bucket):
            if not key.startswith(Prefix):
                continue
            if len(contents) >= MaxKeys:
                break
            metadata = bucket[key]
            suffix = key[len(Prefix):]
            if Delimiter and Delimiter in suffix:
                prefix = Prefix + suffix.split(Delimiter, 1)[0] + Delimiter
                common.setdefault(prefix, {"Prefix": prefix})
                continue
            contents.append(
                {
                    "Key": key,
                    "ETag": metadata.get("ETag"),
                    "LastModified": metadata.get("LastModified"),
                    "Size": metadata.get("Size"),
                    "StorageClass": metadata.get("StorageClass"),
                }
            )
        return {
            "Contents": contents,
            "CommonPrefixes": list(common.values()),
            "IsTruncated": False,
        }

    def head_object(self, Bucket: str, Key: str):
        bucket = self._bucket(Bucket)
        if Key not in bucket:
            raise _client_error("404", "HeadObject")
        metadata = bucket[Key]
        return {
            "ContentLength": metadata.get("Size"),
            "LastModified": metadata.get("LastModified"),
            "ETag": metadata.get("ETag"),
        }


class ApiClient:
    def __init__(self, app):
        self._app = app

    def request(self, method: str, url: str, **kwargs):
        async def _call():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=self._app),
                base_url="http://testserver",
            ) as client:
                return await client.request(method, url, **kwargs)

        return asyncio.run(_call())

    def get(self, url: str, **kwargs):
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs):
        return self.request("POST", url, **kwargs)


@pytest.fixture()
def api_app(monkeypatch):
    monkeypatch.setenv("BUCKET_NAME", "artifacts-bucket")
    monkeypatch.setenv("TABLE_NAME", "jobs-table")
    monkeypatch.setenv("STATE_MACHINE_ARN", "arn:aws:states:region:acct:stateMachine:jobs")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")

    class _DummyDynamoResource:
        def Table(self, _name: str):  # noqa: N802 - mimic boto3
            return {}

    monkeypatch.setattr("boto3.client", lambda service: object())
    monkeypatch.setattr("boto3.resource", lambda service: _DummyDynamoResource())

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    module = importlib.import_module("services.api.app")
    importlib.reload(module)

    fake_s3 = FakeS3()
    fake_table = FakeTable()
    fake_sfn = FakeStepFunctions()
    fake_cloudwatch = FakeCloudWatch()

    module.s3 = fake_s3
    module.table = fake_table
    module.sfn = fake_sfn
    module.cloudwatch = fake_cloudwatch

    client = ApiClient(module.app)

    yield client, module, fake_s3, fake_table, fake_sfn, fake_cloudwatch

    sys.path.pop(0)


def test_create_job_enqueues_state_machine(api_app):
    client, module, _s3, table, sfn, cloudwatch = api_app

    response = client.post(
        "/jobs",
        json={"source_type": "upload"},
    )

    assert response.status_code == 200
    payload = response.json()
    job_id = payload["jobId"]
    assert payload["uploadUrl"].startswith("https://example.com/")

    record = table._items[(f"job#{job_id}", "meta")]
    assert record["status"] == "QUEUED"
    assert record["source"]["type"] == "upload"

    assert len(sfn.executions) == 1
    execution = sfn.executions[0]
    assert json.loads(execution["input"]) == {"jobId": job_id}
    assert execution["stateMachineArn"] == module.STATE_MACHINE_ARN

    metric_names = [metric["MetricName"] for metric in cloudwatch.metrics]
    assert "JobQueued" in metric_names
    assert "JobCreated" in metric_names


def test_create_job_handles_state_machine_failure(api_app):
    client, module, _s3, table, sfn, cloudwatch = api_app

    sfn.fail_with(_client_error("StateMachineDoesNotExist", "StartExecution"))

    response = client.post(
        "/jobs",
        json={"source_type": "s3", "s3_path": "s3://bucket/data.csv"},
    )

    assert response.status_code == 502
    body = response.json()
    assert body["detail"] == "Failed to start job workflow"

    assert len(table._items) == 1
    (pk, sk), record = next(iter(table._items.items()))
    assert sk == "meta"
    assert record["status"] == "FAILED"
    assert "error" in record

    metric_names = [metric["MetricName"] for metric in cloudwatch.metrics]
    assert "JobWorkflowStartFailed" in metric_names


def test_artifact_and_results_endpoints(api_app):
    client, module, s3, table, _sfn, _cloudwatch = api_app

    job_id = "job-123"
    table.put_item(
        {
            "pk": f"job#{job_id}",
            "sk": "meta",
            "status": "SUCCEEDED",
            "createdAt": 1700000000,
            "updatedAt": 1700000001,
            "source": {"type": "upload"},
            "resultKey": f"artifacts/{job_id}/results/results.json",
        }
    )

    s3.put_object(Bucket=module.BUCKET_NAME, Key=f"artifacts/{job_id}/input/data.csv", Body="id,value\n1,2\n")
    s3.put_object(Bucket=module.BUCKET_NAME, Key=f"artifacts/{job_id}/results/results.json", Body="{}")

    job_response = client.get(f"/jobs/{job_id}")
    assert job_response.status_code == 200
    assert job_response.json()["jobId"] == job_id

    artifacts = client.get(f"/jobs/{job_id}/artifacts")
    assert artifacts.status_code == 200
    body = artifacts.json()
    keys = [obj["key"] for obj in body["objects"]]
    assert keys == []
    assert f"artifacts/{job_id}/input/" in body["commonPrefixes"]

    results_listing = client.get(f"/jobs/{job_id}/results/files")
    assert results_listing.status_code == 200
    results_body = results_listing.json()
    result_keys = [obj["key"] for obj in results_body["objects"]]
    assert f"artifacts/{job_id}/results/results.json" in result_keys

    download = client.get(f"/jobs/{job_id}/results")
    assert download.status_code == 200
    download_body = download.json()
    assert download_body["key"].endswith("results.json")
    assert download_body["downloadUrl"].startswith("https://example.com/")
