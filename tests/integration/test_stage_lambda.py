import importlib
import io
import hashlib
from datetime import datetime, timezone
from typing import Dict

import pytest
from botocore.exceptions import ClientError


def _client_error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


class FakeS3:
    def __init__(self) -> None:
        self._buckets: Dict[str, Dict[str, Dict[str, object]]] = {}

    def _bucket(self, name: str) -> Dict[str, Dict[str, object]]:
        return self._buckets.setdefault(name, {})

    def put_object(self, Bucket: str, Key: str, Body, ContentType: str | None = None):
        if isinstance(Body, str):
            payload = Body.encode("utf-8")
        elif hasattr(Body, "read"):
            payload = Body.read()
        else:
            payload = bytes(Body)
        metadata = {
            "Body": payload,
            "ContentLength": len(payload),
            "ContentType": ContentType,
            "LastModified": datetime.now(timezone.utc),
            "ETag": hashlib.md5(payload).hexdigest(),
        }
        self._bucket(Bucket)[Key] = metadata
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def copy_object(self, Bucket: str, Key: str, CopySource: Dict[str, str]):
        source_bucket = CopySource["Bucket"]
        source_key = CopySource["Key"]
        source = self._bucket(source_bucket).get(source_key)
        if source is None:
            raise _client_error("NoSuchKey", "CopyObject")
        copied = dict(source)
        self._bucket(Bucket)[Key] = copied
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def head_object(self, Bucket: str, Key: str):
        bucket = self._bucket(Bucket)
        if Key not in bucket:
            raise _client_error("404", "HeadObject")
        metadata = bucket[Key]
        return {
            "ContentLength": metadata["ContentLength"],
            "ContentType": metadata.get("ContentType"),
            "LastModified": metadata["LastModified"],
        }

    def get_object(self, Bucket: str, Key: str):
        bucket = self._bucket(Bucket)
        if Key not in bucket:
            raise _client_error("NoSuchKey", "GetObject")
        metadata = bucket[Key]
        return {
            "Body": io.BytesIO(metadata["Body"]),
            "ContentLength": metadata["ContentLength"],
            "ContentType": metadata.get("ContentType"),
            "LastModified": metadata["LastModified"],
        }


class FakeDynamoTable:
    def __init__(self) -> None:
        self._items: Dict[tuple[str, str], Dict[str, object]] = {}

    def put_item(self, Item: Dict[str, object]):
        key = (Item["pk"], Item["sk"])
        self._items[key] = dict(Item)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def get_item(self, Key: Dict[str, str]):
        key = (Key["pk"], Key["sk"])
        item = self._items.get(key)
        return {"Item": dict(item)} if item else {}

    def update_item(self, Key: Dict[str, str], UpdateExpression: str, ExpressionAttributeNames: Dict[str, str], ExpressionAttributeValues: Dict[str, object]):
        key = (Key["pk"], Key["sk"])
        if key not in self._items:
            raise _client_error("ResourceNotFoundException", "UpdateItem")
        item = dict(self._items[key])
        expression = UpdateExpression.replace("SET", "").strip()
        for part in expression.split(","):
            name_alias, value_alias = [segment.strip() for segment in part.split("=", 1)]
            attribute_name = ExpressionAttributeNames.get(name_alias, name_alias)
            item[attribute_name] = ExpressionAttributeValues[value_alias]
        self._items[key] = item
        return {"Attributes": dict(item)}


class FakeDynamoResource:
    def __init__(self, table: FakeDynamoTable) -> None:
        self._table = table

    def Table(self, _name: str) -> FakeDynamoTable:  # noqa: N802 - mimic boto3
        return self._table


@pytest.fixture()
def stage_lambda(monkeypatch):
    monkeypatch.setenv("JOBS_TABLE", "jobs-table")
    monkeypatch.setenv("ARTIFACTS_BUCKET", "artifacts-bucket")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    module = importlib.import_module("lambdas.stage.handler")
    importlib.reload(module)

    fake_s3 = FakeS3()
    fake_table = FakeDynamoTable()

    module.s3 = fake_s3
    module.ddb = FakeDynamoResource(fake_table)

    return module, fake_s3, fake_table


@pytest.mark.parametrize(
    "job_source, bucket, key, body, content_type, expected_format",
    [
        (
            {"type": "upload", "bucket": "incoming", "key": "uploads/sample.csv"},
            "incoming",
            "uploads/sample.csv",
            b"id,value\n1,2\n",
            "text/csv",
            "csv",
        ),
        (
            {"type": "s3", "uri": "s3://external/data.jsonl"},
            "external",
            "data.jsonl",
            b"{\"id\": 1}\n",
            "application/json",
            "jsonl",
        ),
        (
            {"type": "upload", "bucket": "incoming", "key": "uploads/metrics.parquet"},
            "incoming",
            "uploads/metrics.parquet",
            b"PARQUET",
            "application/octet-stream",
            "parquet",
        ),
    ],
)
def test_stage_lambda_end_to_end(stage_lambda, job_source, bucket, key, body, content_type, expected_format):
    module, fake_s3, fake_table = stage_lambda
    job_id = f"job-{expected_format}"

    fake_table.put_item(
        {
            "pk": f"job#{job_id}",
            "sk": "meta",
            "status": "QUEUED",
            "source": job_source,
        }
    )

    fake_s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)

    result = module.handler({"jobId": job_id}, None)

    filename = key.rsplit("/", 1)[-1]
    expected_key = f"artifacts/{job_id}/input/{filename}"

    staged = fake_s3.get_object(Bucket=module.ARTIFACTS_BUCKET, Key=expected_key)
    assert staged["Body"].read() == body

    record = fake_table.get_item({"pk": f"job#{job_id}", "sk": "meta"}).get("Item")
    assert record is not None
    assert record["status"] == module.STATUS_STAGED
    assert record["inputKey"] == expected_key
    assert record["inputMetadata"]["format"] == expected_format
    assert record["inputMetadata"]["sourceType"] == job_source["type"]

    metadata = result["metadata"]
    assert metadata["format"] == expected_format
    assert result["input"]["key"] == expected_key
