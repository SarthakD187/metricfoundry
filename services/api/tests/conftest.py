import importlib
import io
import hashlib
from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError
import anyio
import httpx


def _client_error(code: str, operation: str):
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


class InMemoryS3:
    def __init__(self):
        self._buckets: dict[str, dict[str, dict]] = {}

    def _bucket(self, bucket: str) -> dict[str, dict]:
        return self._buckets.setdefault(bucket, {})

    def put_object(self, Bucket: str, Key: str, Body, ContentType: str | None = None):
        if isinstance(Body, str):
            body_bytes = Body.encode("utf-8")
        elif hasattr(Body, "read"):
            body_bytes = Body.read()
        else:
            body_bytes = Body
        metadata = {
            "Body": body_bytes,
            "LastModified": datetime.now(timezone.utc),
            "Size": len(body_bytes),
            "ContentType": ContentType,
            "ETag": hashlib.md5(body_bytes).hexdigest(),
        }
        self._bucket(Bucket)[Key] = metadata
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def get_object(self, Bucket: str, Key: str):
        bucket = self._bucket(Bucket)
        if Key not in bucket:
            raise _client_error("NoSuchKey", "GetObject")
        metadata = bucket[Key]
        return {
            "Body": io.BytesIO(metadata["Body"]),
            "ContentLength": metadata["Size"],
            "ETag": metadata["ETag"],
            "LastModified": metadata["LastModified"],
        }

    def head_object(self, Bucket: str, Key: str):
        bucket = self._bucket(Bucket)
        if Key not in bucket:
            raise _client_error("404", "HeadObject")
        metadata = bucket[Key]
        return {
            "ContentLength": metadata["Size"],
            "LastModified": metadata["LastModified"],
        }

    def list_objects_v2(self, Bucket: str, Prefix: str = "", MaxKeys: int = 1000, Delimiter: str | None = None, ContinuationToken: str | None = None):
        bucket = self._bucket(Bucket)
        keys = [key for key in bucket if key.startswith(Prefix)]
        keys.sort()

        contents = []
        common_prefixes: set[str] = set()

        for key in keys:
            remainder = key[len(Prefix):]
            if Delimiter and Delimiter in remainder:
                prefix = Prefix + remainder.split(Delimiter, 1)[0] + Delimiter
                common_prefixes.add(prefix)
                continue
            metadata = bucket[key]
            contents.append({
                "Key": key,
                "Size": metadata["Size"],
                "LastModified": metadata["LastModified"],
                "ETag": metadata["ETag"],
            })

        return {
            "Contents": contents[:MaxKeys],
            "CommonPrefixes": [{"Prefix": value} for value in sorted(common_prefixes)],
            "IsTruncated": False,
        }

    def generate_presigned_url(self, ClientMethod: str, Params: dict, ExpiresIn: int):
        bucket = Params.get("Bucket")
        key = Params.get("Key")
        return f"https://presigned.local/{bucket}/{key}?expires={ExpiresIn}"


class FakeDynamoTable:
    def __init__(self):
        self._items: dict[tuple[str, str], dict] = {}

    def put_item(self, Item: dict, ConditionExpression: str | None = None):
        key = (Item["pk"], Item["sk"])
        if ConditionExpression and key in self._items:
            raise _client_error("ConditionalCheckFailedException", "PutItem")
        self._items[key] = dict(Item)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def get_item(self, Key: dict):
        key = (Key["pk"], Key["sk"])
        item = self._items.get(key)
        return {"Item": dict(item)} if item else {}

    def update_item(self, Key: dict, UpdateExpression: str, ExpressionAttributeNames: dict, ExpressionAttributeValues: dict):
        key = (Key["pk"], Key["sk"])
        if key not in self._items:
            raise _client_error("ResourceNotFoundException", "UpdateItem")
        item = self._items[key]
        expression = UpdateExpression.replace("SET", "").strip()
        for part in expression.split(","):
            name_alias, value_alias = [segment.strip() for segment in part.split("=", 1)]
            attribute_name = ExpressionAttributeNames.get(name_alias, name_alias)
            value = ExpressionAttributeValues[value_alias]
            item[attribute_name] = value
        self._items[key] = item
        return {"Attributes": dict(item)}


class FakeStepFunctions:
    def __init__(self):
        self.executions = []

    def start_execution(self, **kwargs):
        self.executions.append(kwargs)
        return {"executionArn": f"arn:aws:states:local:execution:{kwargs.get('name', 'execution')}"}


class FakeCloudWatch:
    def __init__(self):
        self.metric_calls = []

    def put_metric_data(self, Namespace, MetricData):
        self.metric_calls.append({"Namespace": Namespace, "MetricData": MetricData})
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


@pytest.fixture()
def api_app(monkeypatch):
    monkeypatch.setenv("BUCKET_NAME", "metricfoundry-artifacts")
    monkeypatch.setenv("TABLE_NAME", "metricfoundry-jobs")
    monkeypatch.setenv("STATE_MACHINE_ARN", "arn:aws:states:local:stateMachine:metricfoundry")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    from services.api import app as app_module

    importlib.reload(app_module)

    fake_s3 = InMemoryS3()
    fake_table = FakeDynamoTable()
    fake_sfn = FakeStepFunctions()
    fake_cw = FakeCloudWatch()

    app_module.s3 = fake_s3
    app_module.table = fake_table
    app_module.sfn = fake_sfn
    app_module.cloudwatch = fake_cw

    transport = httpx.ASGITransport(app=app_module.app)
    async_client = httpx.AsyncClient(transport=transport, base_url="http://testserver")

    class SyncClient:
        def request(self, method: str, url: str, **kwargs):
            return anyio.run(lambda: async_client.request(method, url, **kwargs))

        def get(self, url: str, **kwargs):
            return self.request("GET", url, **kwargs)

        def post(self, url: str, **kwargs):
            return self.request("POST", url, **kwargs)

    client = SyncClient()

    try:
        yield {
            "client": client,
            "module": app_module,
            "sfn": fake_sfn,
            "cloudwatch": fake_cw,
        }
    finally:
        anyio.run(async_client.aclose)
