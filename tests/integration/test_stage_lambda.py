import importlib
import io
import json
import hashlib
import sqlite3
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


class FakeSecretsManager:
    def __init__(self) -> None:
        self._secrets: Dict[str, str] = {}

    def put_secret(self, arn: str, value: str):
        self._secrets[arn] = value

    def get_secret_value(self, SecretId: str):  # noqa: N802 - mimic boto3
        if SecretId not in self._secrets:
            raise _client_error("ResourceNotFoundException", "GetSecretValue")
        return {"ARN": SecretId, "SecretString": self._secrets[SecretId]}


class FakeSSM:
    def __init__(self) -> None:
        self._parameters: Dict[str, str] = {}

    def put_parameter(self, Name: str, Value: str):  # noqa: N802 - mimic boto3
        self._parameters[Name] = Value

    def get_parameter(self, Name: str, WithDecryption: bool = False):  # noqa: N802 - mimic boto3
        if Name not in self._parameters:
            raise _client_error("ParameterNotFound", "GetParameter")
        return {"Parameter": {"Name": Name, "Value": self._parameters[Name]}}


@pytest.fixture()
def stage_lambda(monkeypatch):
    monkeypatch.setenv("JOBS_TABLE", "jobs-table")
    monkeypatch.setenv("ARTIFACTS_BUCKET", "artifacts-bucket")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")

    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

    module = importlib.import_module("lambdas.stage.handler")
    importlib.reload(module)

    fake_s3 = FakeS3()
    fake_table = FakeDynamoTable()
    fake_secrets = FakeSecretsManager()
    fake_ssm = FakeSSM()

    module.s3 = fake_s3
    module.ddb = FakeDynamoResource(fake_table)
    module.secretsmanager = fake_secrets
    module.ssm = fake_ssm

    return module, fake_s3, fake_table, fake_secrets, fake_ssm


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
    module, fake_s3, fake_table, _, _ = stage_lambda
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


def test_stage_lambda_http_connector(stage_lambda, monkeypatch):
    module, fake_s3, fake_table, _, _ = stage_lambda
    job_id = "job-http"
    payload = b"id,value\n1,99\n"

    class DummyResponse:
        status_code = 200
        headers = {"Content-Type": "text/csv", "Content-Disposition": "attachment; filename=data.csv"}
        content = payload

    def fake_request(method, url, headers=None, data=None, timeout=None):  # noqa: D401 - simple stub
        assert method == "GET"
        assert url == "https://example.com/data.csv"
        return DummyResponse()

    fake_table.put_item(
        {
            "pk": f"job#{job_id}",
            "sk": "meta",
            "status": "QUEUED",
            "source": {"type": "http", "protocol": "https", "url": "https://example.com/data.csv"},
        }
    )

    monkeypatch.setattr(module.requests, "request", fake_request)

    result = module.handler({"jobId": job_id}, None)

    expected_key = f"artifacts/{job_id}/input/data.csv"
    obj = fake_s3.get_object(Bucket=module.ARTIFACTS_BUCKET, Key=expected_key)
    assert obj["Body"].read() == payload

    assert result["metadata"]["sourceType"] == "https"


def test_stage_lambda_sqlite_connector(stage_lambda, tmp_path):
    module, fake_s3, fake_table, _, _ = stage_lambda
    job_id = "job-sqlite"

    db_path = tmp_path / "example.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE metrics(id INTEGER PRIMARY KEY, value INTEGER)")
    conn.execute("INSERT INTO metrics(value) VALUES (42)")
    conn.commit()
    conn.close()

    fake_table.put_item(
        {
            "pk": f"job#{job_id}",
            "sk": "meta",
            "status": "QUEUED",
            "source": {
                "type": "database",
                "url": f"sqlite:///{db_path}",
                "query": "SELECT id, value FROM metrics",
                "filename": "metrics.csv",
            },
        }
    )

    result = module.handler({"jobId": job_id}, None)

    expected_key = f"artifacts/{job_id}/input/metrics.csv"
    obj = fake_s3.get_object(Bucket=module.ARTIFACTS_BUCKET, Key=expected_key)
    body = obj["Body"].read().decode("utf-8")
    assert "id,value" in body
    assert "42" in body

    assert result["metadata"]["sourceType"] == "database"


def test_stage_lambda_warehouse_metadata(stage_lambda, tmp_path):
    module, fake_s3, fake_table, _, _ = stage_lambda
    job_id = "job-warehouse"

    db_path = tmp_path / "warehouse.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE warehouse_data(id INTEGER PRIMARY KEY, amount INTEGER)")
    conn.execute("INSERT INTO warehouse_data(amount) VALUES (7)")
    conn.commit()
    conn.close()

    fake_table.put_item(
        {
            "pk": f"job#{job_id}",
            "sk": "meta",
            "status": "QUEUED",
            "source": {
                "type": "warehouse",
                "warehouseType": "snowflake",
                "url": f"sqlite:///{db_path}",
                "query": "SELECT amount FROM warehouse_data",
            },
        }
    )

    module.handler({"jobId": job_id}, None)

    record = fake_table.get_item({"pk": f"job#{job_id}", "sk": "meta"}).get("Item")
    assert record["inputMetadata"]["sourceType"] == "warehouse:snowflake"


def test_stage_lambda_database_secret(stage_lambda, tmp_path):
    module, fake_s3, fake_table, fake_secrets, _ = stage_lambda
    job_id = "job-secret"

    db_path = tmp_path / "secret.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE secrets(id INTEGER PRIMARY KEY, value INTEGER)")
    conn.execute("INSERT INTO secrets(value) VALUES (9)")
    conn.commit()
    conn.close()

    secret_arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:database"
    fake_secrets.put_secret(secret_arn, json.dumps({"url": f"sqlite:///{db_path}"}))

    fake_table.put_item(
        {
            "pk": f"job#{job_id}",
            "sk": "meta",
            "status": "QUEUED",
            "source": {
                "type": "database",
                "connection": {
                    "type": "secretsManager",
                    "secretArn": secret_arn,
                    "secretField": "url",
                },
                "query": "SELECT value FROM secrets",
                "filename": "secret.csv",
            },
        }
    )

    result = module.handler({"jobId": job_id}, None)

    expected_key = f"artifacts/{job_id}/input/secret.csv"
    obj = fake_s3.get_object(Bucket=module.ARTIFACTS_BUCKET, Key=expected_key)
    body = obj["Body"].read().decode("utf-8")
    assert "value" in body
    assert "9" in body
    assert result["metadata"]["sourceType"] == "database"


def test_stage_lambda_database_parameter(stage_lambda, tmp_path):
    module, fake_s3, fake_table, _, fake_ssm = stage_lambda
    job_id = "job-parameter"

    db_path = tmp_path / "parameter.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE parameters(id INTEGER PRIMARY KEY, value INTEGER)")
    conn.execute("INSERT INTO parameters(value) VALUES (5)")
    conn.commit()
    conn.close()

    parameter_name = "/metricfoundry/databases/parameter"
    fake_ssm.put_parameter(Name=parameter_name, Value=f"sqlite:///{db_path}")

    fake_table.put_item(
        {
            "pk": f"job#{job_id}",
            "sk": "meta",
            "status": "QUEUED",
            "source": {
                "type": "database",
                "connection": {
                    "type": "parameterStore",
                    "parameterName": parameter_name,
                },
                "query": "SELECT value FROM parameters",
            },
        }
    )

    module.handler({"jobId": job_id}, None)

    expected_key = f"artifacts/{job_id}/input/database-export.csv"
    obj = fake_s3.get_object(Bucket=module.ARTIFACTS_BUCKET, Key=expected_key)
    body = obj["Body"].read().decode("utf-8")
    assert "value" in body
    assert "5" in body
