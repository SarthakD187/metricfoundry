import importlib
import io
import json
import sqlite3
import sys
from pathlib import Path
import zipfile

import pytest

from .utils.aws import (
    FakeDynamoResource,
    FakeDynamoTable,
    FakeS3,
    FakeSSM,
    FakeSecretsManager,
)


def _zip_bytes(name: str, content: bytes) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name, content)
    return buffer.getvalue()


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

    def fake_public_getaddrinfo(host, port, *args, **kwargs):
        return [
            (
                module.socket.AF_INET,
                None,
                None,
                None,
                ("93.184.216.34", port or 0),
            )
        ]

    monkeypatch.setattr(module.socket, "getaddrinfo", fake_public_getaddrinfo)

    return module, fake_s3, fake_table, fake_secrets, fake_ssm


def test_stage_lambda_registers_warehouse_dialects(stage_lambda, monkeypatch):
    module, *_ = stage_lambda

    registered: list[tuple[str, str, str]] = []

    def fake_register(name: str, module_path: str, cls_name: str) -> None:
        registered.append((name, module_path, cls_name))

    monkeypatch.setattr(module.registry, "register", fake_register)

    available = {
        "snowflake.sqlalchemy",
        "sqlalchemy_redshift.dialect",
        "pybigquery.sqlalchemy_bigquery",
        "databricks.sqlalchemy",
    }

    def fake_import(name: str):
        if name in available:
            return object()
        raise ImportError

    monkeypatch.setattr(module, "import_module", fake_import)

    module._register_sqlalchemy_dialects()

    expected_entries = [
        ("snowflake", "snowflake.sqlalchemy", "dialect"),
        ("redshift", "sqlalchemy_redshift.dialect", "RedshiftDialect_psycopg2"),
        ("redshift+redshift_connector", "sqlalchemy_redshift.dialect", "RedshiftDialect_redshift_connector"),
        ("bigquery", "pybigquery.sqlalchemy_bigquery", "BigQueryDialect"),
        ("bigquery+pybigquery", "pybigquery.sqlalchemy_bigquery", "BigQueryDialect"),
        ("databricks", "databricks.sqlalchemy", "DatabricksDialect"),
        ("databricks+connector", "databricks.sqlalchemy", "DatabricksDialect"),
    ]

    for entry in expected_entries:
        assert entry in registered


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
        (
            {"type": "upload", "bucket": "incoming", "key": "uploads/archive.zip"},
            "incoming",
            "uploads/archive.zip",
            _zip_bytes("dataset.csv", b"id,value\n1,2\n"),
            "application/zip",
            "zip",
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
        def __init__(self, body: bytes):
            self.status_code = 200
            self._body = body
            self.headers = {
                "Content-Type": "text/csv",
                "Content-Disposition": "attachment; filename=data.csv",
                "Content-Length": str(len(body)),
            }

        def iter_content(self, chunk_size=8192):
            for index in range(0, len(self._body), chunk_size):
                yield self._body[index : index + chunk_size]

        def close(self):
            pass

    def fake_request(method, url, headers=None, data=None, timeout=None, **kwargs):  # noqa: D401 - simple stub
        assert method == "GET"
        assert url == "https://example.com/data.csv"
        assert timeout == (5.0, 20.0)
        assert kwargs.get("stream") is True
        assert kwargs.get("allow_redirects") is False
        return DummyResponse(payload)

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

    expected_normalized = f"artifacts/{job_id}/staged/normalized.csv"
    expected_original = f"artifacts/{job_id}/staged/original.csv"

    normalized = fake_s3.get_object(Bucket=module.ARTIFACTS_BUCKET, Key=expected_normalized)
    assert normalized["Body"].read() == payload

    original = fake_s3.get_object(Bucket=module.ARTIFACTS_BUCKET, Key=expected_original)
    assert original["Body"].read() == payload

    manifest_key = f"artifacts/{job_id}/manifest.json"
    manifest_obj = fake_s3.get_object(Bucket=module.ARTIFACTS_BUCKET, Key=manifest_key)
    manifest = json.loads(manifest_obj["Body"].read().decode("utf-8"))

    assert manifest["format"] == "csv"
    assert manifest["source"] == {"type": "http", "url": "https://example.com/data.csv"}
    assert manifest["schemaSample"] == ["id", "value"]
    assert manifest["rowCountEstimate"] == 1

    assert result["input"]["key"] == expected_normalized
    assert result["metadata"]["sourceType"] == "http"
    assert result["metadata"]["manifestKey"] == manifest_key
    assert result["metadata"]["schemaSample"] == ["id", "value"]
    assert result["metadata"]["rowCountEstimate"] == 1

    record = fake_table.get_item({"pk": f"job#{job_id}", "sk": "meta"}).get("Item")
    assert record is not None
    assert record["inputKey"] == expected_normalized
    assert record["manifestKey"] == manifest_key
    assert record["inputMetadata"]["schemaSample"] == ["id", "value"]


def test_stage_lambda_http_blocks_private_host(stage_lambda, monkeypatch):
    module, _, fake_table, _, _ = stage_lambda
    job_id = "job-http-private"

    fake_table.put_item(
        {
            "pk": f"job#{job_id}",
            "sk": "meta",
            "status": "QUEUED",
            "source": {"type": "http", "url": "https://metadata.internal/latest"},
        }
    )

    def fake_private_getaddrinfo(host, port, *args, **kwargs):
        return [
            (
                module.socket.AF_INET,
                None,
                None,
                None,
                ("169.254.169.254", port or 0),
            )
        ]

    monkeypatch.setattr(module.socket, "getaddrinfo", fake_private_getaddrinfo)

    with pytest.raises(ValueError) as excinfo:
        module.handler({"jobId": job_id}, None)

    assert "private" in str(excinfo.value).lower()

    record = fake_table.get_item({"pk": f"job#{job_id}", "sk": "meta"}).get("Item")
    assert record is not None
    assert record["status"] == module.STATUS_FAILED
    assert "private" in record.get("error", "").lower()


def test_stage_lambda_http_enforces_max_size(stage_lambda, monkeypatch):
    module, _, fake_table, _, _ = stage_lambda
    module.MAX_HTTP_BYTES = 8
    job_id = "job-http-large"
    payload = b"0123456789"

    class DummyResponse:
        def __init__(self, body: bytes):
            self.status_code = 200
            self._body = body
            self.headers = {
                "Content-Type": "text/csv",
                "Content-Disposition": "attachment; filename=data.csv",
                "Content-Length": str(len(body)),
            }

        def iter_content(self, chunk_size=8192):
            for index in range(0, len(self._body), chunk_size):
                yield self._body[index : index + chunk_size]

        def close(self):
            pass

    def fake_request(method, url, headers=None, data=None, timeout=None, **kwargs):
        assert kwargs.get("stream") is True
        assert kwargs.get("allow_redirects") is False
        return DummyResponse(payload)

    fake_table.put_item(
        {
            "pk": f"job#{job_id}",
            "sk": "meta",
            "status": "QUEUED",
            "source": {"type": "http", "url": "https://example.com/data.csv"},
        }
    )

    monkeypatch.setattr(module.requests, "request", fake_request)

    with pytest.raises(ValueError) as excinfo:
        module.handler({"jobId": job_id}, None)

    assert "max_http_bytes" in str(excinfo.value).lower()

    record = fake_table.get_item({"pk": f"job#{job_id}", "sk": "meta"}).get("Item")
    assert record is not None
    assert record["status"] == module.STATUS_FAILED
    assert "max_http_bytes" in record.get("error", "").lower()


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
