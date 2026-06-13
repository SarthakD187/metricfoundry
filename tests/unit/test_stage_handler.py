"""Unit tests for lambdas/stage/handler.py using moto."""
from __future__ import annotations

import importlib
import io
import json
import sqlite3
import sys
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TABLE = "test-jobs"
BUCKET = "test-artifacts"
REGION = "us-east-1"


def _create_aws_resources(ddb_resource, s3_client):
    """Create moto DynamoDB table and S3 bucket."""
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


def _seed_job(table, job_id: str, source: dict) -> None:
    table.put_item(
        Item={
            "pk": f"job#{job_id}",
            "sk": "meta",
            "status": "QUEUED",
            "source": source,
        }
    )


def _zip_bytes(name: str, content: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr(name, content)
    return buf.getvalue()


class _FakePublicDNS:
    """Monkeypatch socket.getaddrinfo to always return a public IP."""

    @staticmethod
    def getaddrinfo(host, port, *args, **kwargs):
        import socket

        return [(socket.AF_INET, None, None, None, ("93.184.216.34", port or 0))]


class _FakePrivateDNS:
    """Returns a link-local/private IP (IMDS-style)."""

    @staticmethod
    def getaddrinfo(host, port, *args, **kwargs):
        import socket

        return [(socket.AF_INET, None, None, None, ("169.254.169.254", port or 0))]


@pytest.fixture()
def stage_module(monkeypatch):
    monkeypatch.setenv("JOBS_TABLE", TABLE)
    monkeypatch.setenv("ARTIFACTS_BUCKET", BUCKET)
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")

    mod = importlib.import_module("lambdas.stage.handler")
    return importlib.reload(mod)


# ---------------------------------------------------------------------------
# _detect_format
# ---------------------------------------------------------------------------


def test_detect_format_csv(stage_module):
    assert stage_module._detect_format("data.csv", None) == "csv"


def test_detect_format_tsv(stage_module):
    assert stage_module._detect_format("data.tsv", None) == "tsv"
    assert stage_module._detect_format("data.tab", None) == "tsv"


def test_detect_format_jsonl(stage_module):
    assert stage_module._detect_format("data.jsonl", None) == "jsonl"
    assert stage_module._detect_format("data.ndjson", None) == "jsonl"


def test_detect_format_json(stage_module):
    assert stage_module._detect_format("data.json", None) == "json"


def test_detect_format_excel(stage_module):
    assert stage_module._detect_format("book.xlsx", None) == "excel"
    assert stage_module._detect_format("book.xls", None) == "excel"


def test_detect_format_parquet(stage_module):
    assert stage_module._detect_format("data.parquet", None) == "parquet"
    assert stage_module._detect_format("data.pq", None) == "parquet"


def test_detect_format_sqlite(stage_module):
    assert stage_module._detect_format("db.sqlite", None) == "sqlite"
    assert stage_module._detect_format("db.db", None) == "sqlite"


def test_detect_format_archives(stage_module):
    assert stage_module._detect_format("arch.zip", None) == "zip"
    assert stage_module._detect_format("arch.tar.gz", None) == "tar"
    assert stage_module._detect_format("arch.tgz", None) == "tar"
    assert stage_module._detect_format("arch.tar", None) == "tar"
    assert stage_module._detect_format("arch.gz", None) == "gzip"


def test_detect_format_from_content_type(stage_module):
    assert stage_module._detect_format("file", "text/csv") == "csv"
    assert stage_module._detect_format("file", "application/json") == "json"
    assert stage_module._detect_format("file", "application/octet-stream; x-parquet") == "parquet"


def test_detect_format_unknown(stage_module):
    assert stage_module._detect_format("file.bin", None) == "unknown"


# ---------------------------------------------------------------------------
# _safe_filename
# ---------------------------------------------------------------------------


def test_safe_filename_uses_fallback(stage_module):
    assert stage_module._safe_filename(None, "fallback.csv") == "fallback.csv"
    assert stage_module._safe_filename("", "fallback.csv") == "fallback.csv"


def test_safe_filename_sanitises_dotdot(stage_module):
    result = stage_module._safe_filename("../evil.txt", "fallback.txt")
    assert ".." not in result


# ---------------------------------------------------------------------------
# handler — upload source
# ---------------------------------------------------------------------------


@mock_aws
def test_handler_upload_csv(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    job_id = "job-upload-csv"
    src_bucket = "src-bucket"
    src_key = "uploads/data.csv"
    csv_body = b"id,value\n1,10\n2,20\n"

    s3.create_bucket(Bucket=src_bucket)
    s3.put_object(Bucket=src_bucket, Key=src_key, Body=csv_body, ContentType="text/csv")
    _seed_job(table, job_id, {"type": "upload", "bucket": src_bucket, "key": src_key})

    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    result = stage_module.handler({"jobId": job_id}, None)

    expected_key = f"artifacts/{job_id}/input/data.csv"
    assert result["input"]["key"] == expected_key
    assert result["metadata"]["format"] == "csv"
    assert result["metadata"]["sourceType"] == "upload"
    assert result["artifactPrefix"] == f"artifacts/{job_id}"

    staged = s3.get_object(Bucket=BUCKET, Key=expected_key)
    assert staged["Body"].read() == csv_body

    item = table.get_item(Key={"pk": f"job#{job_id}", "sk": "meta"})["Item"]
    assert item["status"] == "STAGED"
    assert item["inputKey"] == expected_key


@mock_aws
def test_handler_upload_parquet(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    job_id = "job-upload-parquet"
    src_bucket = "src-bucket"
    src_key = "uploads/data.parquet"

    s3.create_bucket(Bucket=src_bucket)
    s3.put_object(Bucket=src_bucket, Key=src_key, Body=b"PARQUET", ContentType="application/octet-stream")
    _seed_job(table, job_id, {"type": "upload", "bucket": src_bucket, "key": src_key})

    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    result = stage_module.handler({"jobId": job_id}, None)

    assert result["metadata"]["format"] == "parquet"


@mock_aws
def test_handler_upload_zip(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    job_id = "job-upload-zip"
    src_bucket = "src-bucket"
    src_key = "uploads/data.zip"
    zip_body = _zip_bytes("dataset.csv", b"id,value\n1,2\n")

    s3.create_bucket(Bucket=src_bucket)
    s3.put_object(Bucket=src_bucket, Key=src_key, Body=zip_body, ContentType="application/zip")
    _seed_job(table, job_id, {"type": "upload", "bucket": src_bucket, "key": src_key})

    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    result = stage_module.handler({"jobId": job_id}, None)
    assert result["metadata"]["format"] == "zip"


# ---------------------------------------------------------------------------
# handler — S3 source
# ---------------------------------------------------------------------------


@mock_aws
def test_handler_s3_source(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    job_id = "job-s3-source"
    ext_bucket = "external-bucket"
    ext_key = "data/metrics.jsonl"

    s3.create_bucket(Bucket=ext_bucket)
    s3.put_object(Bucket=ext_bucket, Key=ext_key, Body=b'{"id":1}\n', ContentType="application/json")
    _seed_job(table, job_id, {"type": "s3", "uri": f"s3://{ext_bucket}/{ext_key}"})

    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    result = stage_module.handler({"jobId": job_id}, None)

    expected_key = f"artifacts/{job_id}/input/metrics.jsonl"
    assert result["input"]["key"] == expected_key
    assert result["metadata"]["sourceType"] == "s3"
    assert result["metadata"]["format"] == "jsonl"


# ---------------------------------------------------------------------------
# handler — HTTP source (CSV)
# ---------------------------------------------------------------------------


@mock_aws
def test_handler_http_csv(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    job_id = "job-http-csv"
    csv_payload = b"a,b\n1,2\n3,4\n"

    class _Resp:
        status_code = 200
        headers = {
            "Content-Type": "text/csv",
            "Content-Disposition": "attachment; filename=data.csv",
            "Content-Length": str(len(csv_payload)),
        }

        def iter_content(self, chunk_size=8192):
            yield csv_payload

        def close(self):
            pass

    monkeypatch.setattr(stage_module.requests, "request", lambda *a, **kw: _Resp())
    monkeypatch.setattr(stage_module.socket, "getaddrinfo", _FakePublicDNS.getaddrinfo)

    _seed_job(table, job_id, {"type": "http", "url": "https://example.com/data.csv"})
    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    result = stage_module.handler({"jobId": job_id}, None)

    assert result["metadata"]["sourceType"] == "http"
    assert result["metadata"]["format"] == "csv"
    assert result["metadata"]["schemaSample"] == ["a", "b"]
    assert result["metadata"]["rowCountEstimate"] == 2

    # Normalized and original are both staged
    normalized_key = f"artifacts/{job_id}/staged/normalized.csv"
    obj = s3.get_object(Bucket=BUCKET, Key=normalized_key)
    assert obj["Body"].read() == csv_payload

    # Manifest is written
    manifest_key = f"artifacts/{job_id}/manifest.json"
    mobj = s3.get_object(Bucket=BUCKET, Key=manifest_key)
    manifest = json.loads(mobj["Body"].read())
    assert manifest["format"] == "csv"
    assert manifest["source"]["url"] == "https://example.com/data.csv"


@mock_aws
def test_handler_http_json(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    job_id = "job-http-json"
    records = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
    json_payload = json.dumps(records).encode()

    class _Resp:
        status_code = 200
        headers = {"Content-Type": "application/json", "Content-Length": str(len(json_payload))}

        def iter_content(self, chunk_size=8192):
            yield json_payload

        def close(self):
            pass

    monkeypatch.setattr(stage_module.requests, "request", lambda *a, **kw: _Resp())
    monkeypatch.setattr(stage_module.socket, "getaddrinfo", _FakePublicDNS.getaddrinfo)

    _seed_job(table, job_id, {"type": "http", "url": "https://example.com/data.json"})
    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    result = stage_module.handler({"jobId": job_id}, None)
    assert result["metadata"]["format"] == "jsonl"
    assert result["metadata"]["schemaSample"] == ["id", "val"]
    assert result["metadata"]["rowCountEstimate"] == 2


@mock_aws
def test_handler_http_blocks_private_ip(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    job_id = "job-http-private"
    _seed_job(table, job_id, {"type": "http", "url": "https://metadata.internal/latest"})
    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)
    monkeypatch.setattr(stage_module.socket, "getaddrinfo", _FakePrivateDNS.getaddrinfo)

    with pytest.raises(ValueError, match="private"):
        stage_module.handler({"jobId": job_id}, None)

    item = table.get_item(Key={"pk": f"job#{job_id}", "sk": "meta"})["Item"]
    assert item["status"] == "FAILED"


@mock_aws
def test_handler_http_enforces_max_size(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    monkeypatch.setattr(stage_module, "MAX_HTTP_BYTES", 8)

    job_id = "job-http-large"
    payload = b"0123456789"

    class _BigResp:
        status_code = 200
        headers = {"Content-Type": "text/csv", "Content-Length": str(len(payload))}

        def iter_content(self, chunk_size=8192):
            yield payload

        def close(self):
            pass

    monkeypatch.setattr(stage_module.requests, "request", lambda *a, **kw: _BigResp())
    monkeypatch.setattr(stage_module.socket, "getaddrinfo", _FakePublicDNS.getaddrinfo)
    _seed_job(table, job_id, {"type": "http", "url": "https://example.com/big.csv"})
    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    with pytest.raises(ValueError, match="MAX_HTTP_BYTES"):
        stage_module.handler({"jobId": job_id}, None)

    item = table.get_item(Key={"pk": f"job#{job_id}", "sk": "meta"})["Item"]
    assert item["status"] == "FAILED"


@mock_aws
def test_handler_http_error_status(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    job_id = "job-http-500"

    class _ErrResp:
        status_code = 500
        headers = {}

        def iter_content(self, chunk_size=8192):
            return iter([])

        def close(self):
            pass

    monkeypatch.setattr(stage_module.requests, "request", lambda *a, **kw: _ErrResp())
    monkeypatch.setattr(stage_module.socket, "getaddrinfo", _FakePublicDNS.getaddrinfo)
    _seed_job(table, job_id, {"type": "http", "url": "https://example.com/err"})
    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    with pytest.raises(ValueError, match="status 500"):
        stage_module.handler({"jobId": job_id}, None)


# ---------------------------------------------------------------------------
# handler — database source (SQLite)
# ---------------------------------------------------------------------------


@mock_aws
def test_handler_database_sqlite(stage_module, monkeypatch, tmp_path):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE data(id INTEGER PRIMARY KEY, value INTEGER)")
    conn.execute("INSERT INTO data VALUES (1, 42)")
    conn.commit()
    conn.close()

    job_id = "job-db-sqlite"
    _seed_job(table, job_id, {
        "type": "database",
        "url": f"sqlite:///{db_path}",
        "query": "SELECT id, value FROM data",
        "filename": "output.csv",
    })
    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    result = stage_module.handler({"jobId": job_id}, None)

    expected_key = f"artifacts/{job_id}/input/output.csv"
    assert result["input"]["key"] == expected_key
    assert result["metadata"]["sourceType"] == "database"

    obj = s3.get_object(Bucket=BUCKET, Key=expected_key)
    body = obj["Body"].read().decode()
    assert "id,value" in body
    assert "42" in body


@mock_aws
def test_handler_database_jsonl_format(stage_module, monkeypatch, tmp_path):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    db_path = tmp_path / "test2.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t(v TEXT)")
    conn.execute("INSERT INTO t VALUES ('hello')")
    conn.commit()
    conn.close()

    job_id = "job-db-jsonl"
    _seed_job(table, job_id, {
        "type": "database",
        "url": f"sqlite:///{db_path}",
        "query": "SELECT v FROM t",
        "format": "jsonl",
    })
    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    result = stage_module.handler({"jobId": job_id}, None)
    expected_key = f"artifacts/{job_id}/input/database-export.csv"
    obj = s3.get_object(Bucket=BUCKET, Key=expected_key)
    body = obj["Body"].read().decode()
    assert "hello" in body


# ---------------------------------------------------------------------------
# handler — warehouse source
# ---------------------------------------------------------------------------


@mock_aws
def test_handler_warehouse_sqlite_fallback(stage_module, monkeypatch, tmp_path):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    db_path = tmp_path / "wh.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE wh(amount INTEGER)")
    conn.execute("INSERT INTO wh VALUES (99)")
    conn.commit()
    conn.close()

    job_id = "job-warehouse"
    _seed_job(table, job_id, {
        "type": "warehouse",
        "warehouseType": "databricks",
        "url": f"sqlite:///{db_path}",
        "query": "SELECT amount FROM wh",
    })
    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    result = stage_module.handler({"jobId": job_id}, None)
    assert result["metadata"]["sourceType"] == "warehouse:databricks"


@mock_aws
def test_handler_warehouse_redshift_uses_unload(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)
    executed = []

    class _Cursor:
        def execute(self, statement):
            executed.append(statement)
            s3.put_object(
                Bucket=BUCKET,
                Key="artifacts/job-redshift/native/redshift/job-redshift-000",
                Body=b"id,value\n1,10\n",
                ContentType="text/csv",
            )

        def close(self):
            pass

    class _Connection:
        def cursor(self):
            return _Cursor()

        def commit(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr(stage_module, "_connect_redshift", lambda **kwargs: _Connection())
    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    job_id = "job-redshift"
    _seed_job(table, job_id, {
        "type": "warehouse",
        "warehouseType": "redshift",
        "url": "redshift://user:pass@example.abc.us-east-1.redshift.amazonaws.com:5439/dev",
        "query": "SELECT id, value FROM metrics",
        "unloadIamRole": "arn:aws:iam::123456789012:role/redshift-unload",
        "filename": "metrics.csv",
    })

    result = stage_module.handler({"jobId": job_id}, None)

    assert "UNLOAD" in executed[0]
    assert "IAM_ROLE" in executed[0]
    assert result["metadata"]["sourceType"] == "warehouse:redshift"
    obj = s3.get_object(Bucket=BUCKET, Key="artifacts/job-redshift/input/metrics.csv")
    assert obj["Body"].read() == b"id,value\n1,10\n"


@mock_aws
def test_handler_warehouse_snowflake_uses_copy_into(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)
    executed = []

    class _Cursor:
        def execute(self, statement):
            executed.append(statement)
            if statement.startswith("COPY INTO"):
                s3.put_object(
                    Bucket=BUCKET,
                    Key="artifacts/job-snowflake/native/snowflake/job-snowflake/export.csv",
                    Body=b"amount\n7\n",
                    ContentType="text/csv",
                )

        def close(self):
            pass

    class _Connection:
        def cursor(self):
            return _Cursor()

        def close(self):
            pass

    monkeypatch.setattr(stage_module, "_connect_snowflake", lambda **kwargs: _Connection())
    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    job_id = "job-snowflake"
    _seed_job(table, job_id, {
        "type": "warehouse",
        "warehouseType": "snowflake",
        "connectionDetails": {
            "account": "acct",
            "user": "user",
            "password": "pass",
            "database": "db",
        },
        "storageIntegration": "metricfoundry_s3_int",
        "query": "SELECT amount FROM warehouse_data",
        "filename": "export.csv",
    })

    result = stage_module.handler({"jobId": job_id}, None)

    assert executed[0].startswith("CREATE OR REPLACE TEMPORARY STAGE")
    assert executed[1].startswith("COPY INTO")
    assert "FILE_FORMAT" in executed[1]
    assert result["metadata"]["sourceType"] == "warehouse:snowflake"
    obj = s3.get_object(Bucket=BUCKET, Key="artifacts/job-snowflake/input/export.csv")
    assert obj["Body"].read() == b"amount\n7\n"


@mock_aws
def test_handler_warehouse_bigquery_uses_storage_read(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    class _Field:
        def __init__(self, name):
            self.name = name

    class _Table:
        project = "proj"
        dataset_id = "dataset"
        table_id = "table"
        schema = [_Field("id"), _Field("value")]

    class _QueryJob:
        destination = "proj.dataset.temp_table"

        def result(self):
            return None

    class _BQClient:
        project = "proj"

        def query(self, query, job_config=None):
            self.query_text = query
            return _QueryJob()

        def get_table(self, table_ref):
            self.table_ref = table_ref
            return _Table()

    class _ReadOptions:
        selected_fields = []
        row_restriction = ""

    class _ReadSession:
        class TableReadOptions(_ReadOptions):
            pass

        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class _Types:
        class DataFormat:
            ARROW = "ARROW"

        ReadSession = _ReadSession

    class _BQModule:
        @staticmethod
        def QueryJobConfig(**kwargs):
            return kwargs

    class _StorageModule:
        ReadSession = _ReadSession
        types = _Types

    class _Stream:
        name = "stream-1"

    class _Session:
        streams = [_Stream()]

    class _Reader:
        def rows(self, session):
            return [{"id": 1, "value": "alpha"}, {"id": 2, "value": "beta"}]

    class _ReadClient:
        def create_read_session(self, **kwargs):
            self.kwargs = kwargs
            return _Session()

        def read_rows(self, stream_name):
            return _Reader()

    monkeypatch.setattr(stage_module, "_bigquery_clients", lambda source, connection: (_BQClient(), _ReadClient(), _BQModule, _StorageModule))
    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    job_id = "job-bigquery"
    _seed_job(table, job_id, {
        "type": "warehouse",
        "warehouseType": "bigquery",
        "connectionDetails": {"project": "proj"},
        "query": "SELECT id, value FROM dataset.table",
        "filename": "bq.csv",
    })

    result = stage_module.handler({"jobId": job_id}, None)

    assert result["metadata"]["sourceType"] == "warehouse:bigquery"
    obj = s3.get_object(Bucket=BUCKET, Key="artifacts/job-bigquery/input/bq.csv")
    assert obj["Body"].read().decode() == "id,value\r\n1,alpha\r\n2,beta\r\n"


# ---------------------------------------------------------------------------
# handler — SecretsManager resolution
# ---------------------------------------------------------------------------


@mock_aws
def test_handler_database_secretsmanager(stage_module, monkeypatch, tmp_path):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    sm = boto3.client("secretsmanager", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    db_path = tmp_path / "secret.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t(x INTEGER)")
    conn.execute("INSERT INTO t VALUES (7)")
    conn.commit()
    conn.close()

    secret_arn_resp = sm.create_secret(
        Name="db-secret",
        SecretString=json.dumps({"url": f"sqlite:///{db_path}"}),
    )
    secret_arn = secret_arn_resp["ARN"]

    job_id = "job-sm-secret"
    _seed_job(table, job_id, {
        "type": "database",
        "connection": {"type": "secretsManager", "secretArn": secret_arn, "secretField": "url"},
        "query": "SELECT x FROM t",
    })
    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)
    monkeypatch.setattr(stage_module, "secretsmanager", sm)

    result = stage_module.handler({"jobId": job_id}, None)
    assert result["metadata"]["sourceType"] == "database"

    obj = s3.get_object(Bucket=BUCKET, Key=result["input"]["key"])
    assert b"7" in obj["Body"].read()


# ---------------------------------------------------------------------------
# handler — SSM ParameterStore resolution
# ---------------------------------------------------------------------------


@mock_aws
def test_handler_database_ssm_parameter(stage_module, monkeypatch, tmp_path):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    ssm = boto3.client("ssm", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    db_path = tmp_path / "ssm.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t(y INTEGER)")
    conn.execute("INSERT INTO t VALUES (55)")
    conn.commit()
    conn.close()

    ssm.put_parameter(Name="/mf/db/url", Value=f"sqlite:///{db_path}", Type="SecureString")

    job_id = "job-ssm-param"
    _seed_job(table, job_id, {
        "type": "database",
        "connection": {"type": "parameterStore", "parameterName": "/mf/db/url"},
        "query": "SELECT y FROM t",
    })
    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)
    monkeypatch.setattr(stage_module, "ssm", ssm)

    result = stage_module.handler({"jobId": job_id}, None)
    obj = s3.get_object(Bucket=BUCKET, Key=result["input"]["key"])
    assert b"55" in obj["Body"].read()


# ---------------------------------------------------------------------------
# handler — error paths
# ---------------------------------------------------------------------------


@mock_aws
def test_handler_raises_on_missing_job_id(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    _create_aws_resources(ddb, s3)

    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    with pytest.raises(ValueError, match="jobId is required"):
        stage_module.handler({}, None)


@mock_aws
def test_handler_raises_on_unknown_job(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    _create_aws_resources(ddb, s3)

    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    with pytest.raises(ValueError, match="not found"):
        stage_module.handler({"jobId": "no-such-job"}, None)


@mock_aws
def test_handler_upload_file_not_ready(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    job_id = "job-not-ready"
    # Upload job pointing to a key that doesn't exist in S3
    _seed_job(table, job_id, {"type": "upload", "bucket": BUCKET, "key": f"uploads/missing.csv"})
    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    with pytest.raises(stage_module.FileNotReadyError):
        stage_module.handler({"jobId": job_id}, None)


@mock_aws
def test_handler_unsupported_source_type(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    job_id = "job-bad-type"
    _seed_job(table, job_id, {"type": "ftp", "uri": "ftp://example.com/data.csv"})
    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    with pytest.raises(ValueError, match="Unsupported source type"):
        stage_module.handler({"jobId": job_id}, None)


# ---------------------------------------------------------------------------
# handler — uses job's artifactPrefix if stored
# ---------------------------------------------------------------------------


@mock_aws
def test_handler_respects_stored_artifact_prefix(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    job_id = "job-custom-prefix"
    custom_prefix = "artifacts/tenant-abc/job-custom-prefix"
    src_bucket = "src-bucket"
    src_key = "uploads/file.csv"

    s3.create_bucket(Bucket=src_bucket)
    s3.put_object(Bucket=src_bucket, Key=src_key, Body=b"a,b\n1,2\n")

    table.put_item(Item={
        "pk": f"job#{job_id}",
        "sk": "meta",
        "status": "QUEUED",
        "source": {"type": "upload", "bucket": src_bucket, "key": src_key},
        "artifactPrefix": custom_prefix,
    })
    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    result = stage_module.handler({"jobId": job_id}, None)
    assert result["artifactPrefix"] == custom_prefix
    assert result["input"]["key"].startswith(custom_prefix)


# ---------------------------------------------------------------------------
# lambda_handler wrapper
# ---------------------------------------------------------------------------


@mock_aws
def test_lambda_handler_returns_200_on_success(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    job_id = "job-lh-ok"
    src_bucket = "src-bucket"
    src_key = "uploads/data.csv"
    s3.create_bucket(Bucket=src_bucket)
    s3.put_object(Bucket=src_bucket, Key=src_key, Body=b"a,b\n1,2\n")
    _seed_job(table, job_id, {"type": "upload", "bucket": src_bucket, "key": src_key})

    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    resp = stage_module.lambda_handler({"jobId": job_id}, None)
    assert resp["statusCode"] == 200
    assert resp["body"]["metadata"]["format"] == "csv"


@mock_aws
def test_lambda_handler_returns_400_on_value_error(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    _create_aws_resources(ddb, s3)

    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    resp = stage_module.lambda_handler({}, None)
    assert resp["statusCode"] == 400


@mock_aws
def test_lambda_handler_returns_409_on_file_not_ready(stage_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = _create_aws_resources(ddb, s3)

    job_id = "job-lh-not-ready"
    _seed_job(table, job_id, {"type": "upload", "bucket": BUCKET, "key": "uploads/missing.csv"})
    monkeypatch.setattr(stage_module, "s3", s3)
    monkeypatch.setattr(stage_module, "ddb", ddb)

    resp = stage_module.lambda_handler({"jobId": job_id}, None)
    assert resp["statusCode"] == 409


# ---------------------------------------------------------------------------
# _normalise_http_download — edge cases
# ---------------------------------------------------------------------------


def test_normalise_http_download_json_array(stage_module):
    download = stage_module.HttpDownload(
        url="https://x.com/data.json",
        filename="data.json",
        content=json.dumps([{"a": 1}, {"a": 2}]).encode(),
        content_type="application/json",
    )
    norm, fmt, schema, count, ext, ct = stage_module._normalise_http_download(download)
    assert fmt == "jsonl"
    assert count == 2
    assert schema == ["a"]
    assert ext == ".jsonl"


def test_normalise_http_download_json_data_wrapper(stage_module):
    payload = json.dumps({"data": [{"x": 1}]}).encode()
    download = stage_module.HttpDownload(
        url="https://x.com/api",
        filename="response",
        content=payload,
        content_type="application/json",
    )
    _, fmt, schema, count, _, _ = stage_module._normalise_http_download(download)
    assert fmt == "jsonl"
    assert count == 1
    assert "x" in schema


def test_normalise_http_download_invalid_content_type_sniff(stage_module):
    """Content that looks like JSON but has no extension or content-type gets sniffed."""
    download = stage_module.HttpDownload(
        url="https://x.com/data",
        filename="data",
        content=b'[{"k":"v"}]',
        content_type=None,
    )
    _, fmt, _, count, _, _ = stage_module._normalise_http_download(download)
    assert fmt == "jsonl"
    assert count == 1


def test_normalise_http_download_tsv_sniff(stage_module):
    download = stage_module.HttpDownload(
        url="https://x.com/data",
        filename="data.tsv",
        content=b"col1\tcol2\n1\t2\n",
        content_type=None,
    )
    _, fmt, schema, count, ext, _ = stage_module._normalise_http_download(download)
    assert fmt == "tsv"
    assert ext == ".tsv"
    assert "col1" in schema


def test_normalise_http_download_unsupported_raises(stage_module):
    download = stage_module.HttpDownload(
        url="https://x.com/data.bin",
        filename="data.bin",
        content=b"\x00\x01\x02",
        content_type="application/octet-stream",
    )
    with pytest.raises(ValueError, match="only supports CSV or JSON"):
        stage_module._normalise_http_download(download)
