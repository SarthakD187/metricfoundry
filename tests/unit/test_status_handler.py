"""Unit tests for lambdas/status/handler.py using moto."""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TABLE = "test-jobs"
REGION = "us-east-1"


def _create_table(ddb_resource):
    return ddb_resource.create_table(
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


def _seed_job(table, job_id: str, status: str = "QUEUED") -> None:
    table.put_item(
        Item={"pk": f"job#{job_id}", "sk": "meta", "status": status, "createdAt": 0}
    )


@pytest.fixture()
def status_module(monkeypatch):
    monkeypatch.setenv("JOBS_TABLE", TABLE)
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")

    mod = importlib.import_module("lambdas.status.handler")
    return importlib.reload(mod)


# ---------------------------------------------------------------------------
# handler()
# ---------------------------------------------------------------------------


@mock_aws
def test_handler_updates_status_to_failed(status_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    table = _create_table(ddb)
    _seed_job(table, "j1")

    monkeypatch.setattr(status_module, "ddb", ddb)

    result = status_module.handler({"jobId": "j1", "status": "FAILED"}, None)

    assert result == {"jobId": "j1", "status": "FAILED"}
    item = table.get_item(Key={"pk": "job#j1", "sk": "meta"})["Item"]
    assert item["status"] == "FAILED"


@mock_aws
def test_handler_updates_status_to_succeeded(status_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    table = _create_table(ddb)
    _seed_job(table, "j2")

    monkeypatch.setattr(status_module, "ddb", ddb)

    result = status_module.handler({"jobId": "j2", "status": "SUCCEEDED"}, None)

    assert result["status"] == "SUCCEEDED"
    item = table.get_item(Key={"pk": "job#j2", "sk": "meta"})["Item"]
    assert item["status"] == "SUCCEEDED"


@mock_aws
def test_handler_records_error_message(status_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    table = _create_table(ddb)
    _seed_job(table, "j3")

    monkeypatch.setattr(status_module, "ddb", ddb)

    status_module.handler({"jobId": "j3", "status": "FAILED", "error": "something broke"}, None)

    item = table.get_item(Key={"pk": "job#j3", "sk": "meta"})["Item"]
    assert item["error"] == "something broke"
    assert item["status"] == "FAILED"


@mock_aws
def test_handler_truncates_long_error(status_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    table = _create_table(ddb)
    _seed_job(table, "j4")

    monkeypatch.setattr(status_module, "ddb", ddb)

    long_error = "x" * 5000
    status_module.handler({"jobId": "j4", "status": "FAILED", "error": long_error}, None)

    item = table.get_item(Key={"pk": "job#j4", "sk": "meta"})["Item"]
    assert len(item["error"]) == 1000


@mock_aws
def test_handler_uses_failed_as_default_status(status_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    table = _create_table(ddb)
    _seed_job(table, "j5")

    monkeypatch.setattr(status_module, "ddb", ddb)

    result = status_module.handler({"jobId": "j5"}, None)
    assert result["status"] == "FAILED"


@mock_aws
def test_handler_raises_on_missing_job_id(status_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    _create_table(ddb)

    monkeypatch.setattr(status_module, "ddb", ddb)

    with pytest.raises(ValueError, match="jobId is required"):
        status_module.handler({}, None)


@mock_aws
def test_handler_updates_updated_at(status_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    table = _create_table(ddb)
    _seed_job(table, "j6")

    monkeypatch.setattr(status_module, "ddb", ddb)

    before = 0
    status_module.handler({"jobId": "j6", "status": "RUNNING"}, None)

    item = table.get_item(Key={"pk": "job#j6", "sk": "meta"})["Item"]
    assert int(item.get("updatedAt", 0)) > before


# ---------------------------------------------------------------------------
# lambda_handler() wrapper
# ---------------------------------------------------------------------------


@mock_aws
def test_lambda_handler_returns_200_on_success(status_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    table = _create_table(ddb)
    _seed_job(table, "j7")

    monkeypatch.setattr(status_module, "ddb", ddb)

    resp = status_module.lambda_handler({"jobId": "j7", "status": "SUCCEEDED"}, None)
    assert resp["statusCode"] == 200
    assert resp["body"]["status"] == "SUCCEEDED"


@mock_aws
def test_lambda_handler_returns_400_on_missing_job_id(status_module, monkeypatch):
    ddb = boto3.resource("dynamodb", region_name=REGION)
    _create_table(ddb)

    monkeypatch.setattr(status_module, "ddb", ddb)

    resp = status_module.lambda_handler({}, None)
    assert resp["statusCode"] == 400
    assert "jobId is required" in resp["body"]["error"]
