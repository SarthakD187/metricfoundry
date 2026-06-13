"""Unit tests for services/common/pipeline.py helpers."""
from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import boto3
import pytest
from moto import mock_aws

from services.common.pipeline import (
    ANALYSIS_VERSION,
    artifact_key_for,
    build_results_payload,
    emit_parse_debug,
    error_key_for,
    manifest_key_for,
    object_exists,
    persist_pipeline_outputs,
    phase_key_for,
    result_key_for,
    summarize_phase_payload,
)

BUCKET = "test-pipeline-bucket"
JOB_ID = "job-abc123"
PREFIX = f"artifacts/user/{JOB_ID}"


# ---------------------------------------------------------------------------
# Key helper functions
# ---------------------------------------------------------------------------


def test_result_key_for_with_prefix():
    assert result_key_for(JOB_ID, PREFIX) == f"{PREFIX}/results/results.json"


def test_result_key_for_without_prefix():
    assert result_key_for(JOB_ID) == f"artifacts/{JOB_ID}/results/results.json"


def test_manifest_key_for_with_prefix():
    assert manifest_key_for(JOB_ID, PREFIX) == f"{PREFIX}/results/manifest.json"


def test_manifest_key_for_without_prefix():
    assert manifest_key_for(JOB_ID) == f"artifacts/{JOB_ID}/results/manifest.json"


def test_phase_key_for():
    assert phase_key_for(JOB_ID, "ingest", PREFIX) == f"{PREFIX}/phases/ingest.json"


def test_phase_key_for_without_prefix():
    assert phase_key_for(JOB_ID, "profile") == f"artifacts/{JOB_ID}/phases/profile.json"


def test_error_key_for():
    assert error_key_for(JOB_ID, PREFIX) == f"{PREFIX}/results/error.json"


def test_artifact_key_for():
    assert artifact_key_for(JOB_ID, "results/report.html", PREFIX) == f"{PREFIX}/results/report.html"


def test_artifact_key_for_normalises_double_slash():
    # The function does a single-pass // → / replacement at the join point.
    key = artifact_key_for(JOB_ID, "results/report.html", PREFIX)
    assert f"{PREFIX}/results/report.html" == key


# ---------------------------------------------------------------------------
# summarize_phase_payload
# ---------------------------------------------------------------------------


def test_summarize_phase_payload_with_summary():
    result = summarize_phase_payload({"summary": "done", "other": "ignored"})
    assert result["summary"] == "done"


def test_summarize_phase_payload_with_metrics():
    result = summarize_phase_payload({"metrics": {"rows": 100, "cols": 5}})
    assert result["metrics"] == {"rows": 100, "cols": 5}


def test_summarize_phase_payload_with_dq_score():
    result = summarize_phase_payload({"datasetCompleteness": 0.95})
    assert result["datasetCompleteness"] == 0.95


def test_summarize_phase_payload_falls_back_to_field_list():
    result = summarize_phase_payload({"a": 1, "b": 2, "c": 3})
    assert "fields" in result
    assert "a" in result["fields"]


def test_summarize_phase_payload_empty():
    result = summarize_phase_payload({})
    assert "fields" in result
    assert result["fields"] == []


# ---------------------------------------------------------------------------
# build_results_payload
# ---------------------------------------------------------------------------


def _make_result(**overrides: Any) -> Any:
    defaults = dict(
        phases={
            "ingest": {"summary": "ingested"},
            "profile": {
                "columnProfiles": [
                    {"name": "id", "inferredType": "integer"},
                    {"name": "value", "inferredType": "float"},
                ],
                "shape": {"rows": 10, "columns": 2},
            },
        },
        metrics={"rows": 10, "columns": 2, "bytesRead": 500},
        manifest={"jobId": JOB_ID, "artifacts": []},
        artifact_contents={},
        correlations=[{"feature_x": "id", "feature_y": "value", "pearson_r": 0.9}],
        outliers=[{"column": "value", "value": 999, "z": 4.2}],
        ml_inference={"status": "skipped"},
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_build_results_payload_basic():
    result = _make_result()
    payload = build_results_payload(
        JOB_ID,
        result,
        source_input={"bucket": "src-bucket", "key": "data.csv"},
        artifact_bucket=BUCKET,
        artifact_prefix=PREFIX,
    )

    assert payload["jobId"] == JOB_ID
    assert payload["analysisVersion"] == ANALYSIS_VERSION
    assert "generatedAt" in payload
    assert payload["summary"]["rows"] == 10
    assert payload["summary"]["columns"] == 2
    assert payload["summary"]["bytesRead"] == 500
    assert len(payload["schema"]) == 2
    assert payload["schema"][0] == {"name": "id", "type": "integer"}
    assert payload["links"]["input"] == "s3://src-bucket/data.csv"
    assert payload["links"]["resultsManifest"].startswith(f"s3://{BUCKET}/")
    assert payload["links"]["resultsJson"].startswith(f"s3://{BUCKET}/")
    assert payload["correlations"] == result.correlations
    assert payload["outliers"] == result.outliers
    assert payload["mlInference"] == result.ml_inference


def test_build_results_payload_rows_from_profile_fallback():
    result = _make_result(
        metrics={},  # no rows in metrics
        phases={
            "profile": {
                "shape": {"rows": 50, "columns": 3},
                "columnProfiles": [{"name": "a"}, {"name": "b"}, {"name": "c"}],
            }
        },
    )
    payload = build_results_payload(JOB_ID, result)
    assert payload["summary"]["rows"] == 50
    assert payload["summary"]["columns"] == 3


def test_build_results_payload_no_source_input():
    result = _make_result()
    payload = build_results_payload(JOB_ID, result)
    assert "input" not in payload.get("links", {})


def test_build_results_payload_no_profile_phase():
    result = _make_result(phases={"ingest": {"summary": "done"}})
    payload = build_results_payload(JOB_ID, result)
    assert payload["schema"] == []


# ---------------------------------------------------------------------------
# object_exists
# ---------------------------------------------------------------------------


@mock_aws
def test_object_exists_true():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    s3.put_object(Bucket=BUCKET, Key="some/key.txt", Body=b"hello")
    assert object_exists(s3, BUCKET, "some/key.txt") is True


@mock_aws
def test_object_exists_false():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    assert object_exists(s3, BUCKET, "missing.txt") is False


# ---------------------------------------------------------------------------
# persist_pipeline_outputs
# ---------------------------------------------------------------------------


@mock_aws
def test_persist_pipeline_outputs_uploads_phases_and_artifacts():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)

    result = _make_result(
        phases={
            "ingest": {"rows": 5},
            "profile": {"columnProfiles": [{"name": "x"}]},
        },
        artifact_contents={
            "results/report.html": {"kind": "html", "html": "<p>hi</p>"},
            "results/stats.csv": {
                "kind": "csv",
                "headers": ["col", "count"],
                "rows": [{"col": "x", "count": 5}],
            },
            "results/data.json": {"kind": "json", "data": {"key": "val"}},
            "results/note.txt": {"kind": "text", "text": "analysis done"},
            "results/bundle.zip": {"kind": "binary", "data": b"PKZIP"},
        },
        manifest={"jobId": JOB_ID, "artifacts": [{"key": "artifact.json"}]},
    )

    uploaded = persist_pipeline_outputs(JOB_ID, BUCKET, result, s3_client=s3, artifact_prefix=PREFIX)

    assert "phase:ingest" in uploaded
    assert "phase:profile" in uploaded
    assert "manifest" in uploaded

    # Verify phase files were written
    ingest_key = phase_key_for(JOB_ID, "ingest", PREFIX)
    resp = s3.get_object(Bucket=BUCKET, Key=ingest_key)
    assert json.loads(resp["Body"].read())["rows"] == 5

    # Verify manifest was written
    mkey = manifest_key_for(JOB_ID, PREFIX)
    mresp = s3.get_object(Bucket=BUCKET, Key=mkey)
    assert json.loads(mresp["Body"].read())["jobId"] == JOB_ID

    # Verify artifact files
    html_key = artifact_key_for(JOB_ID, "results/report.html", PREFIX)
    html_resp = s3.get_object(Bucket=BUCKET, Key=html_key)
    assert b"<p>hi</p>" in html_resp["Body"].read()


@mock_aws
def test_persist_pipeline_outputs_csv_artifact():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)

    result = _make_result(
        artifact_contents={
            "results/stats.csv": {
                "kind": "csv",
                "headers": ["col", "mean"],
                "rows": [{"col": "x", "mean": 1.5}],
            }
        },
    )
    persist_pipeline_outputs(JOB_ID, BUCKET, result, s3_client=s3, artifact_prefix=PREFIX)

    csv_key = artifact_key_for(JOB_ID, "results/stats.csv", PREFIX)
    resp = s3.get_object(Bucket=BUCKET, Key=csv_key)
    body = resp["Body"].read().decode()
    assert "col,mean" in body
    assert "x" in body


@mock_aws
def test_persist_pipeline_outputs_unsupported_kind_raises():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)

    result = _make_result(
        artifact_contents={
            "results/bad.xyz": {"kind": "unsupported_type", "data": b""}
        },
    )
    with pytest.raises(ValueError, match="Unsupported artifact kind"):
        persist_pipeline_outputs(JOB_ID, BUCKET, result, s3_client=s3, artifact_prefix=PREFIX)


# ---------------------------------------------------------------------------
# emit_parse_debug
# ---------------------------------------------------------------------------


@mock_aws
def test_emit_parse_debug_writes_file():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)

    profile = {
        "shape": {"rows": 10, "columns": 2},
        "dtypes": {"id": "int64", "value": "float64"},
        "head": [{"id": 1, "value": 2.0}],
        "columnProfiles": [{"name": "id"}, {"name": "value"}],
    }
    key = emit_parse_debug(JOB_ID, BUCKET, profile, s3_client=s3, artifact_prefix=PREFIX)
    assert key is not None

    resp = s3.get_object(Bucket=BUCKET, Key=key)
    debug = json.loads(resp["Body"].read())
    assert debug["shape"] == {"rows": 10, "columns": 2}
    assert debug["dtypes"] == {"id": "int64", "value": "float64"}
    assert len(debug["head"]) == 1
    assert "id" in debug["columnNames"]


@mock_aws
def test_emit_parse_debug_returns_none_on_s3_error():
    s3 = boto3.client("s3", region_name="us-east-1")
    # Bucket does NOT exist — put_object will fail
    result = emit_parse_debug(JOB_ID, "nonexistent-bucket", {}, s3_client=s3)
    assert result is None
