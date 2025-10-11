# tests/test_pipeline_smoke.py
import os
import io
import json
import pathlib
import contextlib

import pytest

from services.workers.graph import run_pipeline


CSV_SMALL = (
    b"sepal_length,sepal_width,petal_length,petal_width\n"
    b"5.1,3.5,1.4,0.2\n"
    b"4.9,3.0,1.4,0.2\n"
)

PHASES_EXPECTED = [
    "ingest",
    "profile",
    "dq_validate",
    "descriptive_stats",
    "ml_inference",
    "nl_report",
    "finalize",
]


def _assert_pipeline_result(res):
    # Phases present & ordered
    assert list(res.phases.keys()) == PHASES_EXPECTED

    # Metrics have core fields
    m = res.metrics
    for k in ["rows", "columns", "bytesRead"]:
        assert k in m, f"missing metric key: {k}"

    # Manifest has expected shape
    assert isinstance(res.manifest, dict)
    for k in ["jobId", "basePath", "artifacts"]:
        assert k in res.manifest

    # Bundles exist and contain real bytes
    ac = res.artifact_contents
    assert isinstance(ac, dict) and len(ac) > 0

    analytics_key = "results/bundles/analytics_bundle.zip"
    viz_key = "results/bundles/visualizations.zip"
    phase_zip_key = "phases/phase_payloads.zip"

    for key in [analytics_key, viz_key, phase_zip_key]:
        assert key in ac, f"missing artifact: {key}"
        spec = ac[key]
        # body may be under "data", "bytes", or "body" depending on the writer
        body = spec.get("data") or spec.get("bytes") or spec.get("body")
        assert isinstance(body, (bytes, bytearray)), f"{key} has no binary body"
        assert len(body) > 0, f"{key} body is empty"


@pytest.fixture
def env_guard():
    """Save/restore env so tests don't leak env vars."""
    before = dict(os.environ)
    try:
        yield
    finally:
        # Restore original environment
        # Remove any keys that were added in the test run
        to_del = set(os.environ.keys()) - set(before.keys())
        for k in to_del:
            os.environ.pop(k, None)
        # Restore original values
        for k, v in before.items():
            os.environ[k] = v


def test_pipeline_no_checkpoint(env_guard, tmp_path):
    # Disable checkpointing
    os.environ["MF_DISABLE_CHECKPOINT"] = "1"

    res = run_pipeline(
        job_id="pytest-smoke-no-ckpt",
        source={"key": "iris.csv"},
        body=io.BytesIO(CSV_SMALL),
        artifact_prefix=f"artifacts/pytest-no-ckpt",
    )

    _assert_pipeline_result(res)

    # With checkpoint disabled, no sqlite file should be created even if a path is set
    ckpt_path = pathlib.Path(os.environ.get("CHECKPOINT_SQLITE_PATH", "graph.ckpt.sqlite"))
    # Allow presence if left over from another run, but size should not change here; safest: just assert not created in tmp run
    assert not ckpt_path.exists(), "checkpoint file should not exist when MF_DISABLE_CHECKPOINT=1"


def test_pipeline_with_checkpoint(env_guard, tmp_path):
    # Enable checkpointing & write to a temp sqlite
    os.environ.pop("MF_DISABLE_CHECKPOINT", None)
    ckpt_path = tmp_path / "graph.ckpt.sqlite"
    os.environ["CHECKPOINT_SQLITE_PATH"] = str(ckpt_path)

    res = run_pipeline(
        job_id="pytest-smoke-ckpt",
        source={"key": "iris.csv"},
        body=io.BytesIO(CSV_SMALL),
        artifact_prefix=f"artifacts/pytest-ckpt",
    )

    _assert_pipeline_result(res)

    # Checkpoint file was created & non-empty
    assert ckpt_path.exists(), "checkpoint file not created"
    assert ckpt_path.stat().st_size > 0, "checkpoint file is empty"
