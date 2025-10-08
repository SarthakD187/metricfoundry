import json

import pytest


def metric_names(metric_calls):
    names = []
    for call in metric_calls:
        for metric in call.get("MetricData", []):
            names.append(metric.get("MetricName"))
    return names


def test_create_upload_job_success(api_app):
    client = api_app["client"]
    response = client.post("/jobs", json={"source_type": "upload"})
    assert response.status_code == 200
    data = response.json()
    assert "jobId" in data
    assert data["uploadUrl"].startswith("https://")

    job_id = data["jobId"]
    job_response = client.get(f"/jobs/{job_id}")
    assert job_response.status_code == 200
    job_payload = job_response.json()
    assert job_payload["status"] == "QUEUED"

    # Step Functions should have been triggered exactly once
    assert len(api_app["sfn"].executions) == 1
    assert api_app["sfn"].executions[0]["name"].startswith("job-")

    # Metrics should reflect validation + success path
    names = metric_names(api_app["cloudwatch"].metric_calls)
    assert "JobCreated" in names
    assert "JobQueued" in names


def test_create_job_validation_error(api_app):
    client = api_app["client"]
    response = client.post("/jobs", json={"source_type": "invalid"})
    assert response.status_code == 400
    assert response.json()["detail"] == "Unsupported source_type"
    names = metric_names(api_app["cloudwatch"].metric_calls)
    assert "JobValidationError" in names


def test_create_http_job(api_app):
    client = api_app["client"]
    module = api_app["module"]
    payload = {
        "source_type": "https",
        "source_config": {"url": "https://example.com/data.csv", "headers": {"Authorization": "Bearer token"}},
    }
    response = client.post("/jobs", json=payload)
    assert response.status_code == 200
    body = response.json()
    assert body.get("uploadUrl") is None

    job_id = body["jobId"]
    record = module.table.get_item({"pk": f"job#{job_id}", "sk": "meta"}).get("Item")
    assert record is not None
    assert record["source"]["type"] == "http"
    assert record["source"]["protocol"] == "https"
    assert record["source"]["url"] == "https://example.com/data.csv"


def test_create_database_job_requires_config(api_app):
    client = api_app["client"]
    response = client.post("/jobs", json={"source_type": "database"})
    assert response.status_code == 400
    assert "database jobs require query" in response.json()["detail"]

    response = client.post(
        "/jobs",
        json={
            "source_type": "database",
            "source_config": {"query": "SELECT 1"},
        },
    )
    assert response.status_code == 400
    assert "connection reference" in response.json()["detail"]

    response = client.post(
        "/jobs",
        json={
            "source_type": "database",
            "source_config": {"url": "sqlite:///tmp/example.db", "query": "SELECT 1", "format": "jsonl"},
        },
    )
    assert response.status_code == 200
    job_id = response.json()["jobId"]
    record = api_app["module"].table.get_item({"pk": f"job#{job_id}", "sk": "meta"}).get("Item")
    assert record["source"]["format"] == "jsonl"
    assert record["source"]["connection"]["type"] == "inline"
    assert record["source"]["connection"]["url"] == "sqlite:///tmp/example.db"


def test_create_database_job_with_secret(api_app):
    client = api_app["client"]
    response = client.post(
        "/jobs",
        json={
            "source_type": "database",
            "source_config": {
                "query": "SELECT 1",
                "secretArn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:database",
                "secretField": "url",
            },
        },
    )
    assert response.status_code == 200
    job_id = response.json()["jobId"]
    record = api_app["module"].table.get_item({"pk": f"job#{job_id}", "sk": "meta"}).get("Item")
    assert record["source"]["connection"]["type"] == "secretsManager"
    assert record["source"]["connection"]["secretArn"].endswith(":database")
    assert record["source"]["connection"]["secretField"] == "url"


def test_create_database_job_rejects_multiple_connections(api_app):
    client = api_app["client"]
    response = client.post(
        "/jobs",
        json={
            "source_type": "database",
            "source_config": {
                "url": "sqlite:///tmp/example.db",
                "secretArn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:database",
                "query": "SELECT 1",
            },
        },
    )
    assert response.status_code == 400
    assert "only one connection" in response.json()["detail"]


def test_create_warehouse_job_validation(api_app):
    client = api_app["client"]
    response = client.post(
        "/jobs",
        json={
            "source_type": "warehouse",
            "source_config": {"url": "sqlite:///tmp/example.db", "query": "SELECT 1"},
        },
    )
    assert response.status_code == 400
    assert "warehouseType" in response.json()["detail"]

    response = client.post(
        "/jobs",
        json={
            "source_type": "warehouse",
            "source_config": {
                "warehouseType": "snowflake",
                "query": "SELECT 1",
                "secretArn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:warehouse",
                "filename": "warehouse-output.csv",
            },
        },
    )
    assert response.status_code == 200
    record = api_app["module"].table.get_item({"pk": f"job#{response.json()['jobId']}", "sk": "meta"}).get("Item")
    assert record["source"]["warehouseType"] == "snowflake"
    assert record["source"]["filename"] == "warehouse-output.csv"
    assert record["source"]["connection"]["type"] == "secretsManager"
    assert record["source"]["connection"]["secretArn"].endswith(":warehouse")


@pytest.mark.parametrize("path,expected_status", [(None, 200), ("data.csv", 200), ("missing.csv", 404)])
def test_manifest_and_results_browsing(api_app, path, expected_status):
    client = api_app["client"]
    module = api_app["module"]

    create_resp = client.post("/jobs", json={"source_type": "upload"})
    assert create_resp.status_code == 200
    job_id = create_resp.json()["jobId"]

    manifest = {"inputs": ["upload.csv"], "results": ["results.json"]}
    module.s3.put_object(
        Bucket=module.BUCKET_NAME,
        Key=f"artifacts/{job_id}/manifest.json",
        Body=json.dumps(manifest),
    )
    module.s3.put_object(
        Bucket=module.BUCKET_NAME,
        Key=f"artifacts/{job_id}/results/results.json",
        Body=json.dumps({"rowCount": 1}),
    )
    module.s3.put_object(
        Bucket=module.BUCKET_NAME,
        Key=f"artifacts/{job_id}/results/data.csv",
        Body="value\n1\n",
    )
    module.s3.put_object(
        Bucket=module.BUCKET_NAME,
        Key=f"artifacts/{job_id}/results/subdir/details.json",
        Body=json.dumps({"score": 10}),
    )

    manifest_resp = client.get(f"/jobs/{job_id}/manifest")
    assert manifest_resp.status_code == 200
    assert manifest_resp.json()["manifest"] == manifest

    artifacts_resp = client.get(f"/jobs/{job_id}/artifacts")
    assert artifacts_resp.status_code == 200
    listed_keys = {obj["key"] for obj in artifacts_resp.json()["objects"]}
    assert f"artifacts/{job_id}/manifest.json" in listed_keys
    common_prefixes = set(artifacts_resp.json().get("commonPrefixes", []))
    assert f"artifacts/{job_id}/results/" in common_prefixes

    filtered_resp = client.get(
        f"/jobs/{job_id}/artifacts",
        params={"prefix": f"artifacts/{job_id}/results/"},
    )
    assert filtered_resp.status_code == 200
    filtered_keys = {obj["key"] for obj in filtered_resp.json()["objects"]}
    assert all(key.startswith(f"artifacts/{job_id}/results/") for key in filtered_keys)

    files_resp = client.get(f"/jobs/{job_id}/results/files")
    assert files_resp.status_code == 200
    file_keys = {obj["key"] for obj in files_resp.json()["objects"]}
    assert f"artifacts/{job_id}/results/data.csv" in file_keys
    assert f"artifacts/{job_id}/results/subdir/details.json" not in file_keys
    assert f"artifacts/{job_id}/results/subdir/" in files_resp.json()["commonPrefixes"]

    params = {"path": path} if path else {}
    download_resp = client.get(f"/jobs/{job_id}/results", params=params)
    assert download_resp.status_code == expected_status
    if expected_status == 200:
        payload = download_resp.json()
        assert payload["key"].endswith(path or "results.json")
        assert payload["downloadUrl"].startswith("https://")


def test_results_listing_without_files(api_app):
    client = api_app["client"]
    create_resp = client.post("/jobs", json={"source_type": "upload"})
    job_id = create_resp.json()["jobId"]

    files_resp = client.get(f"/jobs/{job_id}/results/files")
    assert files_resp.status_code == 404

    manifest_resp = client.get(f"/jobs/{job_id}/manifest")
    assert manifest_resp.status_code == 404


def test_artifact_prefix_outside_job_is_rejected(api_app):
    client = api_app["client"]
    create_resp = client.post("/jobs", json={"source_type": "upload"})
    job_id = create_resp.json()["jobId"]

    resp = client.get(f"/jobs/{job_id}/artifacts", params={"prefix": "other/"})
    assert resp.status_code == 400

    resp = client.get(
        f"/jobs/{job_id}/artifacts",
        params={"prefix": f"artifacts/{job_id}/../other"},
    )
    assert resp.status_code == 400


def test_results_path_outside_job_is_rejected(api_app):
    client = api_app["client"]
    create_resp = client.post("/jobs", json={"source_type": "upload"})
    job_id = create_resp.json()["jobId"]

    resp = client.get(
        f"/jobs/{job_id}/results",
        params={"path": "../manifest.json"},
    )
    assert resp.status_code == 400
