# MetricFoundry

MetricFoundry is a serverless analytics platform on AWS. It stages uploaded/source data to S3, runs a 7-phase LangGraph pipeline, and writes artifacts (including `results.json`) back to S3 while tracking job status in DynamoDB.

## What Is In This Repo

- `services/api/`: FastAPI app (Lambda via Mangum) for job APIs.
- `lambdas/stage/`: Stage Lambda for source resolution and normalization to S3.
- `lambdas/processor/`: Processor Lambda (container) that runs the LangGraph pipeline and persists outputs.
- `lambdas/status/`: Status Lambda for terminal state updates.
- `services/workers/graph/`: LangGraph workflow and phase nodes.
- `services/common/`: Shared pipeline output helpers.
- `infra/`: AWS CDK stacks (`Core`, `Auth`, `Api`).
- `dashboard/`: Next.js dashboard app (uses `pages/`, `components/`, `hooks/`, `lib/`).
- `tests/`: Integration and smoke tests for API/lambdas/pipeline.

## High-Level Architecture

1. `POST /jobs` creates a DynamoDB job record and starts Step Functions.
2. Step Functions runs `StageSourceFn`.
3. `StageSourceFn` resolves the source (`upload`, `s3`, `http/https`, `database`, `warehouse`) and stages data under `artifacts/<owner-or-tenant>/<jobId>/input/`.
4. Step Functions runs `ProcessorFn`.
5. `ProcessorFn` runs the LangGraph phases:
   - `ingest`
   - `profile`
   - `dq_validate`
   - `descriptive_stats`
   - `ml_inference`
   - `nl_report`
   - `finalize`
6. Artifacts are written to S3 (phase JSON files, report/stat artifacts, bundles, and `results/results.json`).
7. Job status is updated in DynamoDB (`QUEUED` → `STAGING`/`STAGED` → `RUNNING` → `SUCCEEDED`/`FAILED`).

## Data Sources and Formats

### Supported `source_type` values for `POST /jobs`

- `upload`
- `s3`
- `http` / `https`
- `database`
- `warehouse`

### Ingestion formats handled by the graph worker

- CSV/TSV
- JSON / JSONL / NDJSON
- Excel (`.xls`, `.xlsx`, `.xlsm`)
- Parquet
- SQLite
- Archives: `.zip`, `.tar`, `.tar.gz`, `.tgz`, `.gz`

## API Endpoints

### Health

- `GET /health`

### Jobs

- `POST /jobs`
- `GET /jobs/{job_id}`
- `POST /jobs/{job_id}/process` (manual in-process processor endpoint in API service; used for development/testing)

### Artifacts

- `GET /jobs/{job_id}/manifest`
- `GET /jobs/{job_id}/artifacts`
- `GET /jobs/{job_id}/results/files`
- `GET /jobs/{job_id}/results`
- `GET /jobs/{job_id}/download`

## Auth Behavior

- API Gateway uses Cognito authorizer for `/jobs` routes in deployed infra.
- API also supports local unverified JWT decoding when `ALLOW_UNVERIFIED_LOCAL_JWT=true`.
- Anonymous job creation can be enabled with `ALLOW_ANONYMOUS_JOB_CREATION=true`.

## Required Environment Variables

### API Lambda (`services/api/app.py`)

- `BUCKET_NAME`
- `TABLE_NAME`
- `STATE_MACHINE_ARN`
- `FRONTEND_ORIGIN` (optional; default `http://localhost:3000`)
- `ALLOW_ANONYMOUS_JOB_CREATION` (optional)
- `ALLOW_UNVERIFIED_LOCAL_JWT` (optional)

### Stage Lambda (`lambdas/stage/handler.py`)

- `JOBS_TABLE`
- `ARTIFACTS_BUCKET`
- `MAX_HTTP_BYTES` (optional)

### Processor Lambda (`lambdas/processor/handler.py`)

- `JOBS_TABLE`
- `ARTIFACTS_BUCKET` (optional fallback to source bucket for outputs)
- `WORKER_INVOKE_MODE` (`embedded`, `lambda`, `http`)
- `WORKER_ARN` (lambda mode)
- `WORKER_URL` (http mode)
- `WORKER_AUTH_HEADER` / `WORKER_BEARER_TOKEN` (optional)
- `MAX_PIPELINE_BODY_BYTES` (worker)

### Status Lambda (`lambdas/status/handler.py`)

- `JOBS_TABLE`

## Deployment (CDK)

From repo root:

```bash
npm install
npm run cdk:deploy
```

Stacks:

- `MetricFoundry-Core`
- `MetricFoundry-Auth`
- `MetricFoundry-Api`

Security controls currently configured in CDK include:

- S3 artifacts bucket with versioning, SSE-S3 encryption, SSL enforcement, and Block Public Access.
- DynamoDB jobs table with point-in-time recovery.
- Cognito User Pool + User Pool authorizer on API routes.

## Dashboard

From `dashboard/`:

```bash
npm install
NEXT_PUBLIC_API_BASE_URL="http://localhost:8000" npm run dev
```

The dashboard currently uses upload-based job creation and job/artifact/result browsing APIs.

## Tests

Primary tests live under `tests/` and `services/api/tests/`.

Typical commands (with pytest installed):

```bash
python3 -m pytest -q tests
python3 -m pytest -q services/api/tests
```

## Notes

- `services/api/` currently includes vendored Python dependencies for packaging.
- The processor Lambda container image is defined in `lambdas/processor/Dockerfile`.
