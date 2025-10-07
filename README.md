# MetricFoundry

## Supported dataset ingestion

MetricFoundry's staging Lambda and LangGraph analytics pipeline can now ingest a
wide range of source formats. Upload CSV, TSV, JSON, JSONL, Excel (`.xls`,
`.xlsx`, `.xlsm`), and Parquet files directly. Database extracts produced as
SQLite databases are parsed table-by-table, with binary columns automatically
base64 encoded for compatibility. Common compression formats including GZIP,
ZIP, TAR, and TAR.GZ archives are unpacked on the fly so that nested datasets
are normalised without additional user effort. These capabilities ensure the
platform accepts virtually any structured dataset for downstream analytics.

### Connector landscape

Jobs are no longer limited to ad-hoc uploads or pre-existing S3 objects. The
ingestion API accepts rich connector metadata via the `source_config` request
field, enabling teams to declaratively pull data from almost any system:

* **Upload:** generate a presigned URL for direct browser uploads into the
  artifacts bucket.
* **S3:** hydrate jobs from objects that already exist in S3 via their URI.
* **HTTP/S endpoints:** stream payloads from REST endpoints with custom methods,
  headers, and request bodies before staging them in S3.
* **Databases:** execute SQL queries against JDBC/SQLAlchemy compatible
  databases (including SQLite for local extracts) and persist the result set as
  CSV or JSONL.
* **Warehouses:** target Snowflake, Redshift, BigQuery, or Databricks using the
  same SQL workflow, producing staged artifacts tagged with warehouse metadata
  for downstream observability.

This connector catalogue underpins the platform's "any dataset" promise while
keeping the job orchestration API consistent for every source type.

## Scalability & workflow resilience

Large ingestion jobs now benefit from more generous Lambda resource profiles:
the staging function has a 10-minute timeout with 2 GB of memory, while the
analytics processor runs for up to 15 minutes with 4 GB available. These
headroom increases allow bigger files and more complex enrichment logic to
complete without falling back to retries.

## API hardening & observability

The FastAPI service that fronts the ingestion workflow now exposes endpoints
for rich status introspection and artifact browsing:

* `GET /jobs/{jobId}` – returns job status, timestamps, result key metadata and
  source information.
* `GET /jobs/{jobId}/manifest` – fetches the manifest JSON captured during
  ingestion for quick inspection.
* `GET /jobs/{jobId}/artifacts` – lists any objects stored under the job's
  artifact prefix with optional pagination and hierarchical filtering.
* `GET /jobs/{jobId}/results/files` – enumerates published result files beneath
  `artifacts/{jobId}/results/`.
* `GET /jobs/{jobId}/results` – issues a signed download URL for either the
  default `results.json` or any specific result file path.

Each request is wrapped with structured logging and emits CloudWatch metrics
for successful and failed job submissions, ensuring production visibility into
latency, validation failures, and workflow launches.

### Automated testing

Python unit tests cover the job lifecycle, manifest/result discovery, and API
validation logic. Run them locally with:

```bash
pip install -r services/api/requirements-dev.txt
pytest services/api/tests
```

Additional integration suites exercise the public API and workflow orchestration
end-to-end, covering job creation, Step Functions launches, artifact discovery,
and result downloads:

```bash
pytest tests/integration/test_api_workflow.py
```
