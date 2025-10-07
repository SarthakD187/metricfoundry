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
