# MetricFoundry

MetricFoundry is a production-ready, serverless data analytics platform built on AWS that automates data profiling, quality assessment, statistical analysis, and machine learning inference. The system combines LangGraph orchestration, AWS Step Functions, and a comprehensive FastAPI backend to process diverse dataset formats and generate actionable insights with minimal user intervention.

## Overview

The platform is designed for data teams who need automated analytics pipelines without managing infrastructure. Jobs are submitted via API or web dashboard, processed through a seven-phase LangGraph pipeline, and results are persisted to S3 with real-time progress tracking in DynamoDB. The system supports multi-tenant isolation, Cognito-based authentication, and scales automatically within configured Lambda memory and timeout budgets.

## Architecture

MetricFoundry consists of four primary components:

1. **FastAPI Service** - HTTP API for job management, artifact browsing, and presigned URL generation
2. **LangGraph Analytics Pipeline** - Seven-phase workflow executing data profiling, quality checks, statistical analysis, ML inference, and report generation
3. **AWS Step Functions Orchestration** - State machine coordinating staging and processing workflows with automatic retry logic
4. **Next.js Dashboard** - Browser-based interface for drag-and-drop uploads, real-time monitoring, and result visualization

The infrastructure is deployed via AWS CDK and includes S3 for artifact storage, DynamoDB for job state persistence, Lambda functions for compute, and API Gateway for HTTP endpoints.

## Supported Data Formats

The ingestion layer handles the following formats natively:

**Structured Data**:
- Delimited text files (CSV, TSV) with automatic delimiter detection
- JSON arrays and JSON Lines (JSONL)
- Excel workbooks (`.xls`, `.xlsx`, `.xlsm`) via pandas and openpyxl
- Apache Parquet via pyarrow
- SQLite database exports with automatic base64 encoding for binary columns

**Archive Formats**:
- GZIP (`.gz`)
- ZIP (`.zip`) with intelligent member selection based on preferred file extensions
- TAR and compressed TAR variants (`.tar`, `.tar.gz`, `.tar.bz2`)

All archives are processed with streaming extraction to minimize memory footprint. Nested archives are expanded recursively, and the pipeline selects the first recognized data format for processing.

**Null Handling**: The pipeline normalizes common null sentinels including empty strings, `"null"`, `"NULL"`, and `"NaN"` to ensure data quality metrics accurately reflect true completeness rather than placeholder counts.

**Format Limitations**: Proprietary binary formats, password-protected archives, and multi-terabyte datasets are out of scope. Failed parsing results in a `FAILED` job state with detailed error artifacts for debugging.

## Data Source Connectors

Jobs can ingest data from multiple source types configured via the `source` field in job creation requests:

**S3 Sources** (`type: "s3"`):
- Direct `s3://bucket/key` references for datasets already in S3
- Presigned URL uploads for browser-based file submission

**HTTP/HTTPS Sources** (`type: "http"` or `type: "https"`):
- Public HTTP endpoints with SSRF protection via DNS validation
- Configurable request timeout and content-length limits
- Automatic format detection (CSV, TSV, JSON)
- Streaming downloads constrained by `MAX_HTTP_BYTES` environment variable

**Database Sources** (`type: "database"`):
- SQL query execution via SQLAlchemy with parameterization support
- Connection configuration via inline URLs, AWS Secrets Manager, or Systems Manager Parameter Store
- Supported engines include PostgreSQL, MySQL, and SQL Server

**Data Warehouse Sources**:
- Amazon Redshift (`type: "redshift"`)
- Snowflake (`type: "snowflake"`)
- Google BigQuery (`type: "bigquery"`)
- Databricks (`type: "databricks"`)

Warehouse connectors use SQLAlchemy for query execution and support the same credential management options as database sources. Native warehouse APIs and bulk export optimizations are not yet implemented.

**Upload Sources** (`type: "upload"`):
- Generates presigned S3 URLs for direct browser uploads
- Optional `Content-Type` upload headers for proper MIME type handling
- Default upload expiration of 1 hour

## Analytics Pipeline

The LangGraph analytics pipeline executes seven sequential phases once a job transitions to the `RUNNING` state. In production deployments, the pipeline is invoked automatically by AWS Step Functions after successful staging. The `POST /jobs/{jobId}/process` endpoint is available for local development but is not used in production orchestration.

### Pipeline Phases

The pipeline executes phases in the following order, persisting artifacts and updating job state in DynamoDB after each phase completes:

1. **Ingest** - Loads the staged dataset from S3, detects format, expands archives, normalizes structure into tabular form
2. **Profile** - Infers schema, classifies column types (numeric, categorical, temporal), computes cardinality and null counts
3. **Data Quality** - Validates completeness, detects duplicates, identifies anomalous values, scores overall data quality
4. **Descriptive Statistics** - Computes mean, median, standard deviation, quantiles, min/max for numeric columns; generates frequency distributions for categorical columns
5. **ML Inference** - Automatically selects and trains scikit-learn models (classification, regression, or clustering) based on schema characteristics; evaluates model performance and extracts feature importance
6. **Report** - Synthesizes natural language summaries of findings, recommendations for data quality improvements, and insights from statistical analysis
7. **Finalization** - Assembles phase outputs into `results.json`, generates visualizations (histograms, scatter plots, correlation heatmaps, box plots) via matplotlib, publishes artifacts to S3

Each phase update triggers a DynamoDB write with current `phaseIndex`, `currentPhase`, and `progress` percentage, enabling real-time monitoring via dashboard and API clients.

### Pipeline Outputs

The completed pipeline produces the following artifacts under `artifacts/{jobId}/`:

- `results.json` - Consolidated analytics report containing all phase outputs
- `manifest.json` - Dataset metadata including row/column counts, inferred schema, detected format
- `phases/{phase_name}.json` - Individual phase results for incremental consumption
- `visualizations/*.png` - Rendered charts and plots

**Statistical Outputs**:
- Descriptive statistics (mean, std, min, max, quartiles) for all numeric columns
- Pearson correlation coefficients for numeric column pairs
- Outlier detection using configurable z-score thresholds (default: 3.0)
- Duplicate row identification with exact match criteria
- Completeness metrics excluding recognized null sentinels

**Machine Learning Outputs**:
- Trained model artifacts (if applicable)
- Model performance metrics (accuracy, F1, RMSE, silhouette score depending on task type)
- Feature importance rankings
- Prediction samples for validation

### Data Quality and Error Handling

The pipeline is designed to handle messy real-world data gracefully:

- **Schema Inference**: Zero-only columns and sparsely populated fields default to categorical type classification
- **Null Handling**: Completeness metrics exclude empty strings, `"null"`, `"NULL"`, and `"NaN"` tokens
- **Numerical Stability**: Quantile and standard deviation calculations use Welford's online algorithm
- **Memory Management**: Streaming processing with 8 MB chunk size limits peak memory usage
- **Size Constraints**: `MAX_PIPELINE_BODY_BYTES` environment variable (default 512 MiB) prevents out-of-memory crashes in Lambda
- **Timeout Handling**: Processor Lambda configured with 15-minute timeout and 10 GB ephemeral storage for large dataset processing
- **Failure Transparency**: Parse errors, validation failures, and runtime exceptions result in `FAILED` job state with detailed error artifacts

When processing exceeds memory or timeout budgets, the job fails gracefully with error details persisted to `artifacts/{jobId}/error.json` for troubleshooting.

## API Endpoints

The FastAPI service provides a RESTful HTTP API for job lifecycle management, artifact retrieval, and system introspection. All endpoints require authentication via Cognito-issued JWT tokens in the `Authorization` header (except health checks).

### Job Management

**`POST /jobs`** - Create a new job
- Accepts source configuration (upload, S3, HTTP, database, warehouse)
- Validates source parameters and permissions
- Generates presigned S3 URLs for upload-type sources
- Returns job ID, status, and upload credentials (if applicable)
- Automatically triggers staging workflow via Step Functions

**`GET /jobs/{jobId}`** - Retrieve job status
- Returns job state (`CREATED`, `QUEUED`, `RUNNING`, `STAGED`, `SUCCEEDED`, `FAILED`)
- Includes timestamps (created, updated), current phase, progress percentage
- Provides source metadata, artifact prefix, and error details (if failed)
- Supports tenant isolation via JWT claims

**`POST /jobs/{jobId}/process`** - Trigger analytics pipeline (local development only)
- Executes LangGraph pipeline synchronously
- Returns phase outputs and final results
- Not used in production deployments (Step Functions orchestration instead)

### Artifact Access

**`GET /jobs/{jobId}/manifest`** - Fetch ingestion manifest
- Returns parsed dataset metadata (row count, column count, inferred schema)
- Includes detected format, null counts, and cardinality statistics

**`GET /jobs/{jobId}/artifacts`** - List job artifacts
- Enumerates all S3 objects under `artifacts/{jobId}/` prefix
- Supports pagination via `continuationToken` query parameter
- Optional `prefix` filter for hierarchical artifact browsing

**`GET /jobs/{jobId}/results/files`** - List result artifacts
- Returns files under `artifacts/{jobId}/results/` (visualizations, reports, models)
- Includes object metadata (size, last modified, ETag)

**`GET /jobs/{jobId}/results`** - Download result file
- Generates presigned S3 download URL for specified result file
- Defaults to `results.json` if no file path provided
- URL valid for 1 hour

### System Endpoints

**`GET /health`** - Health check (no authentication required)
- Returns service status and version information

**`GET /`** - Root endpoint
- Returns API metadata and available routes

### Authentication and Authorization

All job-related endpoints enforce authentication via AWS Cognito:

- **JWT Validation**: API Gateway's `HttpUserPoolAuthorizer` validates tokens before request forwarding
- **Tenant Isolation**: Jobs are scoped to tenant ID extracted from JWT claims (`custom:tenant`, `custom:tenantId`, `tenant`, or `tenantId`)
- **Local Development**: Unverified JWT decoding available when API runs outside API Gateway (logs warning)

The dashboard uses AWS Amplify v6 to handle OAuth 2.0 authorization code flow with Cognito hosted UI.

### Observability

Each API request includes structured logging and CloudWatch metrics:

- **Request Tracking**: Unique request IDs propagated across Lambda invocations
- **Metrics Emitted**:
  - Job submission counts (successful, failed)
  - Validation failure counts by error type
  - Workflow launch counts
  - Latency percentiles (p50, p95, p99)
- **Log Context**: User ID, tenant ID, job ID, source type included in all log entries
- **Error Details**: Failed requests include stack traces, input validation errors, and AWS service errors in CloudWatch Logs

## Web Dashboard

The Next.js dashboard in [`dashboard/`](dashboard/) provides a browser-based control plane for non-technical users and operations teams. The application is built with React 18, Next.js 14, and Tailwind CSS, and integrates with Cognito for authentication.

### Features

**Job Creation**:
- Drag-and-drop file upload with presigned S3 URL generation
- S3 source configuration for datasets already in your buckets
- HTTP/HTTPS URL ingestion with validation
- Database and warehouse query configuration

**Job Monitoring**:
- Real-time status polling with automatic refresh
- Progress bar showing current phase and completion percentage
- Live phase transition notifications
- Job history in local storage for quick access to recent runs

**Artifact Management**:
- Manifest inspection with schema and metadata display
- Hierarchical artifact browser with S3 prefix filtering
- One-click signed downloads for result files
- Inline preview of `results.json` with formatted JSON

**Authentication**:
- OAuth 2.0 authorization code flow via Cognito hosted UI
- Automatic token refresh
- Tenant-scoped job isolation based on JWT claims
- Sign-in, sign-out, and session management

### Local Development

Run the dashboard against a local or deployed API:

```bash
cd dashboard
npm install
NEXT_PUBLIC_API_BASE_URL="http://localhost:8000" npm run dev
```

Alternatively, use the root-level helper scripts:

```bash
npm run dashboard:install
NEXT_PUBLIC_API_BASE_URL="http://localhost:8000" npm run dashboard:dev
```

The dashboard defaults to `http://localhost:8000` for API requests. Override with `NEXT_PUBLIC_API_BASE_URL` to point at deployed infrastructure.

**Local Storage**: Recently viewed job IDs persist in browser local storage to enable quick navigation without re-querying the API.

### Production Deployment

Production deployments require environment variables for Cognito integration and API configuration. Set these when building or hosting the dashboard:

| Variable | Required | Description |
| --- | --- | --- |
| `NEXT_PUBLIC_COGNITO_USER_POOL_ID` | Yes | User pool ID from `AuthStack` CDK output |
| `NEXT_PUBLIC_COGNITO_USER_POOL_CLIENT_ID` | Yes | App client ID (must match API authorizer) |
| `NEXT_PUBLIC_COGNITO_DOMAIN` | Yes | Hosted UI domain (e.g., `prefix.auth.us-east-1.amazoncognito.com`) |
| `NEXT_PUBLIC_AWS_REGION` | Yes | AWS region for Cognito and API (e.g., `us-east-1`) |
| `NEXT_PUBLIC_API_BASE_URL` | Yes | API Gateway HTTPS URL from `ApiStack` output |
| `NEXT_PUBLIC_OAUTH_REDIRECT_SIGN_IN` | Yes | Sign-in redirect URI (must be registered in user pool client) |
| `NEXT_PUBLIC_OAUTH_REDIRECT_SIGN_OUT` | Yes | Sign-out redirect URI (must be registered in user pool client) |
| `NEXT_PUBLIC_ALLOW_ID_TOKEN_AS_BEARER` | No | Set to `true` for local dev to accept ID tokens as Bearer tokens |

**Critical Configuration Requirements**:
- The dashboard and API must share the same Cognito user pool client to ensure JWT audience claims match
- The `FRONTEND_ORIGIN` environment variable or CDK context entry must be set in `ApiStack` to configure CORS headers
- Redirect URIs must exactly match the registered values in the Cognito user pool client (including protocol, domain, port, and path)

## Infrastructure Deployment

MetricFoundry uses AWS CDK for infrastructure provisioning. The deployment creates three CloudFormation stacks:

1. **CoreStack** - S3 artifacts bucket, DynamoDB jobs table, Lambda functions (Stage, Processor, Status), Step Functions state machine
2. **AuthStack** - Cognito user pool, app client, hosted UI domain
3. **ApiStack** - API Gateway HTTP API, Lambda integrations, HTTP authorizer, CORS configuration

### Prerequisites

- AWS account with programmatic access credentials configured
- AWS CLI v2 installed and configured
- Node.js 18+ and npm
- Python 3.11+ (for Lambda runtime compatibility)
- Docker (for Processor Lambda container build)

### Deployment Steps

Install dependencies and deploy infrastructure:

```bash
npm install
npm run cdk:deploy
```

The CDK will prompt for confirmation before creating resources. Approve the changes to proceed with deployment.

**CDK Outputs**: After successful deployment, the CDK emits output values including:
- API Gateway endpoint URL
- Cognito user pool ID and client ID
- Cognito hosted UI domain
- S3 artifacts bucket name
- DynamoDB table name

Use these outputs to configure the dashboard environment variables.

### Configuration Options

**Memory and Timeout Limits**:
- Stage Lambda: 2048 MB memory, 5 minute timeout
- Processor Lambda: 3008 MB memory, 15 minute timeout, 10 GB ephemeral storage
- Status Lambda: 512 MB memory, 1 minute timeout

**Environment Variables** (set via CDK context or stack parameters):
- `MAX_PIPELINE_BODY_BYTES` - Maximum dataset size for pipeline processing (default: 512 MiB)
- `MAX_HTTP_BYTES` - Maximum download size for HTTP sources (default: 100 MiB)
- `FRONTEND_ORIGIN` - Dashboard URL for CORS configuration (e.g., `https://dashboard.example.com`)

**S3 Lifecycle Policies**:
- Artifacts transition to `INTELLIGENT_TIERING` after 30 days
- Artifacts expire after 180 days
- Versioning enabled with delete markers cleaned up after 90 days

**DynamoDB Configuration**:
- On-demand billing mode for automatic scaling
- Point-in-time recovery enabled
- Primary key: `pk` (partition key), `sk` (sort key)

### Multi-Tenant Isolation

The platform supports tenant isolation via JWT claims:

- Jobs are partitioned by `tenantId` extracted from `custom:tenant`, `custom:tenantId`, `tenant`, or `tenantId` claim
- S3 artifact prefixes include tenant ID: `artifacts/{tenantId}/{ownerId}/{jobId}/`
- DynamoDB queries filter by tenant ID to prevent cross-tenant data access
- IAM policies can be scoped per tenant for additional security boundaries

### Cleanup

Remove all deployed resources:

```bash
cdk destroy --all
```

**Warning**: This permanently deletes all job data, artifacts, and configuration. S3 bucket deletion requires manual confirmation if versioning is enabled.

## Security

MetricFoundry implements defense-in-depth security controls across authentication, authorization, network access, and data handling:

### Authentication and Authorization

- **Cognito User Pool**: OAuth 2.0 and OpenID Connect for user authentication
- **JWT Validation**: API Gateway validates tokens before request processing
- **Tenant Isolation**: Jobs scoped to tenant claims in JWT, preventing cross-tenant access
- **IAM Least Privilege**: Lambda execution roles grant only required S3, DynamoDB, and Secrets Manager permissions
- **Token Expiration**: Configurable token lifetimes with automatic refresh in dashboard

### Network Security

- **SSRF Protection**: HTTP/HTTPS sources validated against private IP ranges and cloud metadata endpoints
- **DNS Validation**: Public DNS resolution required before HTTP fetch
- **CORS Configuration**: API responses include `Access-Control-Allow-Origin` restricted to `FRONTEND_ORIGIN`
- **TLS Enforcement**: S3 bucket policies deny non-SSL requests
- **API Gateway**: All traffic encrypted in transit via HTTPS

### Data Protection

- **S3 Encryption**: Server-side encryption enabled on artifacts bucket
- **S3 Versioning**: Protects against accidental deletion and enables audit trails
- **Public Access Block**: All S3 public access blocked at bucket level
- **Presigned URL Expiration**: Upload and download URLs expire after 1 hour
- **Secrets Management**: Database credentials stored in AWS Secrets Manager or Parameter Store, never in code
- **SQL Injection Prevention**: Parameterized queries via SQLAlchemy, no string interpolation

### Input Validation

- **File Size Limits**: `MAX_PIPELINE_BODY_BYTES` and `MAX_HTTP_BYTES` prevent resource exhaustion
- **Source Validation**: S3 paths, HTTP URLs, and SQL connection strings validated before use
- **Archive Bomb Protection**: Streaming extraction with memory limits prevents decompression attacks
- **Schema Validation**: FastAPI Pydantic models enforce request structure and type safety

### Audit and Compliance

- **CloudWatch Logs**: All API requests, Lambda invocations, and job state transitions logged
- **Request IDs**: Unique identifiers enable request tracing across distributed components
- **DynamoDB Point-in-Time Recovery**: Enables restoration to any point within 35 days
- **S3 Lifecycle Policies**: Automatic expiration ensures compliance with data retention policies

## Testing

Run the test suite to validate API functionality:

```bash
# Install test dependencies
pip install pytest pytest-cov httpx

# Run API tests
npm run test
```

The test suite includes:
- Unit tests for job creation, status retrieval, and artifact management endpoints
- Integration tests for DynamoDB job persistence and S3 presigned URL generation
- Mock tests for Step Functions workflow invocation
- Authentication middleware validation

Test coverage reports are generated in the `htmlcov/` directory.

## Technical Architecture

### Job Lifecycle

1. **Job Creation** (`POST /jobs`):
   - API validates source configuration
   - DynamoDB record created with `CREATED` state
   - Step Functions execution started with job ID
   - Presigned URLs generated for upload sources

2. **Staging** (Step Functions → Stage Lambda):
   - Upload sources wait for file upload with retry polling
   - S3 sources validate bucket access permissions
   - HTTP sources download and stream to S3
   - Database sources execute query and export to S3
   - Job state transitions to `STAGED` on success

3. **Processing** (Step Functions → Processor Lambda):
   - LangGraph pipeline invoked with staged S3 key
   - Each phase updates DynamoDB with progress percentage
   - Artifacts written to S3 under `artifacts/{jobId}/phases/`
   - Job state transitions to `RUNNING` → `SUCCEEDED` or `FAILED`

4. **Result Retrieval** (`GET /jobs/{jobId}/results`):
   - API generates presigned download URL for `results.json`
   - Dashboard fetches and displays formatted results
   - Users download visualizations and reports via browser

### State Machine Flow

The Step Functions state machine orchestrates staging and processing:

```
Start → Stage (Lambda)
  ↓ (on success after retries)
  → Process (Lambda)
    ↓ (on success)
    → RecordSuccess (Status Lambda)
  ↓ (on failure)
  → RecordFailure (Status Lambda)
```

**Retry Configuration**:
- Upload staging: 15 attempts, 20-second intervals, 1.5x backoff multiplier
- Processing: No automatic retries (jobs remain in `FAILED` state for manual investigation)

### Data Flow

1. User uploads dataset → S3 artifacts bucket (presigned URL)
2. Stage Lambda validates and catalogs dataset → DynamoDB manifest
3. Processor Lambda streams dataset from S3 → LangGraph pipeline
4. Pipeline phases write intermediate results → S3 artifacts
5. Final results assembled → `results.json` in S3
6. Dashboard polls job status → DynamoDB → displays progress
7. User downloads results → Presigned S3 URL → Browser

### Technology Stack

**Backend**:
- Python 3.11 (Lambda runtime)
- FastAPI 0.104+ (HTTP API framework)
- LangGraph (pipeline orchestration)
- pandas, openpyxl, pyarrow (data format parsing)
- scikit-learn (machine learning inference)
- matplotlib (visualization generation)
- SQLAlchemy (database connectivity)
- boto3 (AWS SDK)

**Frontend**:
- Next.js 14.2 (React framework)
- React 18.2 (UI library)
- AWS Amplify 6.3 (Cognito authentication)
- Tailwind CSS (styling)

**Infrastructure**:
- AWS CDK 2.219 (infrastructure as code)
- TypeScript 5.9 (CDK language)
- AWS Lambda (serverless compute)
- API Gateway HTTP API (RESTful endpoints)
- Step Functions (workflow orchestration)
- DynamoDB (NoSQL job state storage)
- S3 (object storage for artifacts)
- Cognito (user authentication)

## Limitations and Known Issues

- **Dataset Size**: Maximum 512 MiB per job (configurable via `MAX_PIPELINE_BODY_BYTES`)
- **Processing Timeout**: 15-minute Lambda timeout limits long-running analytics
- **Concurrent Jobs**: No global rate limiting (DynamoDB and S3 scale automatically but costs increase)
- **Warehouse Connectors**: SQLAlchemy-based; native APIs (Redshift UNLOAD, Snowflake COPY) not yet implemented
- **POST /jobs/{jobId}/process**: Synchronous endpoint for local development only; production uses Step Functions
- **Binary Formats**: Only SQLite binary columns supported; other binary data types rejected
- **Password-Protected Archives**: Not supported; files must be unencrypted
- **Real-Time Streaming**: Batch processing only; no streaming ingestion from Kinesis, Kafka, etc.

## License

This project is provided as-is without warranty. Refer to LICENSE file for terms.
