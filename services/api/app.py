# services/api/app.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import boto3, os, uuid, time
from botocore.exceptions import ClientError
from mangum import Mangum

# ---- Env ----
BUCKET_NAME = os.environ["BUCKET_NAME"]          # artifacts bucket
TABLE_NAME  = os.environ["TABLE_NAME"]           # DynamoDB table
QUEUE_URL   = os.environ.get("QUEUE_URL")        # optional

# ---- AWS ----
s3  = boto3.client("s3")
ddb = boto3.resource("dynamodb")
table = ddb.Table(TABLE_NAME)

app = FastAPI(title="MetricFoundry API")

# ---- Models ----
class CreateJob(BaseModel):
    source_type: str            # "upload" | "s3"
    s3_path: str | None = None  # required if source_type == "s3"

# ---- Helpers ----
def epoch() -> int:
    return int(time.time())

def ddb_put_job(job_id: str, status: str, source: dict, created: int):
    item = {
        "pk": f"job#{job_id}",
        "sk": "meta",
        "status": status,      # CREATED -> (worker sets RUNNING/SUCCEEDED/FAILED)
        "createdAt": created,
        "updatedAt": created,
        "source": source,
    }
    table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)"
    )
    return item

# ---- Routes ----
@app.get("/health")
def health():
    return {"ok": True}

@app.post("/jobs")
def create_job(body: CreateJob):
    if body.source_type not in ("upload", "s3"):
        raise HTTPException(status_code=400, detail="source_type must be 'upload' or 's3'")

    job_id = str(uuid.uuid4())
    now = epoch()

    if body.source_type == "upload":
        # Unified with worker: artifacts/<jobId>/input/...
        key = f"artifacts/{job_id}/input/upload.csv"
        upload_url = s3.generate_presigned_url(
            ClientMethod="put_object",
            Params={"Bucket": BUCKET_NAME, "Key": key, "ContentType": "text/csv"},
            ExpiresIn=900,
        )
        source = {"type": "upload", "bucket": BUCKET_NAME, "key": key}
    else:
        if not body.s3_path or not body.s3_path.startswith("s3://"):
            raise HTTPException(status_code=400, detail="s3_path must be like s3://bucket/key")
        upload_url = None
        source = {"type": "s3", "uri": body.s3_path}

    ddb_put_job(job_id, status="CREATED", source=source, created=now)
    return {"jobId": job_id, "uploadUrl": upload_url}

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    res = table.get_item(Key={"pk": f"job#{job_id}", "sk": "meta"})
    item = res.get("Item")
    if not item:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "jobId": job_id,
        "status": item.get("status"),
        "createdAt": item.get("createdAt"),
        "updatedAt": item.get("updatedAt"),
        "resultKey": item.get("resultKey"),
        "source": item.get("source"),
    }

@app.get("/jobs/{job_id}/results")
def get_job_results(job_id: str):
    key = f"artifacts/{job_id}/results/results.json"
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NotFound", "NoSuchKey"):
            raise HTTPException(status_code=404, detail="Results not available")
        raise HTTPException(status_code=502, detail="Unable to verify results availability")
    url = s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": BUCKET_NAME, "Key": key},
        ExpiresIn=600,
    )
    return {"jobId": job_id, "downloadUrl": url}

# ✅ GLOBAL Lambda handler (must be at module scope)
handler = Mangum(app)
