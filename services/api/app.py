from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import boto3, os, uuid, time

BUCKET = os.environ["BUCKET_NAME"]
TABLE  = os.environ["TABLE_NAME"]
QUEUE  = os.environ["QUEUE_URL"]

s3 = boto3.client("s3")
ddb = boto3.client("dynamodb")
sqs = boto3.client("sqs")

app = FastAPI(title="MetricFoundry API")

class CreateJob(BaseModel):
    source_type: str            # "upload" | "s3"
    s3_path: str | None = None  # required if source_type == "s3"

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/jobs")
def create_job(body: CreateJob):
    if body.source_type not in ("upload","s3"):
        raise HTTPException(400, "source_type must be 'upload' or 's3'")
    job_id = str(uuid.uuid4())
    now = int(time.time())

    # Where the raw file will live
    if body.source_type == "upload":
        key = f"jobs/{job_id}/raw/upload.csv"
        upload_url = s3.generate_presigned_url(
            ClientMethod="put_object",
            Params={"Bucket": BUCKET, "Key": key, "ContentType": "text/csv"},
            ExpiresIn=900,  # 15 min
        )
        source = {"type": "upload", "bucket": BUCKET, "key": key}
    else:
        if not body.s3_path or not body.s3_path.startswith("s3://"):
            raise HTTPException(400, "s3_path must be like s3://bucket/key")
        source = {"type": "s3", "uri": body.s3_path}
        upload_url = None

    # Create initial DDB items (job + first step)
    ddb.put_item(
        TableName=TABLE,
        Item={
            "pk": {"S": f"JOB#{job_id}"},
            "sk": {"S": "META"},
            "status": {"S": "CREATED"},
            "createdAt": {"N": str(now)},
            "source": {"S": str(source)},
        },
        ConditionExpression="attribute_not_exists(pk)"
    )
    ddb.put_item(
        TableName=TABLE,
        Item={
            "pk": {"S": f"JOB#{job_id}"},
            "sk": {"S": "STEP#ingest"},
            "status": {"S": "PENDING"},
            "attempt": {"N": "0"},
            "createdAt": {"N": str(now)}
        }
    )

    # Enqueue first step
    sqs.send_message(
        QueueUrl=QUEUE,
        MessageBody=f'{{"jobId":"{job_id}","step":"ingest"}}'
    )

    return {"jobId": job_id, "uploadUrl": upload_url}
