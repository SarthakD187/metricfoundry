# services/api/app.py
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="MetricFoundry API")

class CreateJob(BaseModel):
    source_type: str  # "upload" | "s3"
    s3_path: str | None = None

@app.post("/jobs")
def create_job(body: CreateJob):
    # TODO: write job record, return presigned URL if upload
    return {"jobId": "stub-job-id", "uploadUrl": None}
