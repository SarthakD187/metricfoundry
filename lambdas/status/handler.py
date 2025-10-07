import os
import time
from typing import Any, Dict

import boto3

TABLE_NAME = os.environ["JOBS_TABLE"]
ddb = boto3.resource("dynamodb")

def _table():
    return ddb.Table(TABLE_NAME)


def handler(event: Dict[str, Any], _context):
    job_id = event.get("jobId")
    status = event.get("status", "FAILED")
    error = event.get("error")

    if not job_id:
        raise ValueError("jobId is required")

    expr_names = {"#s": "status"}
    expr_vals = {":s": status, ":u": int(time.time())}
    update_parts = ["#s = :s", "updatedAt = :u"]

    if error:
        expr_vals[":e"] = str(error)[:1000]
        update_parts.append("error = :e")

    _table().update_item(
        Key={"pk": f"job#{job_id}", "sk": "meta"},
        UpdateExpression="SET " + ", ".join(update_parts),
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_vals,
    )

    return {"jobId": job_id, "status": status}
