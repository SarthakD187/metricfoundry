import os
import logging
import time
from typing import Any, Dict, Mapping

import boto3
from botocore.exceptions import BotoCoreError, ClientError

TABLE_NAME = os.environ["JOBS_TABLE"]
ddb = boto3.resource("dynamodb")
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


def _table():
    return ddb.Table(TABLE_NAME)


def handler(event: Mapping[str, Any], _context: Any) -> Dict[str, str]:
    """Persist final workflow status for a job in DynamoDB."""
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
        expr_names["#err"] = "error"
        update_parts.append("#err = :e")

    try:
        _table().update_item(
            Key={"pk": f"job#{job_id}", "sk": "meta"},
            UpdateExpression="SET " + ", ".join(update_parts),
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_vals,
        )
    except (BotoCoreError, ClientError) as exc:
        logger.error("failed status update for job %s: %s", job_id, exc)
        raise

    return {"jobId": job_id, "status": status}


def lambda_handler(event: Mapping[str, Any], context: Any) -> Dict[str, Any]:
    """API-shaped wrapper for direct Lambda invocation."""
    try:
        return {"statusCode": 200, "body": handler(event, context)}
    except ValueError as exc:
        return {"statusCode": 400, "body": {"error": str(exc)}}
    except (BotoCoreError, ClientError):
        return {"statusCode": 502, "body": {"error": "Failed to update job status"}}
