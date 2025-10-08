"""Lightweight AWS service fakes for integration testing."""
from __future__ import annotations

import hashlib
import io
from datetime import datetime, timezone
from typing import Dict, Iterable, Optional

from botocore.exceptions import ClientError


__all__ = [
    "client_error",
    "FakeS3",
    "FakeDynamoTable",
    "FakeDynamoResource",
    "FakeSecretsManager",
    "FakeSSM",
]


def client_error(code: str, operation: str) -> ClientError:
    """Create a botocore-style ClientError."""

    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


class FakeS3:
    """In-memory subset of the S3 API used by the lambdas."""

    def __init__(self) -> None:
        self._buckets: Dict[str, Dict[str, Dict[str, object]]] = {}

    def _bucket(self, name: str) -> Dict[str, Dict[str, object]]:
        return self._buckets.setdefault(name, {})

    def put_object(
        self,
        Bucket: str,
        Key: str,
        Body,
        ContentType: Optional[str] = None,
    ) -> Dict[str, object]:  # noqa: N803 - mimic boto3 signature
        if isinstance(Body, str):
            payload = Body.encode("utf-8")
        elif hasattr(Body, "read"):
            payload = Body.read()
        else:
            payload = bytes(Body)
        metadata = {
            "Body": payload,
            "ContentLength": len(payload),
            "ContentType": ContentType,
            "LastModified": datetime.now(timezone.utc),
            "ETag": hashlib.md5(payload).hexdigest(),
        }
        self._bucket(Bucket)[Key] = metadata
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def copy_object(
        self,
        Bucket: str,
        Key: str,
        CopySource: Dict[str, str],
    ) -> Dict[str, object]:  # noqa: N803 - mimic boto3 signature
        source_bucket = CopySource["Bucket"]
        source_key = CopySource["Key"]
        source = self._bucket(source_bucket).get(source_key)
        if source is None:
            raise client_error("NoSuchKey", "CopyObject")
        self._bucket(Bucket)[Key] = dict(source)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def head_object(self, Bucket: str, Key: str) -> Dict[str, object]:  # noqa: N803
        bucket = self._bucket(Bucket)
        if Key not in bucket:
            raise client_error("404", "HeadObject")
        metadata = bucket[Key]
        return {
            "ContentLength": metadata["ContentLength"],
            "ContentType": metadata.get("ContentType"),
            "LastModified": metadata["LastModified"],
        }

    def get_object(self, Bucket: str, Key: str) -> Dict[str, object]:  # noqa: N803
        bucket = self._bucket(Bucket)
        if Key not in bucket:
            raise client_error("NoSuchKey", "GetObject")
        metadata = bucket[Key]
        return {
            "Body": io.BytesIO(metadata["Body"]),
            "ContentLength": metadata["ContentLength"],
            "ContentType": metadata.get("ContentType"),
            "LastModified": metadata["LastModified"],
        }

    def list_keys(self, Bucket: str, Prefix: str = "") -> Iterable[str]:  # noqa: N803
        bucket = self._bucket(Bucket)
        for key in sorted(bucket.keys()):
            if key.startswith(Prefix):
                yield key


class FakeDynamoTable:
    """Minimal DynamoDB table supporting put/get/update operations."""

    def __init__(self) -> None:
        self._items: Dict[tuple[str, str], Dict[str, object]] = {}

    def put_item(self, Item: Dict[str, object]) -> Dict[str, object]:  # noqa: N803
        key = (Item["pk"], Item["sk"])
        self._items[key] = dict(Item)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def get_item(self, Key: Dict[str, str]) -> Dict[str, object]:  # noqa: N803
        key = (Key["pk"], Key["sk"])
        item = self._items.get(key)
        return {"Item": dict(item)} if item else {}

    def update_item(
        self,
        Key: Dict[str, str],
        UpdateExpression: str,
        ExpressionAttributeNames: Dict[str, str],
        ExpressionAttributeValues: Dict[str, object],
    ) -> Dict[str, object]:  # noqa: N803
        key = (Key["pk"], Key["sk"])
        if key not in self._items:
            raise client_error("ResourceNotFoundException", "UpdateItem")
        item = dict(self._items[key])
        expression = UpdateExpression.replace("SET", "").strip()
        for part in expression.split(","):
            name_alias, value_alias = [segment.strip() for segment in part.split("=", 1)]
            attribute_name = ExpressionAttributeNames.get(name_alias, name_alias)
            item[attribute_name] = ExpressionAttributeValues[value_alias]
        self._items[key] = item
        return {"Attributes": dict(item)}


class FakeDynamoResource:
    def __init__(self, table: FakeDynamoTable) -> None:
        self._table = table

    def Table(self, _name: str) -> FakeDynamoTable:  # noqa: N802 - mimic boto3
        return self._table


class FakeSecretsManager:
    def __init__(self) -> None:
        self._secrets: Dict[str, str] = {}

    def put_secret(self, arn: str, value: str) -> None:
        self._secrets[arn] = value

    def get_secret_value(self, SecretId: str) -> Dict[str, object]:  # noqa: N803
        if SecretId not in self._secrets:
            raise client_error("ResourceNotFoundException", "GetSecretValue")
        return {"ARN": SecretId, "SecretString": self._secrets[SecretId]}


class FakeSSM:
    def __init__(self) -> None:
        self._parameters: Dict[str, str] = {}

    def put_parameter(self, Name: str, Value: str) -> None:  # noqa: N803
        self._parameters[Name] = Value

    def get_parameter(
        self,
        Name: str,
        WithDecryption: bool = False,
    ) -> Dict[str, object]:  # noqa: N803
        if Name not in self._parameters:
            raise client_error("ParameterNotFound", "GetParameter")
        return {"Parameter": {"Name": Name, "Value": self._parameters[Name]}}
