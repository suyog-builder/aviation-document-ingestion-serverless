"""Document state and idempotency in DynamoDB.

Table ``docingest-state`` (on-demand, PITR on, TTL on ``expires_at``):

    pk                              sk        meaning
    DOC#<doc_id>                    STATE     lifecycle: status, lane, counters, errors
    HASH#<tenant>#<sha256>          DEDUPE    first doc_id that had this exact content

Why DynamoDB conditional writes:
* SQS is at-least-once and S3 can emit duplicate events; ``claim()`` makes the router
  idempotent - the second delivery of the same event is a no-op.
* Partners often re-send the same file under a new name. The content hash item stops
  us from paying for OCR and embeddings twice (~5-10% of partner feed volume in practice).
* ``add_embedded()`` is an atomic counter; the worker that brings ``embedded`` up to
  ``chunks`` flips the document to INDEXED exactly once.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError

from docingest.models import DocStatus, Lane

STATE_TTL_S = 180 * 24 * 3600


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _conditional_failed(exc: ClientError) -> bool:
    return exc.response["Error"]["Code"] == "ConditionalCheckFailedException"


class StateStore:
    def __init__(self, table_name: str, resource: Any | None = None) -> None:
        self._table = (resource or boto3.resource("dynamodb")).Table(table_name)

    # ------------------------------------------------------------------ claim / dedupe
    def claim(self, doc_id: str, tenant_id: str, key: str, size: int) -> bool:
        """Create the state item. False if this doc_id was already claimed (duplicate event)."""
        try:
            self._table.put_item(
                Item={
                    "pk": f"DOC#{doc_id}",
                    "sk": "STATE",
                    "tenant_id": tenant_id,
                    "s3_key": key,
                    "size": size,
                    "status": DocStatus.RECEIVED.value,
                    "attempts": 0,
                    "created_at": _now(),
                    "updated_at": _now(),
                    "expires_at": int(time.time()) + STATE_TTL_S,
                },
                ConditionExpression="attribute_not_exists(pk)",
            )
            return True
        except ClientError as exc:
            if _conditional_failed(exc):
                return False
            raise

    def register_hash(self, tenant_id: str, sha256: str, doc_id: str) -> str | None:
        """Return the doc_id that already owns this content, or None if we are first."""
        try:
            self._table.put_item(
                Item={
                    "pk": f"HASH#{tenant_id}#{sha256}",
                    "sk": "DEDUPE",
                    "doc_id": doc_id,
                    "created_at": _now(),
                    "expires_at": int(time.time()) + STATE_TTL_S,
                },
                ConditionExpression="attribute_not_exists(pk)",
            )
            return None
        except ClientError as exc:
            if not _conditional_failed(exc):
                raise
        item = self._table.get_item(Key={"pk": f"HASH#{tenant_id}#{sha256}", "sk": "DEDUPE"}).get("Item")
        owner = item["doc_id"] if item else None
        return None if owner == doc_id else owner

    # ------------------------------------------------------------------ transitions
    def set_status(self, doc_id: str, status: DocStatus, **fields: Any) -> None:
        names = {"#s": "status"}
        values: dict[str, Any] = {":s": status.value, ":u": _now()}
        sets = ["#s = :s", "updated_at = :u"]
        for i, (k, v) in enumerate(fields.items()):
            names[f"#f{i}"] = k
            values[f":f{i}"] = v.value if isinstance(v, Lane | DocStatus) else v
            sets.append(f"#f{i} = :f{i}")
        self._table.update_item(
            Key={"pk": f"DOC#{doc_id}", "sk": "STATE"},
            UpdateExpression="SET " + ", ".join(sets),
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )

    def set_chunks(self, doc_id: str, total: int) -> None:
        """Record the chunk count and reset the embedded counter (re-processing safe)."""
        self._table.update_item(
            Key={"pk": f"DOC#{doc_id}", "sk": "STATE"},
            UpdateExpression="SET #s = :s, chunks = :n, embedded = :z, updated_at = :u REMOVE batches_done",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": DocStatus.CHUNKED.value, ":n": total, ":z": 0, ":u": _now()},
        )

    def add_embedded(self, doc_id: str, batch_no: int, count: int) -> bool:
        """Count a finished embed batch exactly once. True for the caller that completes the document.

        SQS may deliver the same batch twice; the ``batches_done`` set makes the counter idempotent,
        so a redelivery can never push ``embedded`` past ``chunks`` and mark a document INDEXED early.
        """
        try:
            resp = self._table.update_item(
                Key={"pk": f"DOC#{doc_id}", "sk": "STATE"},
                UpdateExpression="ADD embedded :c, batches_done :b SET updated_at = :u",
                ConditionExpression="attribute_not_exists(batches_done) OR NOT contains(batches_done, :bn)",
                ExpressionAttributeValues={
                    ":c": count,
                    ":b": {str(batch_no)},
                    ":bn": str(batch_no),
                    ":u": _now(),
                },
                ReturnValues="ALL_NEW",
            )
        except ClientError as exc:
            if _conditional_failed(exc):
                return False  # this batch was already counted
            raise
        item = resp["Attributes"]
        if int(item.get("embedded", 0)) < int(item.get("chunks", 1 << 30)):
            return False
        try:
            self._table.update_item(
                Key={"pk": f"DOC#{doc_id}", "sk": "STATE"},
                UpdateExpression="SET #s = :i, indexed_at = :u",
                ConditionExpression="#s <> :i",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":i": DocStatus.INDEXED.value, ":u": _now()},
            )
            return True
        except ClientError as exc:
            if _conditional_failed(exc):
                return False  # another worker already completed it
            raise

    def get(self, doc_id: str) -> dict[str, Any] | None:
        return self._table.get_item(Key={"pk": f"DOC#{doc_id}", "sk": "STATE"}, ConsistentRead=True).get(
            "Item"
        )

    # ------------------------------------------------------------------ Textract job -> task token
    def save_job(self, job_id: str, task_token: str, doc_id: str, part_no: int) -> None:
        """Textract completion arrives on SNS with only the JobId; this maps it back to the waiting
        Step Functions task."""
        self._table.put_item(
            Item={
                "pk": f"JOB#{job_id}",
                "sk": "TOKEN",
                "task_token": task_token,
                "doc_id": doc_id,
                "part_no": part_no,
                "expires_at": int(time.time()) + 7 * 86400,
            }
        )

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        return self._table.get_item(Key={"pk": f"JOB#{job_id}", "sk": "TOKEN"}, ConsistentRead=True).get(
            "Item"
        )
