"""Raw and processed S3 access plus the DynamoDB document metadata/ACL record.

Processed layout (KMS-encrypted, versioned):
    s3://<processed>/<tenant>/<doc_id>/pages.jsonl     one JSON page per line (text, OCR confidence)
    s3://<processed>/<tenant>/<doc_id>/chunks.jsonl    chunk text + metadata (re-embedding source)
    s3://<processed>/<tenant>/<doc_id>/textract/       raw Textract output (heavy lane)

Keeping chunks in S3 means a model upgrade (e.g. a new embedding model) re-embeds from
``chunks.jsonl`` without re-running OCR, which is by far the most expensive stage.
"""

from __future__ import annotations

import time
from typing import Any

import boto3

from docingest.models import Chunk, Page

HEAD_BYTES = 8192


class S3Store:
    def __init__(
        self, raw_bucket: str, processed_bucket: str, kms_key_id: str, client: Any | None = None
    ) -> None:
        self.raw_bucket = raw_bucket
        self.processed_bucket = processed_bucket
        self._kms = kms_key_id
        self._s3 = client or boto3.client("s3")

    @property
    def client(self) -> Any:
        return self._s3

    def head(self, key: str, version_id: str | None = None) -> dict[str, Any]:
        kwargs: dict[str, Any] = {"Bucket": self.raw_bucket, "Key": key, "ChecksumMode": "ENABLED"}
        if version_id:
            kwargs["VersionId"] = version_id
        return self._s3.head_object(**kwargs)

    def read_head(self, key: str, version_id: str | None = None) -> bytes:
        kwargs: dict[str, Any] = {"Bucket": self.raw_bucket, "Key": key, "Range": f"bytes=0-{HEAD_BYTES - 1}"}
        if version_id:
            kwargs["VersionId"] = version_id
        return self._s3.get_object(**kwargs)["Body"].read()

    def read(self, key: str, version_id: str | None = None) -> bytes:
        kwargs: dict[str, Any] = {"Bucket": self.raw_bucket, "Key": key}
        if version_id:
            kwargs["VersionId"] = version_id
        return self._s3.get_object(**kwargs)["Body"].read()

    def tags(self, key: str, version_id: str | None = None) -> dict[str, str]:
        kwargs: dict[str, Any] = {"Bucket": self.raw_bucket, "Key": key}
        if version_id:
            kwargs["VersionId"] = version_id
        return {t["Key"]: t["Value"] for t in self._s3.get_object_tagging(**kwargs)["TagSet"]}

    def _put(self, key: str, body: bytes, content_type: str) -> str:
        self._s3.put_object(
            Bucket=self.processed_bucket,
            Key=key,
            Body=body,
            ContentType=content_type,
            ServerSideEncryption="aws:kms",
            SSEKMSKeyId=self._kms,
            BucketKeyEnabled=True,  # S3 Bucket Keys cut KMS request costs by ~99% at this volume
        )
        return f"s3://{self.processed_bucket}/{key}"

    def put_pages(self, tenant_id: str, doc_id: str, pages: list[Page]) -> str:
        body = "\n".join(p.model_dump_json() for p in pages).encode()
        return self._put(f"{tenant_id}/{doc_id}/pages.jsonl", body, "application/x-ndjson")

    def put_chunks(self, tenant_id: str, doc_id: str, chunks: list[Chunk]) -> str:
        body = "\n".join(c.model_dump_json() for c in chunks).encode()
        return self._put(f"{tenant_id}/{doc_id}/chunks.jsonl", body, "application/x-ndjson")

    def get_pages(self, tenant_id: str, doc_id: str) -> list[Page]:
        obj = self._s3.get_object(Bucket=self.processed_bucket, Key=f"{tenant_id}/{doc_id}/pages.jsonl")
        return [Page.model_validate_json(line) for line in obj["Body"].iter_lines() if line]

    def quarantine(self, key: str, version_id: str | None, quarantine_bucket: str) -> None:
        source: dict[str, str] = {"Bucket": self.raw_bucket, "Key": key}
        if version_id:
            source["VersionId"] = version_id
        self._s3.copy_object(Bucket=quarantine_bucket, Key=key, CopySource=source)
        self._s3.delete_object(Bucket=self.raw_bucket, Key=key)


class MetadataStore:
    """DynamoDB ``docingest-metadata``: one item per document with its access groups.

    Read by the agent platform to post-filter results ("can this user see this document?") and by
    data stewards to answer "which documents mention part number D23189000-9?".
    """

    def __init__(self, table_name: str, resource: Any | None = None) -> None:
        self._table = (resource or boto3.resource("dynamodb")).Table(table_name)

    def put(
        self,
        *,
        doc_id: str,
        tenant_id: str,
        source_uri: str,
        metadata: dict[str, Any],
        chunks: int,
        acl_groups: list[str],
    ) -> None:
        self._table.put_item(
            Item={
                "pk": f"TENANT#{tenant_id}",
                "sk": f"DOC#{doc_id}",
                "doc_id": doc_id,
                "source_uri": source_uri,
                "chunks": chunks,
                "acl_groups": acl_groups or [f"{tenant_id}:all"],
                "indexed_at": int(time.time()),
                **{k: v for k, v in metadata.items() if isinstance(v, str)},
            }
        )
