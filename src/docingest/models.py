"""Data contracts passed between stages (SQS messages, Step Functions payloads, stored records).

Every message is a pydantic model, versioned with ``schema_version`` so producers and consumers can
be deployed independently.
"""

from __future__ import annotations

import hashlib
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class DocStatus(StrEnum):
    RECEIVED = "RECEIVED"
    DUPLICATE = "DUPLICATE"
    ROUTED = "ROUTED"
    EXTRACTED = "EXTRACTED"
    CHUNKED = "CHUNKED"
    INDEXED = "INDEXED"
    REJECTED = "REJECTED"  # malware, unsupported type, too large
    FAILED = "FAILED"  # retries exhausted, in a DLQ


class Lane(StrEnum):
    FAST = "fast"
    HEAVY = "heavy"


class DocType(StrEnum):
    AMM = "AMM"  # Aircraft Maintenance Manual
    IPC = "IPC"  # Illustrated Parts Catalogue
    SB = "SB"  # Service Bulletin
    AD = "AD"  # Airworthiness Directive
    MEL = "MEL"  # Minimum Equipment List
    TASK_CARD = "TASK_CARD"
    SOP = "SOP"
    TRAINING = "TRAINING"
    OTHER = "OTHER"


class DocumentRef(BaseModel):
    """Identifies one uploaded object and what we know about it so far."""

    schema_version: int = 1
    doc_id: str
    tenant_id: str
    source: str
    bucket: str
    key: str
    version_id: str | None = None
    size: int
    content_type: str = "application/octet-stream"
    sha256: str | None = None
    lane: Lane | None = None
    page_count: int | None = None


class Page(BaseModel):
    number: int
    text: str
    confidence: float | None = None  # OCR only
    ocr: bool = False


class Chunk(BaseModel):
    chunk_id: str
    doc_id: str
    tenant_id: str
    chunk_no: int
    text: str
    token_estimate: int
    page_start: int
    page_end: int
    section: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @staticmethod
    def make_id(doc_id: str, chunk_no: int) -> str:
        # Deterministic: re-processing a document overwrites the same vectors (idempotent indexing).
        return f"{doc_id}:{chunk_no:05d}"

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(self.text.encode()).hexdigest()[:16]


class EmbedMessage(BaseModel):
    """One SQS message on the embed queue = up to 10 chunks of one document."""

    schema_version: int = 1
    doc_id: str
    tenant_id: str
    total_chunks: int
    batch_no: int
    hot: bool = True  # also write to OpenSearch (current revision); S3 Vectors always
    chunks: list[Chunk]
