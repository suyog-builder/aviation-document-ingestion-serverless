"""Amazon S3 Vectors - the *warm* tier: every chunk of every revision, at object-storage cost.

OpenSearch Serverless is billed by compute units that must hold the HNSW graph in memory; keeping
10+ years of superseded manual revisions there is expensive. S3 Vectors stores them durably and
serves sub-second queries for the long tail ("what did revision 52 of this task say in 2023?",
audit and investigation use cases).

Limits that shape this code (check the S3 Vectors quotas page for current values):
* up to 500 vectors per ``PutVectors`` call,
* metadata per vector is size-limited, and only a subset is filterable - so we store ids and
  filter keys here, and keep the full chunk text in the processed S3 bucket.
"""

from __future__ import annotations

from typing import Any

from docingest.models import Chunk

MAX_BATCH = 500
FILTER_KEYS = ("tenant_id", "doc_type", "fleet_type", "ata_chapter", "revision")


def to_vector(chunk: Chunk, vector: list[float], processed_uri: str) -> dict[str, Any]:
    metadata: dict[str, Any] = {k: str(chunk.metadata.get(k, "")) for k in FILTER_KEYS if k != "tenant_id"}
    metadata.update(
        tenant_id=chunk.tenant_id,
        doc_id=chunk.doc_id,
        chunk_no=chunk.chunk_no,
        page_start=chunk.page_start,
        text_uri=processed_uri,
    )
    return {"key": chunk.chunk_id, "data": {"float32": vector}, "metadata": metadata}


class S3VectorsWriter:
    def __init__(self, client: Any, bucket: str, index: str) -> None:
        self._client = client
        self._bucket = bucket
        self._index = index

    def write(self, vectors: list[dict[str, Any]]) -> None:
        for i in range(0, len(vectors), MAX_BATCH):
            self._client.put_vectors(
                vectorBucketName=self._bucket, indexName=self._index, vectors=vectors[i : i + MAX_BATCH]
            )
