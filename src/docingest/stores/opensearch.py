"""OpenSearch Serverless (vector search collection) - the *hot* tier.

Holds the current revision of each document for low-latency hybrid (k-NN + BM25) search.

* Field names follow the Bedrock Knowledge Bases defaults, so a Knowledge Base configured on this
  index (vector field ``bedrock-knowledge-base-default-vector``, text field
  ``AMAZON_BEDROCK_TEXT_CHUNK``, metadata field ``AMAZON_BEDROCK_METADATA``) can query vectors
  written by this pipeline. Filterable metadata is also stored as top-level keyword fields.
* HNSW on the Faiss engine with **fp16 scalar quantisation** halves vector memory (= OCUs = cost)
  with negligible recall loss for 1,024-dim normalised embeddings. Measure recall on your own
  evaluation set before going further (byte or binary quantisation).
* Document ``_id`` = chunk id, so re-processing overwrites instead of duplicating.
* Writes are batched with ``_bulk``; per-item errors are returned so only failed chunks retry.
"""

from __future__ import annotations

import json
from typing import Any

from docingest.models import Chunk

VECTOR_FIELD = "bedrock-knowledge-base-default-vector"
TEXT_FIELD = "AMAZON_BEDROCK_TEXT_CHUNK"
METADATA_FIELD = "AMAZON_BEDROCK_METADATA"
KEYWORD_FIELDS = (
    "tenant_id",
    "doc_id",
    "doc_type",
    "fleet_type",
    "ata_chapter",
    "revision",
    "task_refs",
    "part_numbers",
    "mel_items",
    "registrations",
    "sb_number",
    "ad_number",
)


def index_body(dimensions: int) -> dict[str, Any]:
    """Index definition, applied once by deploy/create_indexes.py."""
    return {
        "settings": {"index": {"knn": True}},
        "mappings": {
            "properties": {
                VECTOR_FIELD: {
                    "type": "knn_vector",
                    "dimension": dimensions,
                    "method": {
                        "name": "hnsw",
                        "engine": "faiss",
                        "space_type": "innerproduct",
                        "parameters": {
                            "m": 16,
                            "ef_construction": 256,
                            "encoder": {"name": "sq", "parameters": {"type": "fp16"}},
                        },
                    },
                },
                TEXT_FIELD: {"type": "text", "analyzer": "standard"},
                METADATA_FIELD: {"type": "text", "index": False},
                "page_start": {"type": "integer"},
                "page_end": {"type": "integer"},
                **{f: {"type": "keyword"} for f in KEYWORD_FIELDS},
            }
        },
    }


def make_client(endpoint: str, region: str) -> Any:
    import boto3
    from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection

    auth = AWSV4SignerAuth(boto3.Session().get_credentials(), region, "aoss")
    return OpenSearch(
        hosts=[{"host": endpoint.removeprefix("https://"), "port": 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        pool_maxsize=16,
        timeout=30,
    )


def to_document(chunk: Chunk, vector: list[float], source_uri: str) -> dict[str, Any]:
    keywords = {k: chunk.metadata[k] for k in KEYWORD_FIELDS if k in chunk.metadata}
    return {
        VECTOR_FIELD: vector,
        TEXT_FIELD: chunk.text,
        METADATA_FIELD: json.dumps(
            {"source": source_uri, "page": chunk.page_start, "section": chunk.section}
        ),
        "x-amz-bedrock-kb-source-uri": source_uri,
        "x-amz-bedrock-kb-document-page-number": chunk.page_start,
        "page_start": chunk.page_start,
        "page_end": chunk.page_end,
        "tenant_id": chunk.tenant_id,
        "doc_id": chunk.doc_id,
        **keywords,
    }


class OpenSearchWriter:
    def __init__(self, client: Any, index: str) -> None:
        self._client = client
        self._index = index

    def write(self, docs: list[tuple[str, dict[str, Any]]]) -> list[str]:
        """Bulk index ``(chunk_id, document)`` pairs. Returns chunk ids that failed."""
        if not docs:
            return []
        body: list[dict[str, Any]] = []
        for chunk_id, doc in docs:
            body.append({"index": {"_index": self._index, "_id": chunk_id}})
            body.append(doc)
        resp = self._client.bulk(body=body)
        if not resp.get("errors"):
            return []
        return [item["index"]["_id"] for item in resp["items"] if item["index"].get("status", 500) >= 300]
