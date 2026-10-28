"""Lazily-created AWS clients and stores, shared by all handlers.

Created on first use and reused across warm invocations (connection reuse matters at 23 docs/s).
Tests replace them with ``set_deps()``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import boto3
from botocore.config import Config

from docingest.config import Settings, get_settings
from docingest.embed import TitanEmbedder
from docingest.state import StateStore
from docingest.stores.s3store import MetadataStore, S3Store

_CFG = Config(retries={"mode": "standard", "max_attempts": 5}, max_pool_connections=32)


@dataclass
class Deps:
    settings: Settings
    s3: S3Store
    state: StateStore
    metadata: MetadataStore
    sqs: Any
    sfn: Any
    textract: Any
    embedder: TitanEmbedder | None = None
    opensearch: Any = None  # OpenSearchWriter
    s3vectors: Any = None  # S3VectorsWriter


_deps: Deps | None = None


def get_deps() -> Deps:
    global _deps
    if _deps is None:
        s = get_settings()
        _deps = Deps(
            settings=s,
            s3=S3Store(s.raw_bucket, s.processed_bucket, s.kms_key_id, boto3.client("s3", config=_CFG)),
            state=StateStore(s.state_table),
            metadata=MetadataStore(s.metadata_table),
            sqs=boto3.client("sqs", config=_CFG),
            sfn=boto3.client("stepfunctions", config=_CFG),
            textract=boto3.client("textract", config=_CFG),
        )
    return _deps


def get_embed_deps() -> Deps:
    """Embed worker needs the embedding model and both vector stores."""
    d = get_deps()
    if d.embedder is None:
        from docingest.stores.opensearch import OpenSearchWriter, make_client
        from docingest.stores.s3vectors import S3VectorsWriter

        s = d.settings
        d.embedder = TitanEmbedder(s.embed_model_id, s.embed_dimensions, s.aws_region)
        d.opensearch = OpenSearchWriter(make_client(s.opensearch_endpoint, s.aws_region), s.opensearch_index)
        d.s3vectors = S3VectorsWriter(
            boto3.client("s3vectors", config=_CFG), s.s3vectors_bucket, s.s3vectors_index
        )
    return d


def set_deps(deps: Deps | None) -> None:
    global _deps
    _deps = deps
