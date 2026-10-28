"""Amazon Titan Text Embeddings V2 client.

* 1,024 dimensions, ``normalize=true`` (unit vectors -> inner product == cosine similarity).
* Bedrock on-demand embeddings are limited by **requests per minute** per account and region,
  not by our Lambda concurrency. The embed queue + capped Lambda concurrency is the throttle:
  raise ``reserved concurrency`` only together with the Bedrock quota.
* Throttling is retried a little in-process (adaptive mode), then surfaced so SQS redelivers the
  message after its visibility timeout - natural backoff without busy-waiting in Lambda.

Example request body: ``{"inputText": "[AMM > TASK 32-42-27-200-801] ...", "dimensions": 1024,
"normalize": true}`` -> ``{"embedding": [0.0123, ...], "inputTextTokenCount": 412}``.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

MAX_INPUT_CHARS = 50_000  # Titan V2 hard limit; our chunks are ~2,000 chars
RETRYABLE = {
    "ThrottlingException",
    "ServiceUnavailableException",
    "ModelNotReadyException",
    "InternalServerException",
}


class EmbeddingThrottled(Exception):
    """Raised when Bedrock keeps throttling; the SQS message should be retried later."""


@dataclass(frozen=True)
class Embedding:
    vector: list[float]
    tokens: int


class TitanEmbedder:
    def __init__(self, model_id: str, dimensions: int, region: str, client: Any | None = None) -> None:
        self._model_id = model_id
        self._dimensions = dimensions
        self._client = client or boto3.client(
            "bedrock-runtime",
            region_name=region,
            config=Config(
                retries={"mode": "adaptive", "max_attempts": 3}, read_timeout=30, max_pool_connections=32
            ),
        )

    def embed(self, text: str) -> Embedding:
        body = json.dumps(
            {"inputText": text[:MAX_INPUT_CHARS], "dimensions": self._dimensions, "normalize": True}
        )
        try:
            resp = self._client.invoke_model(
                modelId=self._model_id, body=body, contentType="application/json", accept="application/json"
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] in RETRYABLE:
                raise EmbeddingThrottled(exc.response["Error"]["Code"]) from exc
            raise
        payload = json.loads(resp["body"].read())
        vector = payload["embedding"]
        if len(vector) != self._dimensions:
            raise ValueError(f"expected {self._dimensions} dims, got {len(vector)}")
        return Embedding(vector=vector, tokens=int(payload.get("inputTextTokenCount", 0)))

    def embed_many(self, texts: list[str], threads: int = 8) -> list[Embedding]:
        """Parallel within one Lambda; boto3 clients are thread-safe for calls."""
        if len(texts) <= 1:
            return [self.embed(t) for t in texts]
        with ThreadPoolExecutor(max_workers=min(threads, len(texts))) as pool:
            return list(pool.map(self.embed, texts))
