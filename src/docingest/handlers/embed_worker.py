"""Embed workers: SQS embed queue -> Titan Text Embeddings V2 -> OpenSearch, S3 Vectors, DynamoDB.

This queue is the **speed limiter for Bedrock**. Throughput is set by:

    reserved concurrency (25-50) x threads per invocation (8) x ~10-15 embeddings/s per thread

and must stay below the account's Bedrock requests/min quota for Titan V2. At 20M chunks/day the
average need is ~230 embeddings/s (~14,000/min) and the peak ~5x that, so request a quota increase
before go-live, and use Bedrock *batch inference* (about half the on-demand price) for back-fills.

Per message (<= 10 chunks of one document):
1. embed the chunks in parallel threads,
2. write S3 Vectors (all chunks, warm tier) and OpenSearch (current revisions, hot tier) with the
   chunk id as the document id - retries overwrite, never duplicate,
3. count the batch exactly once in DynamoDB; the worker that completes the document writes the
   metadata/ACL record and marks it INDEXED.

Throttling -> the message fails -> SQS redelivers after the visibility timeout (natural backoff);
10 receives -> DLQ with an alarm.
"""

from __future__ import annotations

from typing import Any

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

from docingest.deps import get_embed_deps
from docingest.models import EmbedMessage
from docingest.stores.opensearch import to_document
from docingest.stores.s3vectors import to_vector

logger = Logger(service="docingest-embed")
tracer = Tracer(service="docingest-embed")
metrics = Metrics(namespace="DocIngest", service="embed")
processor = BatchProcessor(event_type=EventType.SQS)


class IndexWriteFailed(Exception):
    pass


def process(msg: EmbedMessage) -> bool:
    d = get_embed_deps()
    s = d.settings
    texts = [c.text for c in msg.chunks]
    embeddings = d.embedder.embed_many(texts, threads=s.embed_threads)  # type: ignore[union-attr]

    state = d.state.get(msg.doc_id) or {}
    source_uri = f"s3://{s.raw_bucket}/{state.get('s3_key', msg.doc_id)}"
    processed_uri = f"s3://{s.processed_bucket}/{msg.tenant_id}/{msg.doc_id}/chunks.jsonl"

    d.s3vectors.write(
        [to_vector(c, e.vector, processed_uri) for c, e in zip(msg.chunks, embeddings, strict=True)]
    )
    if msg.hot:
        failed = d.opensearch.write(
            [
                (c.chunk_id, to_document(c, e.vector, source_uri))
                for c, e in zip(msg.chunks, embeddings, strict=True)
            ]
        )
        if failed:
            raise IndexWriteFailed(f"{len(failed)} chunks failed for {msg.doc_id}")

    tokens = sum(e.tokens for e in embeddings)
    metrics.add_metric(name="ChunksEmbedded", unit=MetricUnit.Count, value=len(msg.chunks))
    metrics.add_metric(name="TokensEmbedded", unit=MetricUnit.Count, value=tokens)

    completed = d.state.add_embedded(msg.doc_id, msg.batch_no, len(msg.chunks))
    if completed:
        first = msg.chunks[0].metadata
        d.metadata.put(
            doc_id=msg.doc_id,
            tenant_id=msg.tenant_id,
            source_uri=source_uri,
            metadata={
                k: v
                for k, v in first.items()
                if k in ("doc_type", "fleet_type", "revision", "source", "sb_number", "ad_number")
            },
            chunks=msg.total_chunks,
            acl_groups=[f"{msg.tenant_id}:all"],
        )
        metrics.add_metric(name="DocumentsIndexed", unit=MetricUnit.Count, value=1)
        logger.info("indexed", extra={"doc_id": msg.doc_id, "chunks": msg.total_chunks})
    return completed


def record_handler(record: SQSRecord) -> None:
    process(EmbedMessage.model_validate_json(record.body))


@logger.inject_lambda_context
@tracer.capture_lambda_handler
@metrics.log_metrics
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    return process_partial_response(
        event=event, record_handler=record_handler, processor=processor, context=context
    )
