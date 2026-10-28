"""Router Lambda: S3 event -> SQS ingest queue -> this function -> fast queue or heavy workflow.

Why S3 -> SQS -> Lambda (and not S3 -> Lambda directly, or EventBridge)?
* SQS absorbs bursts (a partner drops 50,000 files at 01:00) and lets us cap Lambda
  concurrency; failed messages retry 5 times then land in a DLQ, so nothing is lost.
* Direct S3 -> SQS notifications are cheaper and simpler than routing through EventBridge for a
  single consumer.

Per object the router does only cheap work (target: < 300 ms, no full download except small PDFs):
1. parse the key -> tenant, source, doc_id
2. ``claim`` the doc in DynamoDB (duplicate S3/SQS deliveries become no-ops)
3. ``HeadObject`` -> size, content type, SHA-256 checksum if the uploader supplied one
4. ranged GET of the first 8 KB -> magic-byte type detection
5. small PDFs: probe the text layer on 3 sampled pages
6. dedupe by content hash (partners re-send identical files under new names)
7. send to the fast SQS queue, or start the heavy-lane Step Functions execution (name = doc_id,
   so a retry cannot start two OCR jobs for one document)

Example: ``example-air/oem-feed/batch-1028/SB-A320-32-1471.pdf`` (2.3 MB, text layer) -> FAST.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
from typing import Any

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

from docingest.classify import FileKind, decide, probe_pdf, sniff
from docingest.deps import Deps, get_deps
from docingest.keys import KeyError_, decode_event_key, parse_key
from docingest.models import DocStatus, DocumentRef, Lane

logger = Logger(service="docingest-router")
tracer = Tracer(service="docingest-router")
metrics = Metrics(namespace="DocIngest", service="router")
processor = BatchProcessor(event_type=EventType.SQS)


def content_sha256(head: dict[str, Any], body: bytes | None) -> str:
    checksum = head.get("ChecksumSHA256")
    if checksum and "-" not in checksum:  # full-object checksum (multipart gives a composite one)
        return base64.b64decode(checksum).hex()
    if body is not None:
        return hashlib.sha256(body).hexdigest()
    # Large objects without a checksum: ETag + size is a good-enough duplicate fingerprint.
    return hashlib.sha256(f"{head['ETag']}:{head['ContentLength']}".encode()).hexdigest()


def route_object(d: Deps, bucket: str, key: str, version_id: str | None) -> str:
    s = d.settings
    try:
        parsed = parse_key(bucket, key, version_id)
    except KeyError_:
        logger.warning("ignored_key", extra={"key": key})
        return "ignored"

    head = d.s3.head(key, version_id)
    size = int(head["ContentLength"])
    if not d.state.claim(parsed.doc_id, parsed.tenant_id, key, size):
        current = d.state.get(parsed.doc_id) or {}
        if current.get("status") != DocStatus.RECEIVED.value:
            return "already_routed"  # duplicate delivery of an event we finished
        # else: a previous attempt crashed after claiming; continue so the doc is not lost

    kind = sniff(d.s3.read_head(key, version_id), parsed.filename)
    body: bytes | None = None
    pdf = None
    if kind is FileKind.PDF and size <= s.fast_lane_max_bytes:
        body = d.s3.read(key, version_id)
        pdf = probe_pdf(body)
    decision = decide(
        kind,
        size,
        max_fast_bytes=s.fast_lane_max_bytes,
        max_fast_pages=s.fast_lane_max_pages,
        min_chars_per_page=s.min_text_chars_per_page,
        pdf=pdf,
    )
    if decision.lane is None:
        d.state.set_status(parsed.doc_id, DocStatus.REJECTED, reason=decision.reason)
        metrics.add_metric(name="Rejected", unit=MetricUnit.Count, value=1)
        return "rejected"

    sha = content_sha256(head, body if body is not None and size <= s.fast_lane_max_bytes else None)
    if owner := d.state.register_hash(parsed.tenant_id, sha, parsed.doc_id):
        d.state.set_status(parsed.doc_id, DocStatus.DUPLICATE, duplicate_of=owner, sha256=sha)
        metrics.add_metric(name="Duplicates", unit=MetricUnit.Count, value=1)
        return "duplicate"

    ref = DocumentRef(
        doc_id=parsed.doc_id,
        tenant_id=parsed.tenant_id,
        source=parsed.source,
        bucket=bucket,
        key=key,
        version_id=version_id,
        size=size,
        content_type=kind.value,
        sha256=sha,
        lane=decision.lane,
        page_count=decision.page_count,
    )
    if decision.lane is Lane.FAST:
        d.sqs.send_message(QueueUrl=s.fast_queue_url, MessageBody=ref.model_dump_json())
    else:
        with contextlib.suppress(d.sfn.exceptions.ExecutionAlreadyExists):  # idempotent start
            d.sfn.start_execution(
                stateMachineArn=s.heavy_state_machine_arn,
                name=parsed.doc_id,
                input=json.dumps({"doc": ref.model_dump(mode="json")}),
            )
    d.state.set_status(
        parsed.doc_id,
        DocStatus.ROUTED,
        lane=decision.lane,
        reason=decision.reason,
        sha256=sha,
        kind=kind.value,
    )
    metrics.add_metric(name=f"Routed{decision.lane.value.title()}", unit=MetricUnit.Count, value=1)
    logger.info("routed", extra={"doc_id": parsed.doc_id, "lane": decision.lane, "reason": decision.reason})
    return decision.lane.value


def record_handler(record: SQSRecord) -> None:
    d = get_deps()
    message = json.loads(record.body)
    if message.get("Event") == "s3:TestEvent":
        return
    for rec in message.get("Records", []):
        s3 = rec["s3"]
        route_object(
            d, s3["bucket"]["name"], decode_event_key(s3["object"]["key"]), s3["object"].get("versionId")
        )


@logger.inject_lambda_context
@tracer.capture_lambda_handler
@metrics.log_metrics
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    # Partial batch response: only failed messages are retried, not the whole batch of 10.
    return process_partial_response(
        event=event, record_handler=record_handler, processor=processor, context=context
    )
