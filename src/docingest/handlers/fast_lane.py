"""Fast lane: SQS fast queue -> scan gate -> native parse -> normalise + chunk -> embed queue.

Handles ~70% of documents (born-digital PDFs, DOCX, HTML, XML, text) in one Lambda invocation,
typically 1-5 seconds each. Lambda: 2,048 MB, 2 GB /tmp, 5 min timeout, SQS batch size 5,
5 receives then DLQ.

If extraction shows that many pages are actually image-only (the router samples only 3 pages),
the document is promoted to the heavy lane for selective OCR instead of indexing empty pages.

Example: ``SB-A320-32-1471.pdf`` (14 pages) -> 14 pages extracted -> 31 chunks ->
4 embed messages (10 chunks each, last one 1).
"""

from __future__ import annotations

import contextlib
import json
from typing import Any

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

from docingest.classify import FileKind
from docingest.deps import get_deps
from docingest.extract.native import extract
from docingest.extract.textract import low_text_pages
from docingest.models import DocStatus, DocumentRef, Lane
from docingest.pipeline import ThreatFound, malware_gate, process_pages

logger = Logger(service="docingest-fast-lane")
tracer = Tracer(service="docingest-fast-lane")
metrics = Metrics(namespace="DocIngest", service="fast-lane")
processor = BatchProcessor(event_type=EventType.SQS)

PROMOTE_IF_LOW_TEXT_RATIO = 0.2


def process(ref: DocumentRef) -> str:
    d = get_deps()
    s = d.settings
    state = d.state.get(ref.doc_id) or {}
    if state.get("status") in (DocStatus.CHUNKED.value, DocStatus.INDEXED.value, DocStatus.REJECTED.value):
        return "already_processed"  # duplicate SQS delivery

    try:
        malware_gate(d, ref)
    except ThreatFound:
        metrics.add_metric(name="MalwareRejected", unit=MetricUnit.Count, value=1)
        return "rejected"

    pages = extract(FileKind(ref.content_type), d.s3.read(ref.key, ref.version_id))
    d.state.set_status(ref.doc_id, DocStatus.EXTRACTED, pages=len(pages))

    if FileKind(ref.content_type) is FileKind.PDF:
        low = low_text_pages(pages, s.min_text_chars_per_page)
        if pages and len(low) / len(pages) > PROMOTE_IF_LOW_TEXT_RATIO:
            promoted = ref.model_copy(update={"lane": Lane.HEAVY, "page_count": len(pages)})
            with contextlib.suppress(d.sfn.exceptions.ExecutionAlreadyExists):  # idempotent start
                d.sfn.start_execution(
                    stateMachineArn=s.heavy_state_machine_arn,
                    name=ref.doc_id,
                    input=json.dumps({"doc": promoted.model_dump(mode="json")}),
                )
            d.state.set_status(
                ref.doc_id, DocStatus.ROUTED, lane=Lane.HEAVY, reason=f"{len(low)} image-only pages"
            )
            metrics.add_metric(name="PromotedToHeavy", unit=MetricUnit.Count, value=1)
            return "promoted"

    chunks = process_pages(d, ref, pages)
    metrics.add_metric(name="ChunksCreated", unit=MetricUnit.Count, value=chunks)
    return "chunked"


def record_handler(record: SQSRecord) -> None:
    process(DocumentRef.model_validate_json(record.body))


@logger.inject_lambda_context
@tracer.capture_lambda_handler
@metrics.log_metrics
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    return process_partial_response(
        event=event, record_handler=record_handler, processor=processor, context=context
    )
