"""Heavy lane: Step Functions (Standard) tasks for scans and very large files (~30% of documents).

    Plan ──> OcrParts (Map, wait for Textract via task token) ──> Collect
         └─(native, no OCR needed)──────────────────────────────> Collect
    Collect ──> ChunkRanges (Distributed Map, 200-page ranges) ──> Finalize

Why Step Functions *Standard* here (and not for the fast lane):
* Textract async jobs take seconds to many minutes; Standard workflows can wait up to a year on a
  task token for free, while Express workflows stop at 5 minutes.
* Per-state retries with backoff for Textract ``LimitExceededException`` and malware-scan delays.
* Distributed Map fans a 9,800-page parts catalogue out to 49 parallel chunking children.
* Standard is billed per state transition, so it is used only on the 30% that needs it; the 70%
  fast lane stays on plain SQS + Lambda.

Modes chosen by ``plan``:
    native        big PDF with a text layer (e.g. 410 MB IPC) - extract once, no OCR at all
    ocr_full      scanned PDF / TIFF / PNG / JPEG - OCR the original object
    ocr_selective mostly-text PDF with some image-only pages - OCR only those pages
    ocr_split     scanned PDF above the Textract page limit - OCR in 2,500-page parts

Example: a 64-page scanned work package for VT-EXA C-check -> ocr_full -> 1 Textract job ->
64 OCR pages (mean confidence 96.4) -> 1 range -> 118 chunks -> 12 embed messages.
"""

from __future__ import annotations

import json
from typing import Any

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit

from docingest.classify import FileKind
from docingest.deps import Deps, get_deps
from docingest.extract.native import extract_pdf
from docingest.extract.textract import (
    TABLE_DOC_TYPES,
    cut_pages,
    low_text_pages,
    pages_from_blocks,
    start_ocr,
)
from docingest.metadata import guess_doc_type
from docingest.models import Chunk, DocStatus, DocumentRef, Page
from docingest.pipeline import ScanPending, chunk_pages, enqueue_embeddings, malware_gate

logger = Logger(service="docingest-heavy-lane")
tracer = Tracer(service="docingest-heavy-lane")
metrics = Metrics(namespace="DocIngest", service="heavy-lane")

TEXTRACT_MAX_PAGES = 3000
OCR_PART_PAGES = 2500
RANGE_PAGES = 200
CHUNK_NO_STRIDE = 100_000  # chunk numbers per range: range i uses i*STRIDE + n (unique, deterministic)
SELECTIVE_MAX_RATIO = 0.8  # above this share of image pages, OCR the whole file instead


class TextractBusy(Exception):
    """Concurrent-job quota reached; Step Functions retries with exponential backoff."""


def _prefix(ref: DocumentRef) -> str:
    return f"{ref.tenant_id}/{ref.doc_id}"


def _write_pages(d: Deps, ref: DocumentRef, pages: list[Page], name: str = "pages.jsonl") -> None:
    body = "\n".join(p.model_dump_json() for p in pages).encode()
    d.s3.client.put_object(
        Bucket=d.settings.processed_bucket,
        Key=f"{_prefix(ref)}/{name}",
        Body=body,
        ServerSideEncryption="aws:kms",
        SSEKMSKeyId=d.settings.kms_key_id,
        BucketKeyEnabled=True,
    )


def _read_pages(d: Deps, ref: DocumentRef, name: str = "pages.jsonl") -> list[Page]:
    obj = d.s3.client.get_object(Bucket=d.settings.processed_bucket, Key=f"{_prefix(ref)}/{name}")
    return [Page.model_validate_json(line) for line in obj["Body"].iter_lines() if line]


def _upload_part(d: Deps, ref: DocumentRef, part_no: int, pdf: bytes) -> str:
    key = f"{_prefix(ref)}/ocr-parts/part-{part_no:03d}.pdf"
    d.s3.client.put_object(
        Bucket=d.settings.processed_bucket,
        Key=key,
        Body=pdf,
        ServerSideEncryption="aws:kms",
        SSEKMSKeyId=d.settings.kms_key_id,
    )
    return key


# ------------------------------------------------------------------------------------- Plan
@logger.inject_lambda_context
@tracer.capture_lambda_handler
def plan(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    d = get_deps()
    ref = DocumentRef.model_validate(event["doc"])
    malware_gate(d, ref)  # ScanPending -> Step Functions retry (60 s, x10)
    kind = FileKind(ref.content_type)

    if kind is not FileKind.PDF:  # images: one OCR job on the original object
        parts = [
            {
                "part_no": 0,
                "bucket": ref.bucket,
                "key": ref.key,
                "version_id": ref.version_id,
                "page_map": None,
            }
        ]
        return {"mode": "ocr_full", "ocr_parts": parts}

    data = d.s3.read(ref.key, ref.version_id)
    native = extract_pdf(data)
    low = low_text_pages(native, d.settings.min_text_chars_per_page)
    total = len(native)
    d.state.set_status(ref.doc_id, DocStatus.EXTRACTED, pages=total, image_pages=len(low))

    if not low:
        _write_pages(d, ref, native)
        return {"mode": "native", "ocr_parts": []}

    if len(low) / max(total, 1) <= SELECTIVE_MAX_RATIO:
        # OCR only the image pages; text pages keep their (exact) native text.
        _write_pages(d, ref, native, "pages-native.jsonl")
        parts = []
        for i in range(0, len(low), OCR_PART_PAGES):
            page_map = low[i : i + OCR_PART_PAGES]
            key = _upload_part(d, ref, i // OCR_PART_PAGES, cut_pages(data, page_map))
            parts.append(
                {
                    "part_no": i // OCR_PART_PAGES,
                    "bucket": d.settings.processed_bucket,
                    "key": key,
                    "version_id": None,
                    "page_map": page_map,
                }
            )
        metrics.add_metric(name="OcrPagesAvoided", unit=MetricUnit.Count, value=total - len(low))
        return {"mode": "ocr_selective", "ocr_parts": parts}

    if total <= TEXTRACT_MAX_PAGES:
        parts = [
            {
                "part_no": 0,
                "bucket": ref.bucket,
                "key": ref.key,
                "version_id": ref.version_id,
                "page_map": None,
            }
        ]
        return {"mode": "ocr_full", "ocr_parts": parts}

    parts = []
    for i in range(0, total, OCR_PART_PAGES):
        page_map = list(range(i + 1, min(i + OCR_PART_PAGES, total) + 1))
        key = _upload_part(d, ref, i // OCR_PART_PAGES, cut_pages(data, page_map))
        parts.append(
            {
                "part_no": i // OCR_PART_PAGES,
                "bucket": d.settings.processed_bucket,
                "key": key,
                "version_id": None,
                "page_map": page_map,
            }
        )
    return {"mode": "ocr_split", "ocr_parts": parts}


# ------------------------------------------------------------------------------------- OCR
@logger.inject_lambda_context
def start_ocr_part(event: dict[str, Any], _context: Any) -> None:
    """``lambda:invoke.waitForTaskToken`` task. Starts Textract and stores JobId -> task token."""
    d = get_deps()
    s = d.settings
    ref = DocumentRef.model_validate(event["doc"])
    part = event["part"]
    with_tables = guess_doc_type(ref.key.rsplit("/", 1)[-1], "").value in TABLE_DOC_TYPES
    try:
        job_id = start_ocr(
            d.textract,
            bucket=part["bucket"],
            key=part["key"],
            version_id=part.get("version_id"),
            doc_id=f"{ref.doc_id[:56]}-p{part['part_no']}",
            sns_topic_arn=s.textract_sns_topic_arn,
            role_arn=s.textract_role_arn,
            output_bucket=s.processed_bucket,
            output_prefix=f"{_prefix(ref)}/textract",
            with_tables=with_tables,
        )
    except d.textract.exceptions.LimitExceededException as exc:
        raise TextractBusy(ref.doc_id) from exc
    d.state.save_job(job_id, event["task_token"], ref.doc_id, part["part_no"])
    logger.info("ocr_started", extra={"doc_id": ref.doc_id, "job_id": job_id, "tables": with_tables})


@logger.inject_lambda_context
def textract_complete(event: dict[str, Any], _context: Any) -> None:
    """SNS subscriber: Textract finished a job -> resume the waiting Step Functions task."""
    d = get_deps()
    for record in event.get("Records", []):
        msg = json.loads(record["Sns"]["Message"])
        job = d.state.get_job(msg["JobId"])
        if job is None:
            logger.warning("unknown_job", extra={"job_id": msg["JobId"]})
            continue
        if msg["Status"] in ("SUCCEEDED", "PARTIAL_SUCCESS"):
            d.sfn.send_task_success(
                taskToken=job["task_token"],
                output=json.dumps(
                    {"job_id": msg["JobId"], "part_no": int(job["part_no"]), "status": msg["Status"]}
                ),
            )
        else:
            d.sfn.send_task_failure(
                taskToken=job["task_token"],
                error="TextractFailed",
                cause=f"job {msg['JobId']} status {msg['Status']}",
            )


def _read_textract_output(d: Deps, ref: DocumentRef, job_id: str) -> list[dict[str, Any]]:
    """Read the JSON files Textract wrote via OutputConfig (avoids the low GetDocument* TPS quota)."""
    s3 = d.s3.client
    prefix = f"{_prefix(ref)}/textract/{job_id}/"
    blocks: list[dict[str, Any]] = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=d.settings.processed_bucket, Prefix=prefix
    ):
        # Output files are named 1, 2, 3...; skip the ".s3_access_check" marker Textract writes.
        keys = [o["Key"] for o in page.get("Contents", []) if o["Key"].rsplit("/", 1)[-1].isdigit()]
        for key in sorted(keys, key=lambda k: int(k.rsplit("/", 1)[-1])):
            body = json.loads(s3.get_object(Bucket=d.settings.processed_bucket, Key=key)["Body"].read())
            blocks.extend(body.get("Blocks", []))
    return blocks


# ------------------------------------------------------------------------------------- Collect
@logger.inject_lambda_context
@tracer.capture_lambda_handler
def collect(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    """Merge OCR output (re-numbered to original pages) with native pages; plan chunk ranges."""
    d = get_deps()
    ref = DocumentRef.model_validate(event["doc"])
    mode = event["plan"]["mode"]
    parts = {p["part_no"]: p for p in event["plan"]["ocr_parts"]}

    if mode == "native":
        pages = _read_pages(d, ref)
    else:
        by_number: dict[int, Page] = {}
        if mode == "ocr_selective":
            by_number = {p.number: p for p in _read_pages(d, ref, "pages-native.jsonl")}
        confidences: list[float] = []
        for result in event.get("ocr", []):
            part = parts[result["part_no"]]
            for page in pages_from_blocks(_read_textract_output(d, ref, result["job_id"])):
                original = part["page_map"][page.number - 1] if part["page_map"] else page.number
                by_number[original] = page.model_copy(update={"number": original})
                confidences.append(page.confidence or 0.0)
        pages = [by_number[n] for n in sorted(by_number)]
        _write_pages(d, ref, pages)
        low_conf = sum(c < d.settings.ocr_confidence_floor for c in confidences)
        if low_conf:
            metrics.add_metric(name="LowConfidenceOcrPages", unit=MetricUnit.Count, value=low_conf)

    ranges = [
        {"index": i, "start": s, "end": min(s + RANGE_PAGES - 1, len(pages))}
        for i, s in enumerate(range(1, len(pages) + 1, RANGE_PAGES))
    ]
    return {"page_count": len(pages), "ranges": ranges}


# ------------------------------------------------------------------------------------- Chunk ranges
@logger.inject_lambda_context
def chunk_range(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    """Distributed Map child: chunk one 200-page range and store it as chunks-<index>.jsonl."""
    d = get_deps()
    ref = DocumentRef.model_validate(event["doc"])
    r = event["range"]
    pages = [p for p in _read_pages(d, ref) if r["start"] <= p.number <= r["end"]]
    chunks = chunk_pages(d, ref, pages, chunk_offset=r["index"] * CHUNK_NO_STRIDE)
    body = "\n".join(c.model_dump_json() for c in chunks).encode()
    d.s3.client.put_object(
        Bucket=d.settings.processed_bucket,
        Key=f"{_prefix(ref)}/chunks/chunks-{r['index']:05d}.jsonl",
        Body=body,
        ServerSideEncryption="aws:kms",
        SSEKMSKeyId=d.settings.kms_key_id,
        BucketKeyEnabled=True,
    )
    return {"index": r["index"], "chunks": len(chunks)}


# ------------------------------------------------------------------------------------- Finalize
@logger.inject_lambda_context
def finalize(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    d = get_deps()
    ref = DocumentRef.model_validate(event["doc"])
    s3 = d.s3.client
    chunks: list[Chunk] = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=d.settings.processed_bucket, Prefix=f"{_prefix(ref)}/chunks/"
    ):
        for obj in sorted(page.get("Contents", []), key=lambda o: o["Key"]):
            body = s3.get_object(Bucket=d.settings.processed_bucket, Key=obj["Key"])["Body"]
            chunks.extend(Chunk.model_validate_json(line) for line in body.iter_lines() if line)
    if not chunks:
        d.state.set_status(ref.doc_id, DocStatus.REJECTED, reason="no extractable text after OCR")
        return {"chunks": 0}
    d.s3.put_chunks(ref.tenant_id, ref.doc_id, chunks)
    batches = enqueue_embeddings(d, ref, chunks)
    metrics.add_metric(name="ChunksCreated", unit=MetricUnit.Count, value=len(chunks))
    return {"chunks": len(chunks), "embed_batches": batches}


@logger.inject_lambda_context
def mark_failed(event: dict[str, Any], _context: Any) -> None:
    d = get_deps()
    ref = DocumentRef.model_validate(event["doc"])
    err = event.get("error", {})
    d.state.set_status(
        ref.doc_id,
        DocStatus.FAILED,
        error=str(err.get("Error", "unknown"))[:200],
        cause=str(err.get("Cause", ""))[:1000],
    )


__all__ = [
    "ScanPending",
    "TextractBusy",
    "chunk_range",
    "collect",
    "finalize",
    "mark_failed",
    "plan",
    "start_ocr_part",
    "textract_complete",
]
