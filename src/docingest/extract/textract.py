"""Amazon Textract asynchronous OCR for the heavy lane (the main cost hotspot).

Design:
* **Async API only** (``StartDocumentTextDetection``): works for multi-page PDF/TIFF up to
  3,000 pages and does not hold a Lambda open while OCR runs. Completion is signalled on SNS,
  which resumes the waiting Step Functions task (task-token pattern) - no polling.
* **Idempotent start**: ``ClientRequestToken`` = document id, so a retried start returns the
  same JobId instead of paying for a second OCR run.
* **Cheapest feature first**: plain text detection is ~10x cheaper than ``AnalyzeDocument``
  with TABLES. Tables are requested only for document types where table layout carries the
  meaning (IPC parts lists, task cards with sign-off grids).
* **Selective OCR**: for mixed PDFs (mostly text, a few scanned pages such as wiring diagrams
  with handwritten annotations) only the low-text pages are cut into a small PDF and sent.
  On a 400-page manual with 12 scanned pages this cuts OCR cost by ~97%.
* **Concurrency**: Textract allows a fixed number of concurrent async jobs per account
  (check the "concurrent jobs" quota in Service Quotas). Step Functions retries
  ``LimitExceededException`` with exponential backoff, so peaks queue instead of failing.
"""

from __future__ import annotations

import io
from collections import defaultdict
from statistics import fmean
from typing import Any

from docingest.models import Page

TABLE_DOC_TYPES = {"IPC", "TASK_CARD"}


def start_ocr(
    textract: Any,
    *,
    bucket: str,
    key: str,
    version_id: str | None,
    doc_id: str,
    sns_topic_arn: str,
    role_arn: str,
    output_bucket: str,
    output_prefix: str,
    with_tables: bool = False,
) -> str:
    s3_object: dict[str, str] = {"Bucket": bucket, "Name": key}
    if version_id:
        s3_object["Version"] = version_id
    common: dict[str, Any] = {
        "DocumentLocation": {"S3Object": s3_object},
        "ClientRequestToken": doc_id[:64],
        "JobTag": doc_id[:64],
        "NotificationChannel": {"SNSTopicArn": sns_topic_arn, "RoleArn": role_arn},
        "OutputConfig": {"S3Bucket": output_bucket, "S3Prefix": output_prefix},
    }
    if with_tables:
        resp = textract.start_document_analysis(FeatureTypes=["TABLES", "LAYOUT"], **common)
    else:
        resp = textract.start_document_text_detection(**common)
    return resp["JobId"]


def pages_from_blocks(blocks: list[dict[str, Any]], page_offset: int = 0) -> list[Page]:
    """Rebuild page text from LINE blocks in reading order, with mean confidence per page."""
    lines: dict[int, list[str]] = defaultdict(list)
    confs: dict[int, list[float]] = defaultdict(list)
    for b in blocks:
        if b.get("BlockType") == "LINE":
            page = int(b.get("Page", 1)) + page_offset
            lines[page].append(b.get("Text", ""))
            confs[page].append(float(b.get("Confidence", 0.0)))
    return [
        Page(
            number=p,
            text="\n".join(lines[p]),
            confidence=round(fmean(confs[p]), 2) if confs[p] else 0.0,
            ocr=True,
        )
        for p in sorted(lines)
    ]


def collect_results(textract: Any, job_id: str, *, with_tables: bool = False) -> tuple[str, list[Page]]:
    """Page through GetDocument* results. For very large jobs prefer the S3 OutputConfig files."""
    getter = textract.get_document_analysis if with_tables else textract.get_document_text_detection
    blocks: list[dict[str, Any]] = []
    token: str | None = None
    status = "IN_PROGRESS"
    while True:
        kwargs: dict[str, Any] = {"JobId": job_id, "MaxResults": 1000}
        if token:
            kwargs["NextToken"] = token
        resp = getter(**kwargs)
        status = resp["JobStatus"]
        if status != "SUCCEEDED" and status != "PARTIAL_SUCCESS":
            return status, []
        blocks.extend(resp.get("Blocks", []))
        token = resp.get("NextToken")
        if not token:
            break
    return status, pages_from_blocks(blocks)


def low_text_pages(pages: list[Page], min_chars: int) -> list[int]:
    return [p.number for p in pages if len(p.text.strip()) < min_chars]


def cut_pages(pdf_bytes: bytes, page_numbers: list[int]) -> bytes:
    """Build a small PDF that contains only ``page_numbers`` (1-based) for selective OCR."""
    from pypdf import PdfReader, PdfWriter

    reader = PdfReader(io.BytesIO(pdf_bytes))
    writer = PdfWriter()
    for n in page_numbers:
        writer.add_page(reader.pages[n - 1])
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def merge_pages(native: list[Page], ocr: list[Page], ocr_page_map: list[int]) -> list[Page]:
    """Replace low-text native pages with their OCR result (``ocr_page_map[i]`` = original page)."""
    by_number = {p.number: p for p in native}
    for ocr_page in ocr:
        original = ocr_page_map[ocr_page.number - 1]
        by_number[original] = Page(
            number=original, text=ocr_page.text, confidence=ocr_page.confidence, ocr=True
        )
    return [by_number[n] for n in sorted(by_number)]
