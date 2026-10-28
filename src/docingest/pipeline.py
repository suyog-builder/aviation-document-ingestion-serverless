"""Stages shared by the fast and heavy lanes: malware gate, chunk, persist, enqueue for embedding."""

from __future__ import annotations

from aws_lambda_powertools import Logger

from docingest.chunking import chunk_document
from docingest.deps import Deps
from docingest.metadata import document_metadata
from docingest.models import Chunk, DocStatus, DocumentRef, EmbedMessage, Page

logger = Logger(child=True)

SCAN_TAG = "GuardDutyMalwareScanStatus"  # written by GuardDuty Malware Protection for S3


class ScanPending(Exception):
    """Malware scan has not finished yet; let SQS / Step Functions retry later."""


class ThreatFound(Exception):
    pass


def malware_gate(d: Deps, ref: DocumentRef, *, required: bool = True) -> None:
    """GuardDuty Malware Protection for S3 scans every new object and tags the result.

    NO_THREATS_FOUND -> continue; THREATS_FOUND -> quarantine; no tag yet -> retry later.
    Example: a partner feed ZIP renamed to .pdf with an embedded dropper is quarantined here and
    never reaches a parser.
    """
    status = d.s3.tags(ref.key, ref.version_id).get(SCAN_TAG)
    if status == "NO_THREATS_FOUND" or (status is None and not required):
        return
    if status is None:
        raise ScanPending(ref.doc_id)
    if status == "THREATS_FOUND":
        d.s3.quarantine(ref.key, ref.version_id, d.settings.quarantine_bucket)
        d.state.set_status(ref.doc_id, DocStatus.REJECTED, reason="malware")
        raise ThreatFound(ref.doc_id)
    # UNSUPPORTED / ACCESS_DENIED / FAILED: do not process unscanned content
    d.state.set_status(ref.doc_id, DocStatus.REJECTED, reason=f"scan status {status}")
    raise ThreatFound(f"{ref.doc_id}: {status}")


def is_hot(ref: DocumentRef) -> bool:
    """Current documents go to OpenSearch (hot) and S3 Vectors; archives only to S3 Vectors."""
    return not ref.source.startswith("archive")


def chunk_pages(d: Deps, ref: DocumentRef, pages: list[Page], *, chunk_offset: int = 0) -> list[Chunk]:
    s = d.settings
    full_text = "\n".join(p.text for p in pages[:20])
    filename = ref.key.rsplit("/", 1)[-1]
    doc_meta = {**document_metadata(filename, full_text), "source": ref.source}
    chunks = chunk_document(
        pages,
        doc_id=ref.doc_id,
        tenant_id=ref.tenant_id,
        doc_metadata=doc_meta,
        target_tokens=s.chunk_target_tokens,
        overlap_tokens=s.chunk_overlap_tokens,
    )
    if chunk_offset:
        chunks = [
            c.model_copy(
                update={
                    "chunk_no": c.chunk_no + chunk_offset,
                    "chunk_id": Chunk.make_id(ref.doc_id, c.chunk_no + chunk_offset),
                }
            )
            for c in chunks
        ]
    return chunks


def enqueue_embeddings(d: Deps, ref: DocumentRef, chunks: list[Chunk]) -> int:
    """Record the chunk total FIRST, then send messages, so the completion counter is never raced."""
    s = d.settings
    d.state.set_chunks(ref.doc_id, len(chunks))
    per_msg = s.chunks_per_embed_message
    batches = [chunks[i : i + per_msg] for i in range(0, len(chunks), per_msg)]
    entries = [
        {
            "Id": str(n),
            "MessageBody": EmbedMessage(
                doc_id=ref.doc_id,
                tenant_id=ref.tenant_id,
                total_chunks=len(chunks),
                batch_no=n,
                hot=is_hot(ref),
                chunks=batch,
            ).model_dump_json(),
        }
        for n, batch in enumerate(batches)
    ]
    for i in range(0, len(entries), 10):  # SendMessageBatch takes 10 entries
        resp = d.sqs.send_message_batch(QueueUrl=s.embed_queue_url, Entries=entries[i : i + 10])
        if resp.get("Failed"):
            raise RuntimeError(f"failed to enqueue {len(resp['Failed'])} embed batches for {ref.doc_id}")
    return len(batches)


def process_pages(d: Deps, ref: DocumentRef, pages: list[Page]) -> int:
    """Normalize + chunk + persist + enqueue. Returns the number of chunks."""
    chunks = chunk_pages(d, ref, pages)
    if not chunks:
        d.state.set_status(ref.doc_id, DocStatus.REJECTED, reason="no extractable text")
        return 0
    d.s3.put_pages(ref.tenant_id, ref.doc_id, pages)
    d.s3.put_chunks(ref.tenant_id, ref.doc_id, chunks)
    batches = enqueue_embeddings(d, ref, chunks)
    logger.info(
        "chunked",
        extra={"doc_id": ref.doc_id, "pages": len(pages), "chunks": len(chunks), "batches": batches},
    )
    return len(chunks)
