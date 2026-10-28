"""Normalise extracted text and split it into ~500-token, structure-aware chunks.

Why structure-aware rather than fixed-size windows:
* A maintenance task is a unit of meaning. Splitting "TASK 32-42-27-200-801" across two chunks
  means neither chunk answers "how do I check brake wear?" on its own.
* **Safety notices stay attached.** A WARNING/CAUTION is always kept in the same chunk as the
  step it protects. Retrieving a step without its warning is a safety problem, not a quality one.
* **Section breadcrumb prefix.** Each chunk starts with ``[AMM > TASK 32-42-27-200-801 ...]``
  so the embedding knows the context even when the chunk text says only "Measure dimension A".

Token counts are estimated (~4 characters per token for English technical text). Titan Text
Embeddings V2 accepts up to 8,192 tokens, so a 500-token target leaves a wide safety margin and
keeps chunks precise for retrieval.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass, field

from docingest.metadata import chunk_metadata
from docingest.models import Chunk, Page

# Hard headings start a new section (a chunk never spans two of them).
HARD_HEADING_RE = re.compile(
    r"^\s*(?:TASK\s+\d{2}-\d{2}-\d{2}-\d{3}-\d{3}.*|SUBTASK\s+\d{2}-\d{2}-\d{2}-\d{3}-\d{3}.*"
    r"|\d{2}-\d{2}-\d{2}\s+[A-Z].{3,80}|[A-Z][A-Z0-9 /,&()-]{6,80})\s*$"
)
# Soft headings ("1. Reason for the Job", "A. Fixtures") start a new paragraph and appear in the
# breadcrumb, but small neighbouring sub-sections are packed into the same chunk.
SOFT_HEADING_RE = re.compile(r"^\s*(?:\d{1,2}|[A-H])\.\s+[A-Z][A-Za-z /,&()-]{3,80}\s*$")
NOTICE_RE = re.compile(r"^\s*(WARNING|CAUTION|NOTE)\b", re.I)
PAGE_NO_RE = re.compile(r"^\s*(?:Page\s+)?\d{1,4}\s*(?:of\s+\d{1,4})?\s*$", re.I)
SENTENCE_RE = re.compile(r"(?<=[.!?;])\s+(?=[A-Z(])")


def estimate_tokens(text: str) -> int:
    return max(1, math.ceil(len(text) / 4))


# ---------------------------------------------------------------------------- normalise
def normalize_pages(pages: list[Page]) -> list[Page]:
    """Remove running headers/footers and page numbers, fix hyphenation, collapse spaces."""
    edge_lines: Counter[str] = Counter()
    for p in pages:
        lines = [ln.strip() for ln in p.text.splitlines() if ln.strip()]
        if len(lines) >= 6:  # short pages (scanned forms, slides) have no reliable header/footer zone
            edge_lines.update(ln for ln in set(lines[:2] + lines[-2:]) if len(ln) <= 80)
    threshold = max(3, int(len(pages) * 0.5))
    repeated = {ln for ln, n in edge_lines.items() if n >= threshold} if len(pages) >= 4 else set()

    out: list[Page] = []
    for p in pages:
        text = re.sub(r"(\w)-\n(\w)", r"\1\2", p.text)  # "inspec-\ntion" -> "inspection"
        kept = [
            re.sub(r"[ \t]+", " ", ln).strip()
            for ln in text.splitlines()
            if ln.strip() and ln.strip() not in repeated and not PAGE_NO_RE.match(ln)
        ]
        out.append(p.model_copy(update={"text": "\n".join(kept)}))
    return out


# ---------------------------------------------------------------------------- blocks
@dataclass
class Block:
    text: str
    page: int
    section: str | None
    is_notice: bool = False
    subsection: str | None = None


def to_blocks(pages: list[Page]) -> list[Block]:
    blocks: list[Block] = []
    section: str | None = None
    subsection: str | None = None

    def flush(para: list[str], page: int) -> None:
        if para:
            text = " ".join(para)
            blocks.append(
                Block(
                    text=text,
                    page=page,
                    section=section,
                    subsection=subsection,
                    is_notice=bool(NOTICE_RE.match(text)),
                )
            )
            para.clear()

    for p in pages:
        para: list[str] = []
        for line in p.text.splitlines():
            is_notice = bool(NOTICE_RE.match(line))
            if not is_notice and len(line) < 100 and HARD_HEADING_RE.match(line):
                flush(para, p.number)
                section, subsection = line.strip(), None
                continue
            if not is_notice and SOFT_HEADING_RE.match(line):
                flush(para, p.number)
                subsection = line.strip()
                para.append(line.strip())  # keep the sub-heading text inside the chunk
                continue
            if is_notice or re.match(r"^\s*(?:\(\d+\)|\([a-z]\))\s", line):
                flush(para, p.number)  # numbered steps and notices start a new paragraph
            para.append(line.strip())
        flush(para, p.number)
    return blocks


def _split_long(block: Block, target_tokens: int) -> list[Block]:
    if estimate_tokens(block.text) <= target_tokens:
        return [block]
    pieces, current = [], ""
    for sentence in SENTENCE_RE.split(block.text):
        if current and estimate_tokens(current + " " + sentence) > target_tokens:
            pieces.append(current)
            current = sentence
        else:
            current = f"{current} {sentence}".strip()
    if current:
        pieces.append(current)
    # A single sentence longer than the target is hard-wrapped by characters.
    final: list[str] = []
    for piece in pieces:
        limit = target_tokens * 4
        final.extend(piece[i : i + limit] for i in range(0, len(piece), limit))
    return [
        Block(
            text=t,
            page=block.page,
            section=block.section,
            is_notice=block.is_notice,
            subsection=block.subsection,
        )
        for t in final
    ]


# ---------------------------------------------------------------------------- chunks
@dataclass
class _Acc:
    blocks: list[Block] = field(default_factory=list)

    @property
    def tokens(self) -> int:
        return sum(estimate_tokens(b.text) for b in self.blocks)


def chunk_document(
    pages: list[Page],
    *,
    doc_id: str,
    tenant_id: str,
    doc_metadata: dict[str, str],
    target_tokens: int = 500,
    overlap_tokens: int = 50,
) -> list[Chunk]:
    blocks: list[Block] = []
    for b in to_blocks(normalize_pages(pages)):
        blocks.extend(_split_long(b, target_tokens))

    groups: list[list[Block]] = []
    acc = _Acc()
    for i, block in enumerate(blocks):
        new_section = acc.blocks and block.section != acc.blocks[-1].section
        too_big = acc.blocks and acc.tokens + estimate_tokens(block.text) > target_tokens
        # never end a chunk right after a WARNING/CAUTION: the protected step must follow it
        notice_open = acc.blocks and acc.blocks[-1].is_notice
        if (new_section or too_big) and not notice_open:
            groups.append(acc.blocks)
            acc = _Acc()
        acc.blocks.append(block)
        if i == len(blocks) - 1:
            groups.append(acc.blocks)

    chunks: list[Chunk] = []
    doc_type = doc_metadata.get("doc_type", "DOC")
    prev_tail = ""
    for n, group in enumerate(groups):
        section = group[0].section
        header = f"[{doc_type}{' > ' + section if section else ''}]"
        body = "\n".join(b.text for b in group)
        text = f"{header}\n{prev_tail}{body}".strip() if prev_tail else f"{header}\n{body}"
        meta = {**doc_metadata, **chunk_metadata(f"{section or ''}\n{body}")}
        chunks.append(
            Chunk(
                chunk_id=Chunk.make_id(doc_id, n),
                doc_id=doc_id,
                tenant_id=tenant_id,
                chunk_no=n,
                text=text,
                token_estimate=estimate_tokens(text),
                page_start=group[0].page,
                page_end=group[-1].page,
                section=section,
                metadata=meta,
            )
        )
        # Overlap only within the same section; carrying text across tasks would mix procedures.
        next_section = groups[n + 1][0].section if n + 1 < len(groups) and groups[n + 1] else None
        prev_tail = _tail(body, overlap_tokens) + "\n" if next_section == section and overlap_tokens else ""
    return chunks


def _tail(text: str, tokens: int) -> str:
    sentences = SENTENCE_RE.split(text)
    tail: list[str] = []
    while sentences and estimate_tokens(" ".join([sentences[-1], *tail])) <= tokens:
        tail.insert(0, sentences.pop())
    return " ".join(tail)
