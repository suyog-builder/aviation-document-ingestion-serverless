"""Aviation metadata extraction.

Metadata does two jobs:
1. **Filtering** at query time (tenant isolation, fleet type, document type, ATA chapter).
2. **Precision**: exact identifiers (ATA references, AMM task numbers, part numbers, SB/AD
   numbers, registrations) are stored as fields so hybrid search and filters can match them
   exactly - semantic embeddings are weak on strings like ``D23189000-9``.

Example: from "SB A320-32-1471 Rev 02 - Main landing gear brake wear indicator" we get
    doc_type=SB, ata_chapter=32, sb_number=A320-32-1471, revision=02, fleet_type=A320 family.
"""

from __future__ import annotations

import re
from collections import Counter

from docingest.models import DocType

ATA_REF_RE = re.compile(r"\b(\d{2})-(\d{2})-(\d{2})(?:-(\d{3})-(\d{3}))?\b")
TASK_RE = re.compile(r"\bTASK\s+(\d{2}-\d{2}-\d{2}-\d{3}-\d{3}(?:-[A-Z0-9]+)?)\b", re.I)
SB_RE = re.compile(r"\b(?:SB|Service Bulletin)\s*[:#]?\s*([A-Z0-9]{2,6}-\d{2}-\d{3,4})\b", re.I)
AD_RE = re.compile(r"\b(?:AD|Airworthiness Directive)\s*[:#]?\s*(\d{4}-\d{2,4}(?:-\d{1,3})?(?:R\d)?)\b", re.I)
MEL_RE = re.compile(r"\bMEL\b.{0,20}?\b(\d{2}-\d{2}-\d{2})\b", re.I)
PART_RE = re.compile(r"\b(?:P/N|PN|Part No\.?)\s*[:#]?\s*([A-Z0-9][A-Z0-9-]{4,24})\b", re.I)
REG_RE = re.compile(r"\b(?:VT|N|G|D|F|9V|A6|EI|C)-[A-Z0-9]{3,5}\b")
REV_RE = re.compile(r"\b(?:Rev(?:ision)?\.?)\s*[:#]?\s*([0-9]{1,3}[A-Z]?)\b", re.I)
FLEET_HINTS = {
    "A20N": ("A320NEO", "A320 NEO", "A20N"),
    "A320": ("A320", "A319", "A321"),
    "B38M": ("737 MAX", "B737-8", "B38M"),
    "B738": ("737-800", "B737NG", "B738"),
    "AT76": ("ATR 72", "ATR72", "AT76"),
}

_DOC_TYPE_HINTS: list[tuple[DocType, tuple[str, ...]]] = [
    (DocType.AD, ("airworthiness directive",)),
    (DocType.SB, ("service bulletin",)),
    (DocType.MEL, ("minimum equipment list", " mel ")),
    (DocType.IPC, ("illustrated parts", " ipc ")),
    (DocType.TASK_CARD, ("task card", "sign-off", "work card")),
    (DocType.AMM, ("aircraft maintenance manual", " amm ", "task 0", "task 1", "task 2", "task 3")),
    (DocType.SOP, ("standard operating procedure", " sop ")),
    (DocType.TRAINING, ("training", "course")),
]


def guess_doc_type(filename: str, text: str) -> DocType:
    sample = f" {filename.replace('_', ' ')} {text[:4000]} ".lower()
    for doc_type, hints in _DOC_TYPE_HINTS:
        if any(h in sample for h in hints):
            return doc_type
    return DocType.OTHER


def guess_fleet(text: str) -> str:
    up = text[:20000].upper()
    for fleet, hints in FLEET_HINTS.items():
        if any(h in up for h in hints):
            return fleet
    return "ALL"


def document_metadata(filename: str, text: str) -> dict[str, str]:
    """Document-level fields (same for every chunk)."""
    meta: dict[str, str] = {
        "doc_type": guess_doc_type(filename, text).value,
        "fleet_type": guess_fleet(f"{filename} {text}"),
    }
    if m := REV_RE.search(f"{filename} {text[:3000]}"):
        meta["revision"] = m.group(1)
    if m := SB_RE.search(text[:3000]):
        meta["sb_number"] = m.group(1).upper()
    if m := AD_RE.search(text[:3000]):
        meta["ad_number"] = m.group(1).upper()
    chapters = Counter(m.group(1) for m in ATA_REF_RE.finditer(text[:50000]))
    if chapters:
        meta["ata_chapter"] = chapters.most_common(1)[0][0]
    return meta


def chunk_metadata(text: str) -> dict[str, list[str] | str]:
    """Chunk-level identifiers for exact-match filters and keyword search."""
    out: dict[str, list[str] | str] = {}
    refs = sorted({m.group(0) for m in ATA_REF_RE.finditer(text)})[:10]
    if refs:
        out["ata_refs"] = refs
        out["ata_chapter"] = refs[0][:2]
    if tasks := sorted({m.group(1).upper() for m in TASK_RE.finditer(text)})[:10]:
        out["task_refs"] = tasks
    if parts := sorted({m.group(1).upper() for m in PART_RE.finditer(text)})[:20]:
        out["part_numbers"] = parts
    if mel := sorted({m.group(1) for m in MEL_RE.finditer(text)})[:10]:
        out["mel_items"] = mel
    if regs := sorted(set(REG_RE.findall(text)))[:10]:
        out["registrations"] = regs
    return out
