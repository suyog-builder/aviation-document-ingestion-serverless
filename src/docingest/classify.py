"""Decide what a file is and which lane processes it.

Fast lane (~70%): born-digital files we can parse in one Lambda in seconds -
    PDFs with a text layer, DOCX, HTML, XML, TXT/MD, CSV, up to 20 MB and 300 pages.
Heavy lane (~30%): anything that needs OCR or is too big for one Lambda -
    scanned PDFs (no text layer), TIFF/PNG/JPEG task card scans, PDFs > 20 MB or > 300 pages.

Detection uses magic bytes, never only the file extension or the declared Content-Type
(uploaders lie: a ".pdf" that is really a ZIP is rejected).

Example decisions:
    AMM_32-42-27_rev61.pdf  4.1 MB, 38 pages, 2,100 chars/page   -> FAST
    TaskCard_VT-EXA_0412.pdf  scanned, 0 chars on sampled pages   -> HEAVY (OCR)
    A320_IPC_full.pdf       410 MB, 9,800 pages                  -> HEAVY (Distributed Map)
    logbook_page.tiff                                           -> HEAVY (OCR)
"""

from __future__ import annotations

import io
from dataclasses import dataclass
from enum import StrEnum

from docingest.models import Lane


class FileKind(StrEnum):
    PDF = "pdf"
    DOCX = "docx"
    HTML = "html"
    XML = "xml"
    TEXT = "text"
    IMAGE = "image"
    UNSUPPORTED = "unsupported"


_TEXT_EXT = {".txt", ".md", ".csv"}


def sniff(head: bytes, filename: str) -> FileKind:
    """Identify the file from its first bytes (read with a ranged GET)."""
    name = filename.lower()
    if head.startswith(b"%PDF-"):
        return FileKind.PDF
    if head.startswith((b"II*\x00", b"MM\x00*", b"\x89PNG", b"\xff\xd8\xff")):
        return FileKind.IMAGE
    if head.startswith(b"PK\x03\x04"):
        return FileKind.DOCX if name.endswith(".docx") else FileKind.UNSUPPORTED
    stripped = head.lstrip().lower()
    if stripped.startswith((b"<!doctype html", b"<html")):
        return FileKind.HTML
    if stripped.startswith(b"<?xml"):
        return FileKind.XML  # e.g. S1000D data modules, ATA iSpec 2200 SGML/XML exports
    if any(name.endswith(ext) for ext in _TEXT_EXT):
        try:
            head.decode("utf-8")
            return FileKind.TEXT
        except UnicodeDecodeError:
            return FileKind.UNSUPPORTED
    return FileKind.UNSUPPORTED


@dataclass(frozen=True)
class PdfProbe:
    pages: int
    text_chars_sampled: int
    sampled_pages: int
    encrypted: bool

    @property
    def has_text_layer(self) -> bool:
        return self.sampled_pages > 0 and self.text_chars_sampled / self.sampled_pages >= 1

    def chars_per_page(self) -> float:
        return self.text_chars_sampled / self.sampled_pages if self.sampled_pages else 0.0


def probe_pdf(data: bytes, sample_pages: int = 3) -> PdfProbe:
    """Count pages and measure the text layer on first, middle and last pages."""
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(data))
    encrypted = reader.is_encrypted
    if encrypted:
        try:
            reader.decrypt("")  # many manuals are "encrypted" with an empty user password
        except Exception:
            return PdfProbe(pages=0, text_chars_sampled=0, sampled_pages=0, encrypted=True)
    n = len(reader.pages)
    idx = sorted({0, n // 2, n - 1})[:sample_pages] if n else []
    chars = sum(len((reader.pages[i].extract_text() or "").strip()) for i in idx)
    return PdfProbe(pages=n, text_chars_sampled=chars, sampled_pages=len(idx), encrypted=encrypted)


@dataclass(frozen=True)
class RouteDecision:
    lane: Lane | None
    kind: FileKind
    reason: str
    page_count: int | None = None


def decide(
    kind: FileKind,
    size: int,
    *,
    max_fast_bytes: int,
    max_fast_pages: int,
    min_chars_per_page: int,
    pdf: PdfProbe | None = None,
) -> RouteDecision:
    if kind is FileKind.UNSUPPORTED:
        return RouteDecision(None, kind, "unsupported file type")
    if kind is FileKind.IMAGE:
        return RouteDecision(Lane.HEAVY, kind, "image needs OCR", page_count=1)
    if size > max_fast_bytes:
        return RouteDecision(Lane.HEAVY, kind, f"large file {size / 1e6:.0f} MB", pdf.pages if pdf else None)
    if kind is FileKind.PDF:
        if pdf is None:
            return RouteDecision(Lane.HEAVY, kind, "pdf not probed")
        if pdf.encrypted and pdf.pages == 0:
            return RouteDecision(None, kind, "password-protected pdf")
        if pdf.pages > max_fast_pages:
            return RouteDecision(Lane.HEAVY, kind, f"{pdf.pages} pages", pdf.pages)
        if pdf.chars_per_page() < min_chars_per_page:
            return RouteDecision(Lane.HEAVY, kind, "no usable text layer (scanned)", pdf.pages)
        return RouteDecision(Lane.FAST, kind, "pdf with text layer", pdf.pages)
    return RouteDecision(Lane.FAST, kind, f"native {kind.value}")
