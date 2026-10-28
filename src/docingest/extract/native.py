"""Native (no OCR) text extraction for the fast lane.

Returns a list of ``Page`` objects so page numbers survive into chunk metadata. A technician
who gets an answer from "AMM 32-42-27, page 7" can open the exact page.

* PDF  - pypdf, page by page; pages with too little text are flagged for selective OCR.
* DOCX - python-docx paragraphs and tables (tables flattened as ``cell | cell`` rows, which
         keeps torque tables and limits readable for the embedding model).
* HTML - BeautifulSoup text with scripts/styles removed.
* XML  - text nodes (S1000D data modules, iSpec 2200 exports); structure-aware parsing is a
         natural extension.
"""

from __future__ import annotations

import io

from docingest.classify import FileKind
from docingest.models import Page


def extract_pdf(data: bytes) -> list[Page]:
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(data))
    if reader.is_encrypted:
        reader.decrypt("")
    return [Page(number=i + 1, text=(p.extract_text() or "")) for i, p in enumerate(reader.pages)]


def extract_docx(data: bytes) -> list[Page]:
    import docx

    document = docx.Document(io.BytesIO(data))
    lines: list[str] = []
    for block in document.element.body.iterchildren():
        tag = block.tag.rsplit("}", 1)[-1]
        if tag == "p":
            text = "".join(node.text or "" for node in block.iter() if node.tag.endswith("}t"))
            if text.strip():
                lines.append(text)
        elif tag == "tbl":
            for row in block.iter():
                if row.tag.endswith("}tr"):
                    cells = []
                    for cell in row.iter():
                        if cell.tag.endswith("}tc"):
                            cells.append(" ".join(t.text or "" for t in cell.iter() if t.tag.endswith("}t")))
                    lines.append(" | ".join(c.strip() for c in cells))
    # DOCX has no fixed pages; one logical page keeps the interface uniform.
    return [Page(number=1, text="\n".join(lines))]


def extract_html(data: bytes) -> list[Page]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(data, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return [Page(number=1, text=soup.get_text("\n"))]


def extract_xml(data: bytes) -> list[Page]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(data, "html.parser")  # tolerant of SGML-ish legacy exports
    return [Page(number=1, text=soup.get_text("\n"))]


def extract_text(data: bytes) -> list[Page]:
    return [Page(number=1, text=data.decode("utf-8", errors="replace"))]


EXTRACTORS = {
    FileKind.PDF: extract_pdf,
    FileKind.DOCX: extract_docx,
    FileKind.HTML: extract_html,
    FileKind.XML: extract_xml,
    FileKind.TEXT: extract_text,
}


def extract(kind: FileKind, data: bytes) -> list[Page]:
    try:
        fn = EXTRACTORS[kind]
    except KeyError as exc:
        raise ValueError(f"no native extractor for {kind}") from exc
    return fn(data)
