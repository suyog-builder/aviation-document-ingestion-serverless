"""Keys, classification, chunking, metadata, Textract parsing, embeddings."""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime

import boto3
import pytest
from botocore.response import StreamingBody
from botocore.stub import Stubber

from docingest.chunking import chunk_document, estimate_tokens, normalize_pages
from docingest.classify import FileKind, decide, probe_pdf, sniff
from docingest.embed import EmbeddingThrottled, TitanEmbedder
from docingest.extract.native import extract
from docingest.extract.textract import cut_pages, low_text_pages, merge_pages, pages_from_blocks
from docingest.keys import decode_event_key, parse_key, safe_filename, upload_key
from docingest.metadata import chunk_metadata, document_metadata
from docingest.models import Lane, Page
from tests.conftest import SAMPLES, mixed_pdf

LIMITS = {"max_fast_bytes": 20_000_000, "max_fast_pages": 300, "min_chars_per_page": 200}


# ------------------------------------------------------------------ keys
def test_upload_key_layout_and_parse_roundtrip():
    key = upload_key(
        "example-air",
        "portal",
        "u_" + "a" * 32,
        "AMM 32-42-27 rev61.pdf",
        now=datetime(2026, 10, 28, tzinfo=UTC),
    )
    assert key == f"example-air/portal/2026/10/28/u_{'a' * 32}/AMM_32-42-27_rev61.pdf"
    parsed = parse_key("raw", key)
    assert (parsed.tenant_id, parsed.source, parsed.doc_id) == ("example-air", "portal", "a" * 32)


def test_sftp_drop_gets_stable_doc_id_per_version():
    a = parse_key("raw", "example-air/oem-feed/batch-1028/SB-A320-32-1471.pdf", "v1")
    b = parse_key("raw", "example-air/oem-feed/batch-1028/SB-A320-32-1471.pdf", "v2")
    assert a.doc_id != b.doc_id and len(a.doc_id) == 32


def test_event_key_decoding_and_safe_names():
    assert (
        decode_event_key("example-air/oem-feed/x/Task+Card%28v2%29.pdf")
        == "example-air/oem-feed/x/Task Card(v2).pdf"
    )
    assert safe_filename("../../etc/pässwd .pdf") == "passwd_.pdf"


# ------------------------------------------------------------------ classification
@pytest.mark.parametrize(
    ("name", "lane"),
    [
        ("AMM_32-42-27_brake_wear_check_rev61.pdf", Lane.FAST),
        ("TaskCard_VT-EXA_C-check_scan.pdf", Lane.HEAVY),
        ("SOP_cold_weather_operations.docx", Lane.FAST),
        ("SB-A320-32-1471_summary.txt", Lane.FAST),
    ],
)
def test_samples_route_to_expected_lane(name, lane):
    data = (SAMPLES / name).read_bytes()
    kind = sniff(data[:8192], name)
    pdf = probe_pdf(data) if kind is FileKind.PDF else None
    assert decide(kind, len(data), pdf=pdf, **LIMITS).lane is lane


def test_renamed_zip_is_rejected_and_big_or_image_files_go_heavy():
    assert decide(sniff(b"PK\x03\x04....", "fake.pdf"), 10, **LIMITS).lane is None
    assert decide(FileKind.IMAGE, 10, **LIMITS).lane is Lane.HEAVY
    assert decide(FileKind.DOCX, 50_000_000, **LIMITS).lane is Lane.HEAVY


# ------------------------------------------------------------------ chunking
def _amm_chunks():
    data = (SAMPLES / "AMM_32-42-27_brake_wear_check_rev61.pdf").read_bytes()
    pages = extract(FileKind.PDF, data)
    meta = document_metadata("AMM_32-42-27_rev61.pdf", "\n".join(p.text for p in pages))
    return meta, chunk_document(pages, doc_id="doc1", tenant_id="example-air", doc_metadata=meta)


def test_amm_chunks_are_task_aligned_with_metadata():
    meta, chunks = _amm_chunks()
    assert meta["doc_type"] == "AMM" and meta["revision"] == "61" and meta["ata_chapter"] == "32"
    assert len(chunks) == 3  # one per task (3 synthetic task variants)
    first = chunks[0]
    assert first.chunk_id == "doc1:00000"
    assert first.text.startswith("[AMM > TASK 32-42-27-200-801-A Brake Wear Check]")
    assert "32-42-27-200-801-A" in first.metadata["task_refs"]
    assert "98D32103500-000" in first.metadata["part_numbers"]


def test_warnings_stay_with_the_step_they_protect():
    _, chunks = _amm_chunks()
    for c in chunks:
        if "WARNING" in c.text:
            assert "Apply the parking brake" in c.text


def test_long_sections_are_split_under_target_with_overlap():
    body = " ".join(
        f"Step {i}. Inspect the torque link bushing {i} for wear and corrosion." for i in range(400)
    )
    pages = [Page(number=1, text=f"TASK 32-11-00-200-801 Torque Link Inspection\n{body}")]
    chunks = chunk_document(
        pages,
        doc_id="d",
        tenant_id="t",
        doc_metadata={"doc_type": "AMM"},
        target_tokens=300,
        overlap_tokens=40,
    )
    assert len(chunks) > 5
    assert all(c.token_estimate <= 300 + 40 + 30 for c in chunks)  # target + overlap + breadcrumb
    tail_of_first = chunks[0].text.split()[-3:]
    assert " ".join(tail_of_first) in chunks[1].text  # overlap carried into the next chunk


def test_running_headers_and_page_numbers_removed():
    pages = [
        Page(
            number=i,
            text="EXAMPLE AIR - CONTROLLED COPY\n"
            + "\n".join(f"Step {i}.{n}: check item {n}" for n in range(6))
            + f"\nPage {i} of 6",
        )
        for i in range(1, 7)
    ]
    out = normalize_pages(pages)
    assert all("CONTROLLED COPY" not in p.text and "Page" not in p.text for p in out)


def test_token_estimate():
    assert estimate_tokens("a" * 2000) == 500


# ------------------------------------------------------------------ metadata
def test_bulletin_and_directive_identifiers():
    text = (
        "Airworthiness Directive AD 2026-0142 requires SB A320-32-1471. "
        "P/N C20195000-2 on VT-EXA. MEL item 21-51-01."
    )
    m = chunk_metadata(text)
    assert (
        m["part_numbers"] == ["C20195000-2"]
        and m["registrations"] == ["VT-EXA"]
        and m["mel_items"] == ["21-51-01"]
    )
    d = document_metadata("ad.pdf", text)
    assert d["doc_type"] == "AD" and d["ad_number"] == "2026-0142" and d["sb_number"] == "A320-32-1471"


# ------------------------------------------------------------------ textract helpers
def test_textract_blocks_to_pages_and_selective_merge():
    blocks = [
        {"BlockType": "PAGE", "Page": 1},
        {"BlockType": "LINE", "Page": 1, "Text": "WORK CARD 0412 VT-EXA", "Confidence": 99.1},
        {"BlockType": "LINE", "Page": 1, "Text": "Brake 3 wear pin flush", "Confidence": 87.3},
        {"BlockType": "LINE", "Page": 2, "Text": "Signed: inspector stamp", "Confidence": 62.0},
    ]
    ocr = pages_from_blocks(blocks)
    assert [p.number for p in ocr] == [1, 2] and ocr[0].confidence == 93.2 and ocr[1].ocr
    native = [Page(number=i, text="text " * 100 if i not in (3, 5) else "") for i in range(1, 7)]
    assert low_text_pages(native, 200) == [3, 5]
    merged = merge_pages(native, ocr, ocr_page_map=[3, 5])
    assert merged[2].text.startswith("WORK CARD") and merged[4].ocr and len(merged) == 6


def test_cut_pages_builds_small_pdf():
    data = mixed_pdf(text_pages=3, image_pages=2)
    small = cut_pages(data, [4, 5])
    assert probe_pdf(small).pages == 2


# ------------------------------------------------------------------ embeddings
def _body(payload: dict) -> StreamingBody:
    raw = json.dumps(payload).encode()
    return StreamingBody(io.BytesIO(raw), len(raw))


def test_titan_embedder_request_and_throttle_mapping():
    client = boto3.client("bedrock-runtime", region_name="us-east-1")
    stub = Stubber(client)
    stub.add_response(
        "invoke_model",
        {
            "body": _body({"embedding": [0.1] * 4, "inputTextTokenCount": 9}),
            "contentType": "application/json",
        },
        {
            "modelId": "amazon.titan-embed-text-v2:0",
            "contentType": "application/json",
            "accept": "application/json",
            "body": json.dumps({"inputText": "brake wear", "dimensions": 4, "normalize": True}),
        },
    )
    stub.add_client_error("invoke_model", service_error_code="ThrottlingException", http_status_code=429)
    emb = TitanEmbedder("amazon.titan-embed-text-v2:0", 4, "us-east-1", client=client)
    with stub:
        out = emb.embed("brake wear")
        assert out.tokens == 9 and len(out.vector) == 4
        with pytest.raises(EmbeddingThrottled):
            emb.embed("again")
