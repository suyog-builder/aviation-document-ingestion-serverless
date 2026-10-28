"""End-to-end through the Lambdas against moto: router -> fast/heavy lane -> embed workers."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from docingest.handlers import embed_worker, fast_lane, heavy_lane, presign, router
from docingest.models import DocStatus, DocumentRef, EmbedMessage, Lane
from docingest.pipeline import ScanPending
from tests.conftest import SAMPLES, drain, mixed_pdf, put_raw, s3_event, sqs_event

UPLOAD = "u_" + "1" * 32
AMM_KEY = f"example-air/portal/2026/10/28/{UPLOAD}/AMM_32-42-27_brake_wear_check_rev61.pdf"
SCAN_KEY = "example-air/oem-feed/batch-1028/TaskCard_VT-EXA_C-check_scan.pdf"


def _route(env, key: str, data: bytes, **kw) -> dict:
    vid = put_raw(env, key, data, **kw)
    return router.handler(sqs_event(json.dumps(s3_event(key, vid))), lambda_ctx_obj())


def lambda_ctx_obj():
    from tests.conftest import Ctx

    return Ctx()


def _doc_id(key: str, vid: str | None = None) -> str:
    from docingest.keys import parse_key

    return parse_key("raw", key, vid).doc_id


# ------------------------------------------------------------------ state store
def test_claim_and_hash_dedupe_are_idempotent(env):
    assert env.state.claim("d1", "t", "k", 10) is True
    assert env.state.claim("d1", "t", "k", 10) is False
    assert env.state.register_hash("t", "abc", "d1") is None
    assert env.state.register_hash("t", "abc", "d1") is None  # same doc again is not a duplicate
    assert env.state.register_hash("t", "abc", "d2") == "d1"


def test_embed_counter_counts_each_batch_once(env):
    env.state.claim("d1", "t", "k", 10)
    env.state.set_chunks("d1", 20)
    assert env.state.add_embedded("d1", 0, 10) is False
    assert env.state.add_embedded("d1", 0, 10) is False  # SQS redelivery of batch 0
    assert env.state.get("d1")["embedded"] == 10
    assert env.state.add_embedded("d1", 1, 10) is True
    assert env.state.get("d1")["status"] == DocStatus.INDEXED


# ------------------------------------------------------------------ router
def test_router_sends_digital_pdf_to_fast_lane(env):
    resp = _route(env, AMM_KEY, (SAMPLES / "AMM_32-42-27_brake_wear_check_rev61.pdf").read_bytes())
    assert resp == {"batchItemFailures": []}
    msgs = drain(env, env.settings.fast_queue_url)
    ref = DocumentRef.model_validate_json(msgs[0])
    assert ref.lane is Lane.FAST and ref.doc_id == "1" * 32 and ref.page_count == 3
    assert env.state.get(ref.doc_id)["status"] == DocStatus.ROUTED


def test_router_starts_heavy_workflow_for_scans(env):
    _route(env, SCAN_KEY, (SAMPLES / "TaskCard_VT-EXA_C-check_scan.pdf").read_bytes())
    execs = env.sfn.list_executions(stateMachineArn=env.settings.heavy_state_machine_arn)["executions"]
    assert len(execs) == 1
    assert drain(env, env.settings.fast_queue_url) == []


def test_router_dedupes_identical_content_and_duplicate_events(env):
    data = (SAMPLES / "SB-A320-32-1471_summary.txt").read_bytes()
    key1 = "example-air/oem-feed/b1/SB-A320-32-1471.txt"
    key2 = "example-air/oem-feed/b2/SB-A320-32-1471-resend.txt"
    vid1 = put_raw(env, key1, data)
    event = sqs_event(json.dumps(s3_event(key1, vid1)))
    router.handler(event, lambda_ctx_obj())
    router.handler(event, lambda_ctx_obj())  # duplicate S3 notification
    vid2 = put_raw(env, key2, data)
    router.handler(sqs_event(json.dumps(s3_event(key2, vid2))), lambda_ctx_obj())

    assert len(drain(env, env.settings.fast_queue_url)) == 1
    dup = env.state.get(_doc_id(key2, vid2))
    assert dup["status"] == DocStatus.DUPLICATE and dup["duplicate_of"] == _doc_id(key1, vid1)


def test_router_rejects_disguised_files_and_ignores_test_events(env):
    key = "example-air/portal/x/invoice.pdf"
    vid = put_raw(env, key, b"PK\x03\x04 not really a pdf")
    router.handler(
        sqs_event(json.dumps(s3_event(key, vid)), json.dumps({"Event": "s3:TestEvent"})), lambda_ctx_obj()
    )
    assert env.state.get(_doc_id(key, vid))["status"] == DocStatus.REJECTED


# ------------------------------------------------------------------ fast lane
def _fast_ref(env) -> DocumentRef:
    _route(env, AMM_KEY, (SAMPLES / "AMM_32-42-27_brake_wear_check_rev61.pdf").read_bytes())
    return DocumentRef.model_validate_json(drain(env, env.settings.fast_queue_url)[0])


def test_fast_lane_chunks_persists_and_enqueues(env):
    ref = _fast_ref(env)
    assert fast_lane.process(ref) == "chunked"
    msgs = [EmbedMessage.model_validate_json(m) for m in drain(env, env.settings.embed_queue_url)]
    assert sum(len(m.chunks) for m in msgs) == 3 and msgs[0].total_chunks == 3
    obj = env.s3.client.get_object(Bucket="processed", Key=f"example-air/{ref.doc_id}/chunks.jsonl")
    assert len(obj["Body"].read().splitlines()) == 3
    assert env.state.get(ref.doc_id)["status"] == DocStatus.CHUNKED
    assert fast_lane.process(ref) == "already_processed"  # redelivery


def test_fast_lane_waits_for_malware_scan_and_quarantines_threats(env):
    ref = _fast_ref(env)
    env.s3.client.delete_object_tagging(Bucket="raw", Key=ref.key, VersionId=ref.version_id)
    with pytest.raises(ScanPending):
        fast_lane.process(ref)
    env.s3.client.put_object_tagging(
        Bucket="raw",
        Key=ref.key,
        VersionId=ref.version_id,
        Tagging={"TagSet": [{"Key": "GuardDutyMalwareScanStatus", "Value": "THREATS_FOUND"}]},
    )
    assert fast_lane.process(ref) == "rejected"
    assert env.s3.client.head_object(Bucket="quarantine", Key=ref.key)["ContentLength"] > 0
    assert env.state.get(ref.doc_id)["reason"] == "malware"


def test_fast_lane_promotes_image_heavy_pdf(env):
    key = f"example-air/portal/2026/10/28/u_{'2' * 32}/wiring.pdf"
    data = mixed_pdf(text_pages=2, image_pages=4)
    vid = put_raw(env, key, data)
    ref = DocumentRef(
        doc_id="2" * 32,
        tenant_id="example-air",
        source="portal",
        bucket="raw",
        key=key,
        version_id=vid,
        size=len(data),
        content_type="pdf",
        lane=Lane.FAST,
    )
    env.state.claim(ref.doc_id, "example-air", key, len(data))
    assert fast_lane.process(ref) == "promoted"


# ------------------------------------------------------------------ embed workers
def test_embed_workers_index_document_exactly_once(env):
    ref = _fast_ref(env)
    fast_lane.process(ref)
    bodies = drain(env, env.settings.embed_queue_url)
    resp = embed_worker.handler(sqs_event(*bodies, bodies[0]), lambda_ctx_obj())  # includes a redelivery
    assert resp == {"batchItemFailures": []}
    assert env.state.get(ref.doc_id)["status"] == DocStatus.INDEXED
    assert len(env.opensearch.docs) == 3 and len(env.s3vectors.vectors) == 3
    doc = env.opensearch.docs[f"{ref.doc_id}:00000"]
    assert doc["tenant_id"] == "example-air" and doc["doc_type"] == "AMM"
    assert doc["x-amz-bedrock-kb-source-uri"].endswith("AMM_32-42-27_brake_wear_check_rev61.pdf")
    meta = env.metadata._table.get_item(Key={"pk": "TENANT#example-air", "sk": f"DOC#{ref.doc_id}"})["Item"]
    assert meta["chunks"] == 3 and meta["acl_groups"] == ["example-air:all"]


def test_embed_worker_reports_partial_failure(env):
    env.settings.chunks_per_embed_message = 1  # 3 chunks -> 3 messages
    ref = _fast_ref(env)
    fast_lane.process(ref)
    bodies = drain(env, env.settings.embed_queue_url)
    env.opensearch.fail_ids.add(f"{ref.doc_id}:00001")
    resp = embed_worker.handler(sqs_event(*bodies), lambda_ctx_obj())
    failed = [f["itemIdentifier"] for f in resp["batchItemFailures"]]
    assert len(failed) == 1  # only the message holding chunk 1 is retried
    assert env.state.get(ref.doc_id)["status"] == DocStatus.CHUNKED  # not marked indexed


# ------------------------------------------------------------------ heavy lane
def _heavy_ref(env, data: bytes, name: str, did: str) -> DocumentRef:
    key = f"example-air/portal/2026/10/28/u_{did}/{name}"
    vid = put_raw(env, key, data)
    env.state.claim(did, "example-air", key, len(data))
    return DocumentRef(
        doc_id=did,
        tenant_id="example-air",
        source="portal",
        bucket="raw",
        key=key,
        version_id=vid,
        size=len(data),
        content_type="pdf",
        lane=Lane.HEAVY,
    )


def _textract_output(env, ref: DocumentRef, job_id: str, pages: int, text: str) -> None:
    blocks = [
        {"BlockType": "LINE", "Page": p, "Text": f"{text} page {p}", "Confidence": 95.0}
        for p in range(1, pages + 1)
    ]
    env.s3.client.put_object(
        Bucket="processed",
        Key=f"example-air/{ref.doc_id}/textract/{job_id}/1",
        Body=json.dumps({"Blocks": blocks}).encode(),
    )
    env.s3.client.put_object(
        Bucket="processed", Key=f"example-air/{ref.doc_id}/textract/{job_id}/.s3_access_check", Body=b""
    )


def test_heavy_lane_selective_ocr_end_to_end(env, lambda_ctx):
    ref = _heavy_ref(env, mixed_pdf(text_pages=6, image_pages=2), "pack_valve.pdf", "3" * 32)
    ev = {"doc": ref.model_dump(mode="json")}

    plan = heavy_lane.plan(ev, lambda_ctx)
    assert plan["mode"] == "ocr_selective"
    part = plan["ocr_parts"][0]
    assert part["page_map"] == [7, 8] and part["bucket"] == "processed"

    env.textract.start_document_text_detection.return_value = {"JobId": "job-1"}
    heavy_lane.start_ocr_part({**ev, "part": part, "task_token": "tok-1"}, lambda_ctx)
    assert env.state.get_job("job-1")["task_token"] == "tok-1"

    env.sfn = MagicMock()
    env.sfn.exceptions = MagicMock()
    sns = {"Records": [{"Sns": {"Message": json.dumps({"JobId": "job-1", "Status": "SUCCEEDED"})}}]}
    heavy_lane.textract_complete(sns, lambda_ctx)
    out = json.loads(env.sfn.send_task_success.call_args.kwargs["output"])
    assert out == {"job_id": "job-1", "part_no": 0, "status": "SUCCEEDED"}

    _textract_output(env, ref, "job-1", pages=2, text="Wiring diagram 21-51 annotated")
    collected = heavy_lane.collect({**ev, "plan": plan, "ocr": [out]}, lambda_ctx)
    assert collected["page_count"] == 8 and collected["ranges"] == [{"index": 0, "start": 1, "end": 8}]
    pages = heavy_lane._read_pages(env, ref)
    assert pages[6].ocr and pages[6].text.startswith("Wiring diagram") and not pages[0].ocr

    for r in collected["ranges"]:
        assert heavy_lane.chunk_range({**ev, "range": r}, lambda_ctx)["chunks"] > 0
    result = heavy_lane.finalize(ev, lambda_ctx)
    msgs = drain(env, env.settings.embed_queue_url)
    assert result["chunks"] == sum(len(EmbedMessage.model_validate_json(m).chunks) for m in msgs)


def test_heavy_lane_scan_goes_full_ocr_and_native_big_pdf_skips_ocr(env, lambda_ctx):
    scan = _heavy_ref(env, (SAMPLES / "TaskCard_VT-EXA_C-check_scan.pdf").read_bytes(), "scan.pdf", "4" * 32)
    assert heavy_lane.plan({"doc": scan.model_dump(mode="json")}, lambda_ctx)["mode"] == "ocr_full"
    big = _heavy_ref(env, mixed_pdf(text_pages=5, image_pages=0), "ipc.pdf", "5" * 32)
    assert heavy_lane.plan({"doc": big.model_dump(mode="json")}, lambda_ctx) == {
        "mode": "native",
        "ocr_parts": [],
    }


def test_textract_quota_maps_to_retryable_error(env, lambda_ctx):
    ref = _heavy_ref(env, b"%PDF-1.4", "x.pdf", "6" * 32)

    class LimitExceeded(Exception):
        pass

    env.textract.exceptions.LimitExceededException = LimitExceeded
    env.textract.start_document_text_detection.side_effect = LimitExceeded()
    part = {"part_no": 0, "bucket": "raw", "key": ref.key, "version_id": None, "page_map": None}
    with pytest.raises(heavy_lane.TextractBusy):
        heavy_lane.start_ocr_part(
            {"doc": ref.model_dump(mode="json"), "part": part, "task_token": "t"}, lambda_ctx
        )


def test_state_machine_definition():
    asl = json.loads((Path(__file__).parents[2] / "statemachine/heavy_lane.asl.json").read_text())
    assert asl["StartAt"] == "Plan"
    assert asl["States"]["ChunkRanges"]["ItemProcessor"]["ProcessorConfig"]["Mode"] == "DISTRIBUTED"
    retry = asl["States"]["OcrParts"]["ItemProcessor"]["States"]["StartOcr"]["Retry"][0]
    assert retry["ErrorEquals"] == ["TextractBusy"]


# ------------------------------------------------------------------ presign
def test_presign_scopes_upload_to_tenant(env, lambda_ctx):
    event = {
        "requestContext": {
            "authorizer": {"jwt": {"claims": {"sub": "u-77", "custom:tenant_id": "example-air"}}}
        },
        "body": json.dumps(
            {"filename": "AMM 32-42-27.pdf", "content_type": "application/pdf", "size": 4_194_304}
        ),
    }
    resp = presign.handler(event, lambda_ctx)
    body = json.loads(resp["body"])
    assert resp["statusCode"] == 200 and body["key"].startswith("example-air/portal/")
    assert body["fields"]["x-amz-server-side-encryption"] == "aws:kms"

    bad = {
        **event,
        "body": json.dumps({"filename": "a.exe", "content_type": "application/x-msdownload", "size": 10}),
    }
    assert presign.handler(bad, lambda_ctx)["statusCode"] == 415
