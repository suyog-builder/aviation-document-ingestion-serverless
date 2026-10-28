"""Push the four synthetic samples through the real handlers, locally, with no AWS account.

S3, SQS, DynamoDB and Step Functions are emulated with moto; Bedrock embeddings, OpenSearch and
S3 Vectors are in-memory fakes; Textract output for the scanned task card is simulated. Every
handler, state transition and message is the production code.

    python -m samples.run_local_pipeline
"""

from __future__ import annotations

import json
from collections import Counter

from docingest.handlers import embed_worker, fast_lane, heavy_lane, router
from docingest.models import DocumentRef, EmbedMessage
from tests.conftest import SAMPLES, Ctx, drain, local_env, put_raw, s3_event, sqs_event

FILES = [
    (
        "example-air/portal/2026/10/28/u_" + "a" * 32 + "/AMM_32-42-27_brake_wear_check_rev61.pdf",
        "AMM_32-42-27_brake_wear_check_rev61.pdf",
    ),
    (
        "example-air/qa-portal/2026/10/28/u_" + "b" * 32 + "/SOP_cold_weather_operations.docx",
        "SOP_cold_weather_operations.docx",
    ),
    ("example-air/oem-feed/batch-1028/SB-A320-32-1471_summary.txt", "SB-A320-32-1471_summary.txt"),
    (
        "example-air/hangar-scans/2026/10/28/u_" + "c" * 32 + "/TaskCard_VT-EXA_C-check_scan.pdf",
        "TaskCard_VT-EXA_C-check_scan.pdf",
    ),
]


def run(env) -> dict[str, str]:
    ctx = Ctx()
    print("1) upload to raw bucket + S3 event -> ingest queue -> router")
    for key, name in FILES:
        vid = put_raw(env, key, (SAMPLES / name).read_bytes())
        router.handler(sqs_event(json.dumps(s3_event(key, vid))), ctx)

    print("2) fast lane")
    for body in drain(env, env.settings.fast_queue_url):
        ref = DocumentRef.model_validate_json(body)
        print(f"   {ref.key.rsplit('/', 1)[-1]:<45} -> {fast_lane.process(ref)}")

    print("3) heavy lane (scanned task card, simulated Textract)")
    execs = env.sfn.list_executions(stateMachineArn=env.settings.heavy_state_machine_arn)["executions"]
    for ex in execs:
        doc = json.loads(env.sfn.describe_execution(executionArn=ex["executionArn"])["input"])["doc"]
        ref = DocumentRef.model_validate(doc)
        plan = heavy_lane.plan({"doc": doc}, ctx)
        cards = [
            (
                "TASK 32-42-27-200-801 Brake wear check",
                "Brake 3 wear pin flush with housing - replace brake.",
            ),
            ("TASK 32-41-00-210-801 Wheel inspection", "Wheel 2 tyre cut 4 mm - within limits, monitor."),
            (
                "TASK 21-51-01-710-801 Pack operational test",
                "Pack 1 FCV fault reproduced - MEL 21-51-01 applied.",
            ),
            ("TASK 05-51-11-200-801 Hard landing check", "Not applicable - no hard landing report."),
        ]
        blocks = [
            {"BlockType": "LINE", "Page": p, "Text": t, "Confidence": 94.0}
            for p, (task, finding) in enumerate(cards, start=1)
            for t in (f"WORK CARD 04{p:02d} - VT-EXA C-CHECK", task, finding, f"Inspector stamp 44{p:02d}")
        ]
        env.s3.client.put_object(
            Bucket="processed",
            Key=f"{ref.tenant_id}/{ref.doc_id}/textract/job-1/1",
            Body=json.dumps({"Blocks": blocks}).encode(),
        )
        collected = heavy_lane.collect(
            {"doc": doc, "plan": plan, "ocr": [{"job_id": "job-1", "part_no": 0}]}, ctx
        )
        for r in collected["ranges"]:
            heavy_lane.chunk_range({"doc": doc, "range": r}, ctx)
        result = heavy_lane.finalize({"doc": doc}, ctx)
        print(
            f"   {ref.key.rsplit('/', 1)[-1]:<45} -> mode={plan['mode']} pages={collected['page_count']} "
            f"chunks={result['chunks']}"
        )

    print("4) embed workers -> Titan V2 (fake) -> OpenSearch + S3 Vectors + metadata")
    bodies = drain(env, env.settings.embed_queue_url)
    embed_worker.handler(sqs_event(*bodies), ctx)
    per_doc = Counter(EmbedMessage.model_validate_json(b).doc_id for b in bodies)

    statuses = {}
    for key, name in FILES:
        from docingest.keys import parse_key

        doc_id = parse_key("raw", key).doc_id if "/u_" in key else None
        item = env.state.get(doc_id) if doc_id else None
        if item is None:  # SFTP drop: id derives from the version id; look it up by key
            item = next((i for i in env.state._table.scan()["Items"] if i.get("s3_key") == key), {})
        statuses[name] = f"{item.get('status')} chunks={item.get('chunks')}"
        print(f"   {name:<45} {statuses[name]}")
    print(
        f"   OpenSearch docs: {len(env.opensearch.docs)}, S3 Vectors: {len(env.s3vectors.vectors)}, "
        f"embed messages: {sum(per_doc.values())}"
    )
    sample = next(iter(env.opensearch.docs.values()))
    print(
        "   example indexed chunk:", json.dumps({k: v for k, v in sample.items() if "vector" not in k})[:400]
    )
    return statuses


def main() -> None:
    with local_env() as env:
        run(env)


if __name__ == "__main__":
    main()
