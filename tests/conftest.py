"""Shared fixtures: a full moto-backed AWS environment and fakes for Bedrock / vector stores."""

from __future__ import annotations

import io
import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("POWERTOOLS_TRACE_DISABLED", "1")
os.environ.setdefault("POWERTOOLS_METRICS_NAMESPACE", "DocIngestTest")

from docingest.config import Settings
from docingest.deps import Deps, set_deps
from docingest.embed import Embedding
from docingest.state import StateStore
from docingest.stores.s3store import MetadataStore, S3Store

SAMPLES = Path(__file__).parents[1] / "samples"


@dataclass
class FakeEmbedder:
    dims: int = 8
    calls: int = 0

    def embed_many(self, texts: list[str], threads: int = 8) -> list[Embedding]:
        self.calls += len(texts)
        return [Embedding(vector=[float(len(t) % 7)] * self.dims, tokens=len(t) // 4) for t in texts]


@dataclass
class FakeOpenSearch:
    docs: dict[str, dict] = field(default_factory=dict)
    fail_ids: set[str] = field(default_factory=set)

    def write(self, docs):
        failed = []
        for cid, doc in docs:
            if cid in self.fail_ids:
                failed.append(cid)
            else:
                self.docs[cid] = doc
        return failed


@dataclass
class FakeS3Vectors:
    vectors: dict[str, dict] = field(default_factory=dict)

    def write(self, vectors):
        for v in vectors:
            self.vectors[v["key"]] = v


class Ctx:
    function_name = "test"
    memory_limit_in_mb = 1024
    invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test"
    aws_request_id = "req-1"

    @staticmethod
    def get_remaining_time_in_millis() -> int:
        return 300_000


@pytest.fixture
def lambda_ctx():
    return Ctx()


@contextmanager
def local_env():
    """Buckets, queues, tables and a state machine in moto; yields the wired Deps."""
    with mock_aws():
        s = Settings(
            raw_bucket="raw",
            processed_bucket="processed",
            quarantine_bucket="quarantine",
            state_table="state",
            metadata_table="meta",
            kms_key_id="alias/aws/s3",
            embed_dimensions=8,
        )
        s3 = boto3.client("s3", region_name="us-east-1")
        for b in ("raw", "processed", "quarantine"):
            s3.create_bucket(Bucket=b)
        s3.put_bucket_versioning(Bucket="raw", VersioningConfiguration={"Status": "Enabled"})
        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        for t in ("state", "meta"):
            ddb.create_table(
                TableName=t,
                BillingMode="PAY_PER_REQUEST",
                KeySchema=[
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"},
                ],
            )
        sqs = boto3.client("sqs", region_name="us-east-1")
        s.fast_queue_url = sqs.create_queue(QueueName="fast")["QueueUrl"]
        s.embed_queue_url = sqs.create_queue(QueueName="embed")["QueueUrl"]
        sfn = boto3.client("stepfunctions", region_name="us-east-1")
        iam = boto3.client("iam", region_name="us-east-1")
        role = iam.create_role(RoleName="sfn", AssumeRolePolicyDocument="{}")["Role"]["Arn"]
        s.heavy_state_machine_arn = sfn.create_state_machine(
            name="heavy",
            roleArn=role,
            definition='{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}',
        )["stateMachineArn"]

        deps = Deps(
            settings=s,
            s3=S3Store("raw", "processed", s.kms_key_id, s3),
            state=StateStore("state", ddb),
            metadata=MetadataStore("meta", ddb),
            sqs=sqs,
            sfn=sfn,
            textract=MagicMock(),
            embedder=FakeEmbedder(),  # type: ignore[arg-type]
            opensearch=FakeOpenSearch(),
            s3vectors=FakeS3Vectors(),
        )
        set_deps(deps)
        try:
            yield deps
        finally:
            set_deps(None)


@pytest.fixture
def env():
    with local_env() as deps:
        yield deps


def put_raw(deps: Deps, key: str, data: bytes, *, scan: str | None = "NO_THREATS_FOUND") -> str | None:
    resp = deps.s3.client.put_object(Bucket="raw", Key=key, Body=data)
    if scan:
        deps.s3.client.put_object_tagging(
            Bucket="raw",
            Key=key,
            VersionId=resp.get("VersionId"),
            Tagging={"TagSet": [{"Key": "GuardDutyMalwareScanStatus", "Value": scan}]},
        )
    return resp.get("VersionId")


def s3_event(key: str, version_id: str | None = None, bucket: str = "raw") -> dict[str, Any]:
    from urllib.parse import quote_plus

    obj = {"key": quote_plus(key, safe="/"), "size": 1}
    if version_id:
        obj["versionId"] = version_id
    return {
        "Records": [{"eventName": "ObjectCreated:Put", "s3": {"bucket": {"name": bucket}, "object": obj}}]
    }


def sqs_event(*bodies: str) -> dict[str, Any]:
    return {
        "Records": [
            {
                "messageId": f"m{i}",
                "receiptHandle": "r",
                "body": b,
                "attributes": {},
                "messageAttributes": {},
                "md5OfBody": "",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:1:q",
                "awsRegion": "us-east-1",
            }
            for i, b in enumerate(bodies)
        ]
    }


def drain(deps: Deps, queue_url: str) -> list[str]:
    out = []
    while True:
        msgs = deps.sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10).get("Messages", [])
        if not msgs:
            return out
        for m in msgs:
            out.append(m["Body"])
            deps.sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])


def mixed_pdf(text_pages: int = 6, image_pages: int = 2) -> bytes:
    """A PDF where the last ``image_pages`` pages have no text layer (like scanned diagrams)."""
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    for i in range(text_pages):
        y = 800
        c.drawString(50, y, f"TASK 21-51-01-200-80{i} Pack Valve Check")
        for line in range(30):
            y -= 20
            c.drawString(
                50, y, f"({line + 1}) Check the pack flow control valve position indicator on panel {line}."
            )
        c.showPage()
    for _ in range(image_pages):
        c.rect(50, 100, 400, 600)
        c.showPage()
    c.save()
    return buf.getvalue()
