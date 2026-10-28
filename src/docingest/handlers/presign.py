"""Upload entry point: API Gateway (HTTP API + JWT authorizer) -> presigned S3 POST.

Clients never stream file bytes through API Gateway or Lambda (10 MB payload limit, and you pay
for Lambda time spent copying bytes). They ask for a short-lived, tightly-scoped presigned POST
and upload straight to S3.

Policy conditions enforced *by S3* (not by trust in the client):
* exact key under the caller's tenant prefix (tenant comes from the JWT, never the request body),
* ``content-length-range`` (1 byte .. 500 MB),
* declared ``Content-Type`` must match an allow-list,
* server-side encryption with the pipeline's KMS key.

Example request (technical publications engineer uploading a revised AMM task):
    POST /uploads  {"filename": "AMM 32-42-27 rev61.pdf", "content_type": "application/pdf", "size": 4194304}
Response:
    {"upload_id": "u_3f9c...", "key": "example-air/portal/2026/10/28/u_3f9c.../AMM_32-42-27_rev61.pdf",
     "url": "https://example-air-docs-raw-prd.s3.amazonaws.com/", "fields": {...}, "expires_in": 900}

The client retries the S3 POST up to 3 times; the upload id makes retries land on the same key.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from pydantic import BaseModel, Field, ValidationError

from docingest.deps import get_deps
from docingest.keys import upload_key

logger = Logger(service="docingest-presign")
metrics = Metrics(namespace="DocIngest", service="presign")

ALLOWED_TYPES = {
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "text/html",
    "text/plain",
    "text/markdown",
    "text/csv",
    "application/xml",
    "image/tiff",
    "image/png",
    "image/jpeg",
}
EXPIRES_S = 900


class UploadRequest(BaseModel):
    filename: str = Field(min_length=1, max_length=255)
    content_type: str
    size: int = Field(gt=0)
    source: str = Field(default="portal", pattern=r"^[a-z0-9-]{2,30}$")


def _response(status: int, body: dict[str, Any]) -> dict[str, Any]:
    return {"statusCode": status, "headers": {"Content-Type": "application/json"}, "body": json.dumps(body)}


@logger.inject_lambda_context
@metrics.log_metrics
def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    d = get_deps()
    claims = event["requestContext"]["authorizer"]["jwt"]["claims"]
    tenant_id = claims.get("custom:tenant_id")
    if not tenant_id:
        return _response(403, {"error": "no tenant on token"})
    try:
        req = UploadRequest.model_validate_json(event.get("body") or "{}")
    except ValidationError as exc:
        return _response(400, {"error": "invalid request", "details": exc.errors(include_url=False)})
    if req.content_type not in ALLOWED_TYPES:
        return _response(415, {"error": f"unsupported content type {req.content_type}"})
    if req.size > d.settings.max_upload_bytes:
        return _response(413, {"error": "file too large"})

    upload_id = f"u_{uuid.uuid4().hex}"
    key = upload_key(tenant_id, req.source, upload_id, req.filename)
    post = d.s3.client.generate_presigned_post(
        Bucket=d.settings.raw_bucket,
        Key=key,
        Fields={
            "Content-Type": req.content_type,
            "x-amz-server-side-encryption": "aws:kms",
            "x-amz-server-side-encryption-aws-kms-key-id": d.settings.kms_key_id,
            "x-amz-meta-uploaded-by": claims.get("sub", "unknown"),
        },
        Conditions=[
            {"Content-Type": req.content_type},
            {"x-amz-server-side-encryption": "aws:kms"},
            {"x-amz-server-side-encryption-aws-kms-key-id": d.settings.kms_key_id},
            {"x-amz-meta-uploaded-by": claims.get("sub", "unknown")},
            ["content-length-range", 1, d.settings.max_upload_bytes],
        ],
        ExpiresIn=EXPIRES_S,
    )
    metrics.add_metric(name="UploadUrlsIssued", unit=MetricUnit.Count, value=1)
    logger.info("presigned", extra={"tenant_id": tenant_id, "key": key, "size": req.size})
    return _response(
        200,
        {
            "upload_id": upload_id,
            "key": key,
            "url": post["url"],
            "fields": post["fields"],
            "expires_in": EXPIRES_S,
        },
    )
