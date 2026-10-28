"""S3 key layout for the raw bucket.

    <tenant>/<source>/<yyyy>/<mm>/<dd>/<upload_id>/<filename>     uploads via presigned URL
    <tenant>/<source>/<anything>/<filename>                         SFTP drops, connectors

Examples:
    example-air/portal/2026/10/28/u_3f9c.../AMM_32-42-27_rev61.pdf
    example-air/oem-feed/batch-1028/SB-A320-32-1471.pdf             (AWS Transfer Family drop)

Why tenant first: S3 prefixes give per-tenant IAM conditions, lifecycle rules, S3 Inventory reports
and request-rate partitioning (each prefix scales to thousands of requests/s independently).
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import unquote_plus

UPLOAD_ID_RE = re.compile(r"^u_[0-9a-f]{32}$")
SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
TENANT_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,40}$")


class KeyError_(ValueError):
    pass


@dataclass(frozen=True)
class ParsedKey:
    tenant_id: str
    source: str
    doc_id: str
    filename: str


def safe_filename(name: str) -> str:
    """Normalise user-supplied file names (unicode, spaces, path tricks)."""
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    name = name.replace("\\", "/").rsplit("/", 1)[-1]
    name = SAFE_NAME_RE.sub("_", name).strip("._") or "document"
    return name[:180]


def upload_key(
    tenant_id: str, source: str, upload_id: str, filename: str, now: datetime | None = None
) -> str:
    now = now or datetime.now(UTC)
    return f"{tenant_id}/{source}/{now:%Y/%m/%d}/{upload_id}/{safe_filename(filename)}"


def decode_event_key(raw_key: str) -> str:
    """S3 event notifications URL-encode keys and use '+' for spaces."""
    return unquote_plus(raw_key)


def parse_key(bucket: str, key: str, version_id: str | None = None) -> ParsedKey:
    parts = key.split("/")
    if len(parts) < 3 or not TENANT_RE.match(parts[0]):
        raise KeyError_(f"key outside the tenant layout: {key}")
    tenant, source, filename = parts[0], parts[1], parts[-1]
    if len(parts) >= 4 and UPLOAD_ID_RE.match(parts[-2]):
        doc_id = parts[-2][2:]
    else:
        # Drops without an upload id: stable id from the object identity (bucket, key, version).
        doc_id = hashlib.sha256(f"{bucket}/{key}#{version_id or ''}".encode()).hexdigest()[:32]
    return ParsedKey(tenant_id=tenant, source=source, doc_id=doc_id, filename=filename)
