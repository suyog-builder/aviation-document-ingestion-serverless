"""Settings, injected by Terraform as Lambda environment variables (prefix ``DOCINGEST_``).

Example values for production in us-east-1:
    DOCINGEST_RAW_BUCKET=example-air-docs-raw-prd
    DOCINGEST_PROCESSED_BUCKET=example-air-docs-processed-prd
    DOCINGEST_STATE_TABLE=docingest-state-prd
    DOCINGEST_FAST_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/111122223333/docingest-fast-prd
    DOCINGEST_EMBED_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/111122223333/docingest-embed-prd
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DOCINGEST_", extra="ignore")

    aws_region: str = "us-east-1"
    raw_bucket: str = "docs-raw"
    processed_bucket: str = "docs-processed"
    quarantine_bucket: str = "docs-quarantine"
    kms_key_id: str = "alias/docingest"

    state_table: str = "docingest-state"
    metadata_table: str = "docingest-metadata"

    fast_queue_url: str = "https://sqs.us-east-1.amazonaws.com/000000000000/docingest-fast"
    embed_queue_url: str = "https://sqs.us-east-1.amazonaws.com/000000000000/docingest-embed"
    heavy_state_machine_arn: str = "arn:aws:states:us-east-1:000000000000:stateMachine:docingest-heavy"

    textract_sns_topic_arn: str = ""
    textract_role_arn: str = ""

    embed_model_id: str = "amazon.titan-embed-text-v2:0"
    embed_dimensions: int = Field(default=1024, description="Titan V2 supports 256, 512 or 1024")
    embed_threads: int = Field(default=8, ge=1, le=32)

    opensearch_endpoint: str = ""  # https://abc123.us-east-1.aoss.amazonaws.com
    opensearch_index: str = "aviation-docs"
    s3vectors_bucket: str = "aviation-docs-vectors"
    s3vectors_index: str = "aviation-docs-1024"

    # Routing thresholds
    fast_lane_max_bytes: int = 20 * 1024 * 1024  # 20 MB
    fast_lane_max_pages: int = 300
    min_text_chars_per_page: int = 200  # below this a PDF page is treated as scanned
    ocr_confidence_floor: float = 80.0  # Textract LINE confidence (0-100)

    # Chunking
    chunk_target_tokens: int = 500
    chunk_overlap_tokens: int = 50
    chunks_per_embed_message: int = 10
    # Upload limits
    max_upload_bytes: int = 500 * 1024 * 1024


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
