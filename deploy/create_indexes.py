"""Create the vector indexes once per environment (idempotent).

    python deploy/create_indexes.py --region us-east-1 \
        --aoss-endpoint https://abc123.us-east-1.aoss.amazonaws.com --aoss-index aviation-docs \
        --vector-bucket example-air-doc-vectors-prd --vector-index aviation-docs-1024

Terraform creates the OpenSearch Serverless collection (type VECTORSEARCH), its encryption /
network / data-access policies, and the S3 vector bucket. Index *mappings* are data-plane objects,
so they are created here and kept in version control next to the code that writes them.
"""

from __future__ import annotations

import argparse

import boto3

from docingest.stores.opensearch import index_body, make_client


def ensure_opensearch_index(endpoint: str, region: str, index: str, dims: int) -> str:
    client = make_client(endpoint, region)
    if client.indices.exists(index=index):
        return "exists"
    client.indices.create(index=index, body=index_body(dims))
    return "created"


def ensure_s3_vector_index(region: str, bucket: str, index: str, dims: int) -> str:
    s3v = boto3.client("s3vectors", region_name=region)
    existing = {i["indexName"] for i in s3v.list_indexes(vectorBucketName=bucket).get("indexes", [])}
    if index in existing:
        return "exists"
    s3v.create_index(
        vectorBucketName=bucket,
        indexName=index,
        dataType="float32",
        dimension=dims,
        distanceMetric="cosine",
        # Pointers are stored but not filterable, keeping the filterable metadata budget for filters.
        metadataConfiguration={"nonFilterableMetadataKeys": ["text_uri", "chunk_no", "page_start"]},
    )
    return "created"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--aoss-endpoint", required=True)
    p.add_argument("--aoss-index", default="aviation-docs")
    p.add_argument("--vector-bucket", required=True)
    p.add_argument("--vector-index", default="aviation-docs-1024")
    p.add_argument("--dims", type=int, default=1024)
    a = p.parse_args()
    print("opensearch:", ensure_opensearch_index(a.aoss_endpoint, a.region, a.aoss_index, a.dims))
    print("s3 vectors:", ensure_s3_vector_index(a.region, a.vector_bucket, a.vector_index, a.dims))


if __name__ == "__main__":
    main()
