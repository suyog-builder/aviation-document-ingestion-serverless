"""Serverless document ingestion for aviation technical content.

Pipeline (one module per stage):

    handlers/presign.py       API Gateway -> presigned S3 upload           (entry: portals, apps)
    handlers/router.py        S3 event -> SQS -> route fast/heavy lane      (dedupe, classify)
    handlers/fast_lane.py     SQS fast queue -> scan, parse, chunk          (~70% of documents)
    handlers/heavy_lane.py    Step Functions tasks: Textract OCR, chunk     (~30%: scans, big files)
    handlers/embed_worker.py  SQS embed queue -> Titan V2 -> stores         (rate-limited)

Shared building blocks: models, keys, state (DynamoDB), classify, extract/*, normalize,
chunking, metadata, embed, stores/*.
"""

__version__ = "1.0.0"
