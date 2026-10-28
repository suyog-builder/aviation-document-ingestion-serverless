# Capacity and cost

## Capacity maths

| Quantity | Average | 5x peak |
|---|---|---|
| Documents | 2,000,000/day = 23.1/s | 116/s |
| Fast lane (70%) | 16.2/s | 81/s |
| Heavy lane (30%) | 6.9 executions/s | 35/s |
| Chunks (~10/doc) | 231/s | 1,157/s |
| Embedding requests (1 per chunk) | ~13,900/min | ~69,400/min |
| Embedding tokens (~500/chunk) | 10B/day | |

**The binding constraints are AWS quotas, not code:**

| Quota (check Service Quotas in your account) | Why it matters | Mitigation |
|---|---|---|
| Bedrock Titan Text Embeddings V2 requests/min (on-demand) | 13.9k/min average, ~69k/min at peak | request an increase before go-live; embed queue absorbs peaks; **batch inference** for back-fills |
| Textract concurrent async jobs and start TPS | heavy lane peaks at ~35 starts/s | backoff retries (`TextractBusy`); selective OCR shrinks jobs; spread partner batch windows |
| Lambda concurrent executions (account) | fast lane + embed + heavy share it | reserved concurrency per function so one stage cannot starve another |
| Step Functions Standard `StartExecution` rate | 35/s at peak | request increase; executions queue via router SQS retries if throttled |
| OpenSearch Serverless OCUs | indexing at ~1,150 docs/s peak | set max OCU; S3 Vectors takes the warm data |

## Cost estimate (on-demand list prices, verify for your region and date)

**Assumptions.** 2M docs/day; 30% go to the heavy lane; 60% of heavy documents need OCR; selective
OCR brings the average OCR'd pages to 4 per OCR'd document; Lambda on arm64; prices checked
October 2026 against the AWS pricing pages (Titan V2: $0.02 per 1M input tokens on-demand, $0.01
batch; Textract DetectDocumentText: $1.50 per 1,000 pages for the first 1M pages/month, $0.60 after;
AnalyzeDocument tables: $15 per 1,000 pages).

| Item | Basis | Per month (approx.) |
|---|---|---|
| Titan V2 embeddings | 300B tokens x $0.02/M | **$6,000** (batch inference: ~$3,000) |
| Textract OCR | 43M pages (1M at $1.50/1k + 42M at $0.60/1k) | **~$26,700** |
| Step Functions Standard | 18M executions x ~10 transitions x $0.025/1k | ~$4,500 |
| Lambda compute | ~13M GB-s/day on arm64 | ~$5,000-6,000 |
| SQS | ~26M requests/day | ~$300 |
| S3, DynamoDB, KMS, CloudWatch | storage and request volume dependent | price in the calculator |
| OpenSearch Serverless, S3 Vectors | OCU hours and stored vectors | price in the calculator |

**Read this table as:** OCR dominates. Without selective OCR (whole heavy documents at ~20 pages)
the Textract line would be roughly 5x larger.

### Cost levers, in order of impact

1. **Do not OCR text that already exists.** Text-layer detection in the router, promotion only for
   image-heavy PDFs, selective OCR of image pages only.
2. **Plain text detection by default.** TABLES/FORMS only for IPC and task cards (10x price).
3. **Dedupe before paying.** Content hash check happens before any OCR or embedding.
4. **Batch inference for back-fills.** Re-embedding a 50M-chunk archive at half price.
5. **Re-embed from `chunks.jsonl`**, never re-OCR, when the embedding model changes.
6. **Hot/warm split.** Only current revisions in OpenSearch; history in S3 Vectors.
7. **arm64 Lambdas, right-sized memory** (measure with Lambda Power Tuning).
8. **S3 Bucket Keys** for KMS.

### Hidden costs to watch

- Step Functions transitions grow with retries; alarm on executions with > 3 retries.
- CloudWatch Logs ingestion at 2M docs/day: keep INFO logs to one line per stage; 30-day retention.
- NAT gateway data processing if Lambdas run in a VPC without endpoints: use VPC endpoints for S3,
  DynamoDB, SQS, Bedrock, Textract, Step Functions.
