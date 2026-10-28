# Design decisions, failure handling, security, DR

## Why fully serverless (and why ECS and EventBridge were removed)

An earlier version used EventBridge between S3 and the router and ECS tasks for OCR post-processing.
Both were removed:

- **EventBridge**: one consumer does not need content-based fan-out; S3 -> SQS is simpler, cheaper and
  gives the same buffering and DLQ semantics.
- **ECS**: the only reason was long-running work on huge PDFs. Distributed Map over 200-page ranges
  covers it with per-range retries and no cluster, image pipeline or capacity planning.

Trade-off accepted: Lambda's 15-minute and 10 GB limits. The `plan` step reads whole PDFs up to the
Lambda /tmp limit; anything larger should be split at the source (rare in practice for manuals).

## Failure scenarios

| Failure | What happens | Data loss? |
|---|---|---|
| Duplicate S3 event / SQS redelivery | DynamoDB claim makes it a no-op | no |
| Router crashes after claim | next delivery sees `RECEIVED` and resumes | no |
| Malware scan not finished | `ScanPending`, retried later (SQS / Step Functions) | no |
| Malware found | quarantined, `REJECTED`, alarm | intentionally not indexed |
| Unsupported or corrupt file | `REJECTED` with reason | intentionally not indexed |
| Textract concurrency quota | `TextractBusy`, exponential backoff up to 15 min | no |
| Textract job fails | task failure -> `MarkFailed` -> execution fails -> alarm | no (raw file intact, redrive) |
| Bedrock throttling | message retried after visibility timeout, DLQ after 10 | no |
| OpenSearch partial bulk failure | only that message retries; ids are deterministic | no |
| Embed message redelivered | `batches_done` set prevents double counting | no |
| Bad parser release | DLQ fills; fix and redrive with `start-message-move-task` | no |

**Reprocessing** a document (new chunking rules, new embedding model) is safe because chunk ids are
deterministic and `set_chunks` resets the completion counter.

## Security

| Area | Control |
|---|---|
| Upload authorization | JWT authorizer; tenant from token; presigned POST conditions enforced by S3 |
| Data isolation | tenant prefix in S3 keys, `tenant_id` on every vector and metadata item, filter at query time |
| Malware | GuardDuty Malware Protection for S3, scan gate before any parser, quarantine bucket |
| File type spoofing | magic-byte detection, not extensions or declared content type |
| Encryption | SSE-KMS everywhere (raw, processed, quarantine, DynamoDB, SQS, OpenSearch, S3 Vectors) |
| Least privilege | one IAM role per function; Textract service role reads raw/processed only |
| Network | Lambdas in private subnets with VPC endpoints; OpenSearch collection with VPC access policy |
| Audit | CloudTrail data events on buckets; DynamoDB state history; Step Functions execution history |
| Partner access | Transfer Family per-partner users mapped to one prefix; keys rotated in Secrets Manager |
| PII | task cards can include names and staff numbers; Amazon Comprehend PII detection can be added before chunking for regulated tenants |

## Disaster recovery

- **Raw documents are the source of truth.** S3 Cross-Region Replication to the DR region (RPO
  typically minutes; S3 RTC gives a 15-minute SLA if needed).
- **Everything else is derived** and can be rebuilt: processed bucket (replicated too, to avoid paying
  for OCR twice), vectors (re-embedded from replicated `chunks.jsonl`), metadata (rebuilt from chunks).
- **DynamoDB state** as a global table so in-flight status and dedupe hashes survive.
- **Same Terraform** deploys the pipeline in the DR region, scaled to zero until needed (serverless
  makes warm standby nearly free).
- Practise it: replay one day of `chunks.jsonl` into the DR indexes quarterly and measure the time.

## Scalability notes

- S3 scales per prefix (3,500 PUT / 5,500 GET per second per prefix): tenant/source/date prefixes
  spread load naturally.
- SQS standard queues have practically unlimited throughput.
- The router's work is O(1) per document except the small-PDF probe; heavy work is pushed downstream.
- The real scaling levers are the quotas in [capacity-and-cost.md](capacity-and-cost.md).

## What to avoid

- OCR by default "to be safe": it multiplies the bill and lowers text quality for born-digital files.
- Fixed-size chunking for maintenance manuals: it separates warnings from steps and mixes tasks.
- Unbounded Lambda concurrency in front of Bedrock: you only get throttled faster.
- Random chunk ids: every retry would create duplicate vectors.
- Polling Textract from Lambda: idle compute and request-quota pressure; use SNS + task tokens.
- Storing chunk text only inside the vector store: keep `chunks.jsonl` so you can change models.
