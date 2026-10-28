# The pipeline, step by step

Each step: **what it does**, **why**, **aviation example**, **code**, **failure handling**.

We follow four real-looking documents from the fictional operator *Example Air*:

| File | Arrives via | Lane |
|---|---|---|
| `AMM_32-42-27_brake_wear_check_rev61.pdf` (born-digital manual task) | upload portal | fast |
| `SOP_cold_weather_operations.docx` (quality department SOP) | upload portal | fast |
| `SB-A320-32-1471.txt` (OEM service bulletin summary) | partner SFTP feed | fast |
| `TaskCard_VT-EXA_C-check_scan.pdf` (scanned, handwritten sign-offs) | hangar scanner | heavy |

---

## Step 1: Sources

| Source | Typical content | Daily share (assumed) |
|---|---|---|
| Enterprise apps (ERP, MRO, HR) | work packages, task cards exported as PDF, training records | 35% |
| Upload portals (web, mobile, API) | technical publications revisions, engineering orders | 20% |
| Partner / OEM feeds | service bulletins, AMM/IPC revisions, vendor CMM updates | 25% |
| Scanners, email, SharePoint, NAS | scanned task cards, tech log pages, emailed reports | 20% |

Two entry doors, chosen by how the source behaves: **API Gateway** for interactive clients and
**AWS Transfer Family** for systems that can only speak SFTP/FTPS.

---

## Step 2a: API Gateway -> presigned S3 upload

**What.** `handlers/presign.py` returns a presigned **POST** for exactly one key. The file goes
from the browser straight to S3; Lambda never touches the bytes.

**Why.** API Gateway caps payloads at 10 MB and Lambda would bill for time spent copying bytes. A
presigned POST also lets **S3 enforce** the rules: key prefix, size range, content type, KMS
encryption.

**Example.**
```
POST /uploads {"filename":"AMM 32-42-27 rev61.pdf","content_type":"application/pdf","size":4194304}
-> key: example-air/portal/2026/10/28/u_3f9c.../AMM_32-42-27_rev61.pdf  (valid 15 minutes)
```
The tenant (`example-air`) comes from the user's JWT, never from the request body, so one airline
can never write into another's prefix.

**Failure.** Client retries the S3 POST up to 3 times; the upload id makes each retry land on the same
key. An expired URL simply means asking for a new one.

## Step 2b: AWS Transfer Family (SFTP/FTPS)

**What.** Partners drop batches into an SFTP endpoint whose home directory maps to
`s3://raw/<tenant>/<source>/...`. Partners retry failed transfers 3 times on their side.

**Example.** The OEM pushes 3,200 revised AMM pages at 01:00 UTC into
`example-air/oem-feed/batch-1028/`. Each file becomes an S3 object and an S3 event.

---

## Step 3: S3 raw bucket

**What.** Versioned, KMS-encrypted (with S3 Bucket Keys), Object Lock optional for regulated
records. Layout `tenant/source/yyyy/mm/dd/upload_id/filename` (`keys.py`).

**Why this layout.** Tenant-first prefixes give per-tenant IAM conditions, lifecycle rules and S3
request-rate scaling per prefix. Versioning means a re-uploaded revision is a new version with a new
`doc_id`, while the old one stays available for audits.

**Malware.** GuardDuty Malware Protection for S3 scans every new object and writes the tag
`GuardDutyMalwareScanStatus`. Parsers only run on `NO_THREATS_FOUND` (step 6).

---

## Step 4: SQS ingest queue (S3 event notifications)

**What.** S3 sends `ObjectCreated` notifications **directly** to SQS. Router Lambda polls it in
batches of 10. 5 receives, then DLQ.

**Why SQS and not S3 -> Lambda or EventBridge.** The partner batch at 01:00 can be 50,000 files in
minutes. SQS absorbs the burst, Lambda drains it at a controlled concurrency, and failures go to a
DLQ instead of being dropped. EventBridge adds cost and a hop with no benefit when there is one
consumer, which is why the earlier design's EventBridge was removed.

**Gotcha handled.** S3 event keys are URL-encoded with `+` for spaces (`decode_event_key`), and S3
sends an `s3:TestEvent` when notifications are configured (ignored).

---

## Step 5: Router Lambda + DynamoDB state

**What** (`handlers/router.py`), target under 300 ms per object:

1. Parse the key -> tenant, source, `doc_id`.
2. **Claim** `DOC#<doc_id>` in DynamoDB with a conditional put. A duplicate S3/SQS delivery finds the
   claim and stops. A claim stuck in `RECEIVED` (previous attempt crashed) is resumed, not skipped,
   so no document is lost.
3. `HeadObject` -> size and the uploader's SHA-256 checksum if present.
4. Ranged GET of the first 8 KB -> **magic-byte** type detection (`classify.sniff`). A `.pdf` that is
   really a ZIP is rejected.
5. PDFs up to 20 MB: probe the text layer on the first, middle and last pages.
6. **Content dedupe**: `HASH#<tenant>#<sha256>`. Partners often re-send identical files with new names;
   the duplicate is marked `DUPLICATE` and never pays for OCR or embeddings.
7. Route: fast queue, or `StartExecution` on the heavy workflow with **name = doc_id** (a retry
   cannot start two OCR workflows).

**Example decisions.**
```
AMM_32-42-27_..._rev61.pdf   3 pages, ~1,400 chars/page       -> FAST  "pdf with text layer"
TaskCard_VT-EXA_..._scan.pdf 4 pages, 0 chars on sampled pages -> HEAVY "no usable text layer (scanned)"
SB-A320-32-1471-resend.txt   same SHA-256 as yesterday's file -> DUPLICATE of 9f1c...
invoice.pdf                  starts with PK\x03\x04            -> REJECTED "unsupported file type"
```

**Failure.** Any exception fails only that SQS message (partial batch response); the other 9 in the
batch succeed.

---

## Step 6: Fast lane (~70%): SQS fast queue -> scan and extract -> normalize and chunk

**Scan gate** (`pipeline.malware_gate`): no scan tag yet -> raise `ScanPending` and let SQS retry
after the visibility timeout; `THREATS_FOUND` -> copy to the quarantine bucket, delete from raw, mark
`REJECTED`.

**Extract** (`extract/native.py`): pypdf page by page (page numbers are preserved for citations),
DOCX paragraphs **and tables** (flattened `cell | cell` so torque and holdover tables stay readable),
HTML without scripts, XML text nodes.

**Promotion.** The router sampled only 3 pages. If extraction shows more than 20% image-only pages
(e.g. wiring diagrams pasted as images), the document is promoted to the heavy lane for selective
OCR instead of indexing empty pages.

**Normalize** (`chunking.normalize_pages`): remove running headers/footers that repeat on most pages
("EXAMPLE AIR - CONTROLLED COPY"), page numbers, hyphenation across line breaks.

**Chunk** (`chunking.chunk_document`), ~500 tokens:
- **Hard boundaries** at `TASK 32-42-27-200-801`, `SUBTASK`, ATA headings: a chunk never mixes two tasks.
- **Soft headings** ("1. Reason for the Job", "A. Fixtures") stay inside the chunk.
- **WARNING / CAUTION is never separated from the step it protects.**
- **Breadcrumb prefix**: `[AMM > TASK 32-42-27-200-801-A Brake Wear Check]` so a chunk that says only
  "Measure the length of the pin" still embeds with its context.
- Overlap of ~50 tokens, only within the same section.
- Deterministic ids `doc_id:00000`, so re-processing overwrites instead of duplicating.

**Metadata** (`metadata.py`): doc type (AMM, IPC, SB, AD, MEL, TASK_CARD, SOP), fleet, revision, ATA
chapter, and per chunk: task refs, part numbers, MEL items, registrations.

**Example output chunk** (from the AMM sample):
```
[AMM > TASK 32-42-27-200-801-A Brake Wear Check]
Effectivity: ALL. Revision 61. ...
WARNING: MAKE SURE THAT THE PARKING BRAKE IS SET ...
(1) Apply the parking brake and make sure that the brake pressure indication shows at least 2000 psi ...
metadata: doc_type=AMM fleet_type=A20N revision=61 ata_chapter=32
          task_refs=[32-42-27-000-801, 32-42-27-200-801-A] part_numbers=[98D32103500-000]
```

**Persist + enqueue** (`pipeline.process_pages`): pages and chunks to the processed bucket, then
`set_chunks(total)` **before** sending embed messages (10 chunks per message, `SendMessageBatch`).

---

## Step 7: Heavy lane (~30%): Step Functions Standard -> Textract -> chunk with Distributed Map

**Plan** (`heavy_lane.plan`) picks a mode:

| Mode | When | Example |
|---|---|---|
| `native` | big PDF that already has text | 410 MB, 9,800-page IPC: no OCR at all |
| `ocr_full` | scans, TIFF/PNG/JPEG | the VT-EXA task card scan |
| `ocr_selective` | mostly text, some image pages | 400-page AMM chapter with 12 scanned diagrams -> OCR 12 pages, not 400 |
| `ocr_split` | scanned and above Textract's page limit | 5,000-page archive split into 2,500-page parts |

**OCR** (`start_ocr_part`): `StartDocumentTextDetection` (or `StartDocumentAnalysis` with TABLES only
for IPC/task cards). `ClientRequestToken = doc_id` makes the start idempotent. The Lambda stores
`JobId -> task token` and returns; the workflow **waits for free**. Textract publishes completion to
SNS -> `textract_complete` -> `SendTaskSuccess`. Concurrent-job quota hits raise `TextractBusy`,
retried with exponential backoff and jitter (up to 15 minutes between attempts).

**Collect**: reads Textract's JSON output files from S3 (`OutputConfig`, avoiding the low
`GetDocument*` request quota), rebuilds page text from LINE blocks with mean confidence, renumbers
selective/split pages back to their original page numbers, merges with native pages, and plans
200-page ranges. Pages under 80% OCR confidence are counted in `LowConfidenceOcrPages`.

**ChunkRanges**: Distributed Map with Express children, 20 in parallel. A 9,800-page catalogue becomes
49 independent, individually retried chunking jobs.

**Finalize**: gathers range chunk files, writes `chunks.jsonl`, records the total, enqueues embeddings.

**Example (scanned task card).** 4 pages -> `ocr_full` -> 1 Textract job -> pages like
"WORK CARD 0401 - VT-EXA C-CHECK / TASK 32-42-27-200-801 Brake wear check / Brake 3 wear pin flush
with housing - replace brake" -> 4 chunks, each tagged `task_refs=[32-42-27-200-801]`,
`registrations=[VT-EXA]`.

**Failure.** Every state catches errors into `MarkFailed`, which writes the error and cause to the
document's state item, then the execution fails (alarmed).

---

## Step 8: SQS embed queue (speed limiter for Bedrock)

**What.** One message = up to 10 chunks of one document. 10 receives, then DLQ.

**Why a queue here.** Bedrock on-demand embeddings are limited by **requests per minute** per
account and region. Lambda can scale far faster than that, so the embed workers' reserved
concurrency (25-50) is the throttle, and the queue holds the backlog during peaks.

---

## Step 9: Embed workers + Titan Text Embeddings V2

**What** (`handlers/embed_worker.py`, `embed.py`):
1. Embed each chunk (`dimensions=1024`, `normalize=true`) with 8 threads per invocation.
2. Write **S3 Vectors** (every chunk) and **OpenSearch** (current revisions) using the chunk id as the
   document id, so retries overwrite.
3. `add_embedded(doc_id, batch_no, n)` counts each batch exactly once (a `batches_done` set guards
   against SQS redelivery). The worker that completes the document writes the metadata/ACL item and
   marks it `INDEXED`.

**Throttling.** In-process adaptive retries (3), then the message fails and SQS redelivers after the
visibility timeout: natural backoff, no busy waiting.

---

## Step 10: Index and stores

| Store | Holds | Why |
|---|---|---|
| **OpenSearch Serverless** (vector collection) | current revisions, HNSW (Faiss) with fp16 quantization | low-latency hybrid search (k-NN + BM25) for the agent; field names match Bedrock Knowledge Bases defaults |
| **S3 Vectors** | every chunk, every revision | long tail and audits at object-storage cost |
| **S3 processed** | `pages.jsonl`, `chunks.jsonl`, Textract output | re-embed with a new model without re-running OCR (the expensive part) |
| **DynamoDB metadata** | one item per document: type, revision, ACL groups | post-filtering by entitlement; data stewardship queries |

**Example OpenSearch document** (abridged):
```json
{"AMAZON_BEDROCK_TEXT_CHUNK": "[AMM > TASK 32-42-27-200-801-A Brake Wear Check] ...",
 "bedrock-knowledge-base-default-vector": [0.0132, ...],
 "x-amz-bedrock-kb-source-uri": "s3://raw/example-air/portal/2026/10/28/u_.../AMM_32-42-27_brake_wear_check_rev61.pdf",
 "tenant_id": "example-air", "doc_type": "AMM", "fleet_type": "A20N", "ata_chapter": "32",
 "revision": "61", "task_refs": ["32-42-27-200-801-A"], "page_start": 1}
```

---

## Step 11: Operations, security and observability (every step)

- **IAM**: one role per function, scoped to its prefix/queue/table; Textract has its own service role.
- **KMS**: one key per environment; S3 Bucket Keys reduce KMS request cost by ~99% at this volume.
- **CloudWatch**: Powertools structured logs with `doc_id`, EMF metrics (`RoutedFast`, `RoutedHeavy`,
  `Duplicates`, `Rejected`, `PromotedToHeavy`, `OcrPagesAvoided`, `LowConfidenceOcrPages`,
  `ChunksEmbedded`, `TokensEmbedded`, `DocumentsIndexed`).
- **X-Ray**: traces across router, lanes and embed workers.
- **CloudTrail**: data events on the raw bucket for who uploaded what.
- **Secrets Manager**: partner SFTP credentials and any connector secrets.
- **SNS alerts**: DLQ depth, Step Functions failures, Bedrock throttling, Textract quota pressure.
