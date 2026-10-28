# Lambda and queue settings (starting values)

| Function | Trigger | Memory | Timeout | Concurrency | Batch | Notes |
|---|---|---|---|---|---|---|
| presign | HTTP API (JWT authorizer) | 256 MB | 5 s | default | - | arm64 |
| router | SQS ingest queue | 1,024 MB | 60 s | reserved 50 | 10, window 1 s | reads at most 20 MB; partial batch response |
| fast_lane | SQS fast queue | 2,048 MB, 2 GB /tmp | 300 s | reserved 200 | 5 | arm64; visibility timeout 6x timeout = 1,800 s |
| embed_worker | SQS embed queue | 1,024 MB | 120 s | **reserved 25-50** | 5 | concurrency = Bedrock speed limit |
| heavy plan / collect | Step Functions | 10,240 MB, 10 GB /tmp | 900 s | reserved 50 | - | big PDFs are read whole |
| heavy start_ocr_part | Step Functions (task token) | 512 MB | 30 s | default | - | |
| heavy chunk_range | Distributed Map (Express) | 2,048 MB | 300 s | default | - | |
| textract_complete | SNS | 256 MB | 10 s | default | - | |

| Queue | Visibility timeout | maxReceiveCount -> DLQ | Retention |
|---|---|---|---|
| ingest (S3 events) | 360 s | 5 | 4 days |
| fast | 1,800 s | 5 | 4 days |
| embed | 720 s | 10 | 14 days |

Every DLQ has a CloudWatch alarm on `ApproximateNumberOfMessagesVisible > 0` and a redrive runbook
(`aws sqs start-message-move-task`).
