# Observability

## Logging

Structured logs required.

Mandatory fields:

- messageId
- correlationId
- userId
- durationMs
- retryCount

## Metrics

- emails_processed_total
- emails_sent_total
- emails_failed_total
- emails_retried_total

## Tracing

correlationId must allow end-to-end trace.