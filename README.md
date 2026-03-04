# Metria.EmailWorker

Production-grade background worker for sending periodic email digests from RabbitMQ with strict idempotency, retry, and DLQ handling.

## Architecture

Clean Architecture with strict dependency rule:

- `Metria.EmailWorker.Domain`: Entities, value objects, domain rules.
- `Metria.EmailWorker.Application`: Use case orchestration and ports.
- `Metria.EmailWorker.Infrastructure`: EF Core, RabbitMQ, SMTP/SendGrid, metrics, logging adapters.
- `Metria.EmailWorker.Processor`: Host composition, background service, health checks, startup migration.

## Message Contract

Current contract version: `v1`.

Supports:
- Envelope format:
  - `{ "version": "v1", "payload": { ... } }`
- Raw payload format:
  - `{ ...v1 fields... }`

## Idempotency

Backed by PostgreSQL table `EmailDispatchLog`.

Unique key:
- `(UserId, PeriodStartUtc, PeriodEndUtc, TemplateKey)`

Behavior:
- First valid message reserves row with `Processing`.
- Duplicate dispatch key is skipped.
- Same message retry is allowed only after `TransientFailed`.
- Success updates status to `Sent`.

## Retry + DLQ

- Manual ACK/NACK only (`autoAck = false`).
- Polly exponential backoff retry for transient exceptions.
- Configurable via env:
  - `EMAIL_RETRY_MAX_ATTEMPTS`
  - `EMAIL_RETRY_BASE_DELAY_SECONDS`
  - `EMAIL_RETRY_MAX_DELAY_SECONDS`
- Permanent failures and exhausted transient retries are NACKed with `requeue=false` and routed to DLQ.

## Logging and Metrics

Structured logs include:
- `messageId`
- `correlationId`
- `userId`
- `durationMs`
- `retryCount`

Metrics counters:
- `emails_processed_total`
- `emails_sent_total`
- `emails_failed_total`
- `emails_retried_total`

## Environment Variables

Copy `.env.example` to `.env` and fill values.

Required:
- `ConnectionStrings__EmailWorkerDb`
- `RABBITMQ_HOST`
- `RABBITMQ_PORT`
- `RABBITMQ_USER`
- `RABBITMQ_PASSWORD`
- `RABBITMQ_QUEUE_EMAIL_DIGEST`
- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USER`
- `SMTP_PASSWORD`
- `SMTP_FROM`
- `EMAIL_DIGEST_ENABLED`
- `EMAIL_DIGEST_CRON`
- `EMAIL_DIGEST_TIMEZONE_DEFAULT`
- `EMAIL_RETRY_MAX_ATTEMPTS`
- `EMAIL_RETRY_BASE_DELAY_SECONDS`
- `EMAIL_RETRY_MAX_DELAY_SECONDS`

Optional:
- `SENDGRID_API_KEY`
- `SENDGRID_FROM`

## Local Run

1. Create env file:
   - `cp .env.example .env`
2. Start dependencies:
   - `docker compose up -d postgres rabbitmq`
3. Restore/build:
   - `dotnet restore`
   - `dotnet build metria-email-worker.slnx`
4. Run worker:
   - `dotnet run --project src/Metria.EmailWorker.Processor`

## Run Tests

Unit tests:
- `dotnet test test/Metria.EmailWorker.Application.UnitTests/Metria.EmailWorker.Application.UnitTests.csproj`

Integration tests (requires Docker running):
- `dotnet test test/Metria.EmailWorker.Infrastructure.IntegrationTests/Metria.EmailWorker.Infrastructure.IntegrationTests.csproj`

## Docker

Run full local stack:
- `docker compose up --build`

## Assumptions

- Scheduler env vars are validated but scheduler execution is out of scope for this worker (consumer-only).
- Supported template keys are:
  - `digest.v1`
  - `digest.biweekly.v1`
  - `weekly-digest`
- If `SENDGRID_API_KEY` is configured, SendGrid adapter is used; otherwise SMTP adapter is used.

