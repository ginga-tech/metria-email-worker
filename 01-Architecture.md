# Architecture

## Style

Clean Architecture with strict boundaries.

## Layers

### Domain
- EmailDigest entity
- Period ValueObject
- EmailAddress ValueObject
- Idempotency rules

### Application
- ProcessEmailDigestUseCase
- IEmailSender
- IEmailDispatchRepository
- Retry orchestration

### Infrastructure
- RabbitMQ consumer
- SMTP / SendGrid adapter
- EF Core DbContext
- Migrations
- Logging adapter

### Worker (Host)
- BackgroundService
- DI container
- HealthChecks
- Graceful shutdown

## Dependency Rule

- Domain -> no dependencies
- Application -> depends only on Domain
- Infrastructure -> depends on Application
- Worker -> depends on Application + Infrastructure