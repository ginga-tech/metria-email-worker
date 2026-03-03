# Metria Email Worker – Overview

## Purpose

This worker is responsible for sending periodic email digests to users.

It operates asynchronously via RabbitMQ and must:

- Be idempotent
- Be observable
- Be resilient
- Follow Clean Architecture

## Scope

- Consume email digest messages
- Assemble localized content
- Send via provider abstraction
- Guarantee no duplicate dispatches
- Support retry and dead-letter routing

## Out of Scope

- Subscription billing logic (handled by API)
- User authentication
- Template authoring UI

## Architectural Constraints

- .NET 10
- EF Core + PostgreSQL
- Env-based configuration only
- Structured logging
- Production-ready robustness