# Retry and Dead Letter Strategy

## Retry Policy

- Exponential backoff
- Configurable max attempts
- Transient failures only

## Transient Examples

- SMTP timeout
- Network failure
- Temporary provider 5xx

## Permanent Failures

- Invalid email format
- Template not found
- User not found

## Dead Letter Queue

Messages exceeding retry limit must:

- Be NACKed
- Routed to DLQ
- Logged with full context