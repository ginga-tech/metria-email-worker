
---

# 📄 docs/03-Idempotency-Strategy.md

```markdown
# Idempotency Strategy

## Goal

Prevent duplicate email dispatch for:

(userId, periodStartUtc, periodEndUtc, templateKey)

## Mechanism

Database-backed idempotency table:

EmailDispatchLog

## Unique Composite Index

(userId, periodStartUtc, periodEndUtc, templateKey)

## Flow

1. Receive message
2. Check if dispatch record exists
3. If exists → skip
4. If not → send email
5. Insert dispatch record
6. Commit

## Race Condition Handling

Use transaction + unique index to prevent double insert.