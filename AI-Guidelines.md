# AI Development Guidelines

When generating code:

1. Respect Clean Architecture boundaries.
2. Never hardcode secrets.
3. Never bypass idempotency checks.
4. Always propagate correlationId.
5. Always use structured logging.
6. Do not place business logic in Infrastructure.
7. Do not use static/global state.

If ambiguity arises:
- Prefer consistency with backend patterns.
- Do not invent new architectural styles.
- Document assumptions.