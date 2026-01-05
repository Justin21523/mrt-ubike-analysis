# Stage 1: TDX Ingestion Client Design

## Goals

TDX is treated as a production data source. The ingestion client must be:

- **Reproducible**: same parameters can be re-run (pair with Bronze persistence)
- **Maintainable**: API changes are isolated to a client layer
- **Observable**: failures include HTTP status, error body, retry context
- **Secure**: credentials are loaded from environment variables only

## Module boundaries

- `TDXClient`: HTTP + OAuth2 token only (no persistence, no transforms)
- `scripts/` or pipeline modules persist raw responses to `data/bronze/...` for traceability

Rationale: API failures/rate limits/auth issues are operational concerns; cleaning/joining is data logic. Separating them reduces debugging cost.

## OAuth2 token strategy

TDX uses the client credentials flow:

1. Exchange `client_id/client_secret` for an `access_token`
2. Refresh before expiration (tokens are short-lived)

Key choices:

- token is stored on the client instance (no globals)
- expiration uses `expires_in` with a safety skew (e.g., 60s)

## Retries and error handling

- Retry transient failures (network, 5xx, 429) with backoff.
- Treat 4xx (especially 401/403) as auth/permission issues; fail fast.
- Preserve error context in exceptions for downstream logging/alerting.

## Code location

Implementation lives in:

- `src/metrobikeatlas/ingestion/tdx_base.py`
- `src/metrobikeatlas/config/`
