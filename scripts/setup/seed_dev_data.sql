-- ═══════════════════════════════════════════════════════════════════════════════
-- LOCAL DEVELOPMENT ONLY — never run against staging or production.
--
-- Mounted by docker-compose as /docker-entrypoint-initdb.d/02_seed_dev.sql, after
-- init_postgres.sql. Seeds a well-known API key so `make smoke-test` and the
-- integration tests work out of the box.
--
-- Key value: dev-api-key-retailnexus-2026
-- Real environments create keys with scripts/deployment/create_api_key.py, which
-- generates a random key and stores only its hash.
-- ═══════════════════════════════════════════════════════════════════════════════
\c lakehouse

INSERT INTO api.api_keys (key_hash, key_prefix, client_name, permissions, expires_at)
VALUES (
    encode(sha256('dev-api-key-retailnexus-2026'::bytea), 'hex'),
    'dev-',
    'Local Development Key',
    '["*"]'::jsonb,
    NOW() + INTERVAL '1 year'
) ON CONFLICT (key_hash) DO UPDATE SET expires_at = EXCLUDED.expires_at, is_active = TRUE;
