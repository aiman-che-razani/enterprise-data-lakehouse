"""
Create an API key for a real environment (staging / production).

Generates a random key, stores ONLY its SHA-256 hash in api.api_keys, and prints the
plaintext once — it cannot be recovered afterwards.

Usage:
    DATABASE_URL="host=... dbname=lakehouse user=... password=..." \\
        python scripts/deployment/create_api_key.py --name grafana \\
        --permission read:pipeline --permission read:quality --expires-days 90

Permissions: read:pipeline, read:quality, read:catalog, or "*" (all). Keys should be
scoped to the caller's needs and given an expiry.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import sys

VALID_PERMISSIONS = {"*", "read:pipeline", "read:quality", "read:catalog"}


def generate_key() -> str:
    return "rn_" + secrets.token_urlsafe(32)


def hash_key(plaintext: str) -> str:
    return hashlib.sha256(plaintext.encode()).hexdigest()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--name", required=True, help="client name, e.g. grafana")
    p.add_argument(
        "--permission",
        action="append",
        dest="permissions",
        required=True,
        choices=sorted(VALID_PERMISSIONS),
    )
    p.add_argument("--expires-days", type=int, default=90, help="0 = never expires (discouraged)")
    args = p.parse_args(argv)
    if args.expires_days < 0:
        p.error("--expires-days must be >= 0")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL (libpq DSN) is required", file=sys.stderr)
        return 2

    import psycopg2

    key = generate_key()
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO api.api_keys (key_hash, key_prefix, client_name, permissions, expires_at)
            VALUES (%s, %s, %s, %s::jsonb,
                    CASE WHEN %s > 0 THEN NOW() + (%s * INTERVAL '1 day') END)
            """,
            (
                hash_key(key),
                key[:6],
                args.name,
                json.dumps(sorted(set(args.permissions))),
                args.expires_days,
                args.expires_days,
            ),
        )
    print(f"API key for '{args.name}' (shown once, store it in your secret manager):\n{key}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
