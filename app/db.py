"""Shared Postgres helpers."""
import os
from contextlib import contextmanager

from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

_pool = None


def pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is None:
        # cursor_factory goes on the connection, not on cursor(): passing it per cursor would
        # bypass the OpenTelemetry psycopg2 instrumentation and produce no DB spans.
        _pool = ThreadedConnectionPool(1, 10, os.getenv("DATABASE_URL", "postgresql://shop:shop@postgres/shop"),
                                       cursor_factory=RealDictCursor)
    return _pool


@contextmanager
def cursor():
    conn = pool().getconn()
    try:
        with conn, conn.cursor() as cur:
            yield cur
    finally:
        pool().putconn(conn)


def chaos_sleep(cur) -> None:
    """Simulated slow query: sleeps for settings.slow_ms inside Postgres (0 = off)."""
    cur.execute("SELECT value FROM settings WHERE key = 'slow_ms'")
    ms = cur.fetchone()["value"]
    if ms > 0:
        cur.execute("SELECT pg_sleep(%s)", (ms / 1000.0,))
