"""Redis helpers: cache-aside for orders, a runtime on/off switch, and hit/miss stats for the UI."""
import json
import os

import redis
from opentelemetry import metrics

r = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), decode_responses=True)

ORDER_TTL = 30  # seconds a cached order lives before it expires on its own

meter = metrics.get_meter("cache")
cache_requests = meter.create_counter("cache_requests", description="Order cache lookups by result (hit/miss/bypass)")


def order_key(order_id: str) -> str:
    return f"cache:order:{order_id}"


def enabled() -> bool:
    return r.get("settings:cache_enabled") != "0"  # on unless explicitly turned off


def set_enabled(on: bool) -> None:
    r.set("settings:cache_enabled", "1" if on else "0")


def record(result: str) -> None:
    cache_requests.add(1, {"result": result})
    r.incr(f"stats:cache:{result}")


def get_order(order_id: str):
    raw = r.get(order_key(order_id))
    return json.loads(raw) if raw else None


def put_order(order: dict) -> None:
    # Only keys with a TTL can be evicted (maxmemory-policy volatile-lru), so analytics data is safe.
    r.setex(order_key(order["id"]), ORDER_TTL, json.dumps(order, default=str))


def invalidate(order_id: str) -> None:
    r.delete(order_key(order_id))


def stats() -> dict:
    hit, miss = (int(r.get(f"stats:cache:{k}") or 0) for k in ("hit", "miss"))
    return {"enabled": enabled(), "hit": hit, "miss": miss,
            "hit_ratio": round(hit / (hit + miss), 2) if hit + miss else None}
