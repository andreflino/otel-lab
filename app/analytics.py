"""Analytics: Kafka consumer group `analytics`. Keeps a live "top items" ranking in a Redis sorted set."""
import logging

from opentelemetry import trace

from cache import r
from events import consumer, trace_context

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("analytics")
logging.getLogger("kafka").setLevel(logging.WARNING)
tracer = trace.get_tracer("analytics")

for msg in consumer("analytics"):
    event = msg.value
    with tracer.start_as_current_span("update_stats", context=trace_context(msg)) as span:
        span.set_attribute("order.id", event["order_id"])
        if event["status"] == "completed":
            # Redis as a data structure server, not a cache: ZINCRBY keeps the ranking sorted for us.
            r.zincrby("analytics:top_items", event["qty"], event["item"])
        r.hincrby("analytics:status", event["status"], 1)
        log.info("stats updated order=%s item=%s status=%s", event["order_id"], event["item"], event["status"])
