"""Outbox relay: moves rows from the `outbox` table to Kafka, then marks them published.

Guarantee: at-least-once. If the relay dies after Kafka acknowledged the message but before the
UPDATE commits, the row is still unpublished and gets sent again -> consumers must be idempotent.
Turn on "relay crash rate" in the UI to see exactly that.
"""
import json
import logging
import random
import time

from opentelemetry import metrics, propagate, trace

from cache import r
from db import cursor
from events import producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("outbox-relay")
logging.getLogger("kafka").setLevel(logging.WARNING)

tracer = trace.get_tracer("outbox-relay")
meter = metrics.get_meter("outbox-relay")
relayed = meter.create_counter("outbox_relayed", description="Outbox rows published to Kafka")
crashes = meter.create_counter("outbox_relay_crashes", description="Simulated relay crashes after publish")


def _backlog(_options):
    with cursor() as cur:
        cur.execute("""SELECT count(*) AS n, coalesce(extract(epoch FROM now() - min(created_at)), 0) AS age
                       FROM outbox WHERE published_at IS NULL""")
        row = cur.fetchone()
    yield metrics.Observation(row["n"], {"measure": "rows"})
    yield metrics.Observation(float(row["age"]), {"measure": "oldest_age_seconds"})


meter.create_observable_gauge("outbox_backlog", callbacks=[_backlog],
                              description="Unpublished outbox rows and age of the oldest one")

kafka = producer()
log.info("outbox relay started")

while True:
    try:
        # One row per transaction. Batching rows in one transaction looks faster, but then a crash on any row
        # rolls back the "published" mark of the whole batch, and at a high crash rate it never makes progress.
        with cursor() as cur:
            # SKIP LOCKED lets several relays run side by side without publishing the same row twice.
            cur.execute("""SELECT id, topic, key, payload, traceparent FROM outbox
                           WHERE published_at IS NULL ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED""")
            row = cur.fetchone()
            if row:
                # Continue the trace that started in the signup request.
                with tracer.start_as_current_span("outbox publish", context=propagate.extract(row["traceparent"]),
                                                  kind=trace.SpanKind.PRODUCER) as span:
                    span.set_attribute("outbox.id", row["id"])
                    kafka.send(row["topic"], key=row["key"], value=row["payload"]).get(timeout=10)

                if random.random() < float(r.get("signup:relay_crash_rate") or 0):
                    # Kafka has the message, but we "die" before recording it -> it will be sent again.
                    crashes.add(1)
                    raise RuntimeError(f"simulated relay crash after publishing outbox id={row['id']}")

                cur.execute("UPDATE outbox SET published_at = now() WHERE id = %s", (row["id"],))
                relayed.add(1, {"topic": row["topic"]})
                log.info("relayed outbox id=%s", row["id"])
        if not row:
            time.sleep(0.5)  # nothing to do
    except Exception as e:  # the transaction rolled back: the row stays unpublished and is retried
        log.error("relay failed: %s", e)
        time.sleep(0.2)
