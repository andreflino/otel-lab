"""Kafka helpers. Topics are append-only logs: `order-events` (order outcomes) and `user-signups`."""
import json
import logging
import os
import time

from kafka import KafkaConsumer, KafkaProducer
from opentelemetry import context, propagate
from kafka.errors import NoBrokersAvailable

TOPIC = "order-events"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
log = logging.getLogger("events")


def _retry(factory):
    while True:
        try:
            return factory()
        except NoBrokersAvailable:
            log.warning("kafka unavailable, retrying in 3s")
            time.sleep(3)


def producer() -> KafkaProducer:
    return _retry(lambda: KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v, default=str).encode(),
    ))


def trace_context(msg) -> context.Context:
    """The producer's trace context, carried in the Kafka message headers (W3C `traceparent`).

    The kafka-python instrumentation records a `receive` span but does not leave it active while your
    code handles the message, so handler spans must be parented explicitly or they start a new trace.
    """
    return propagate.extract({k: v.decode() for k, v in (msg.headers or []) if v is not None})


def consumer(group_id: str) -> KafkaConsumer:
    return consumer_for(TOPIC, group_id)


def consumer_for(topic: str, group_id: str) -> KafkaConsumer:
    # Each consumer group gets its own copy of every event and tracks its own offsets.
    return _retry(lambda: KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP,
        group_id=group_id,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b),
    ))
