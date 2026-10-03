"""Notifier: Kafka consumer group `notifier`. Pretends to email the customer about each finished order."""
import logging
import random
import time

from opentelemetry import trace

from events import consumer, trace_context

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("notifier")
logging.getLogger("kafka").setLevel(logging.WARNING)
tracer = trace.get_tracer("notifier")

for msg in consumer("notifier"):
    event = msg.value
    with tracer.start_as_current_span("send_email", context=trace_context(msg)) as span:
        span.set_attribute("order.id", event["order_id"])
        span.set_attribute("messaging.kafka.partition", msg.partition)
        time.sleep(random.uniform(0.05, 0.2))  # pretend to call an email provider
        log.info("email sent order=%s status=%s partition=%s offset=%s",
                 event["order_id"], event["status"], msg.partition, msg.offset)
