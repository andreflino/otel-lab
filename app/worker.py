"""Worker: consumes `order.created` events from RabbitMQ, "processes" them and updates Postgres."""
import json
import logging
import os
import random
import time

import pika
from opentelemetry import metrics, trace
from opentelemetry.trace import Status, StatusCode

from db import chaos_sleep, cursor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("worker")
logging.getLogger("pika").setLevel(logging.WARNING)

tracer = trace.get_tracer("worker")
meter = metrics.get_meter("worker")
processed = meter.create_counter("orders_processed", description="Orders processed by the worker")
duration = meter.create_histogram("order_processing_seconds", unit="s", description="Time to process an order")


def handle(ch, method, properties, body):
    order = json.loads(body)["order"]
    start = time.time()

    # Manual spans inside the auto-instrumented consumer span show where the time goes.
    with tracer.start_as_current_span("process_order") as span:
        span.set_attribute("order.id", order["id"])
        span.set_attribute("order.item", order["item"])

        with tracer.start_as_current_span("charge_payment"):
            time.sleep(random.uniform(0.05, 0.4))  # pretend to call a payment provider

        with tracer.start_as_current_span("reserve_stock"), cursor() as cur:
            chaos_sleep(cur)
            cur.execute("SELECT count(*) AS n FROM orders WHERE item = %s AND status = 'completed'", (order["item"],))

        status = "completed"
        if random.random() < 0.1:  # ~10% of orders fail
            status = "failed"
            span.set_status(Status(StatusCode.ERROR, "payment declined"))
            log.error("order failed id=%s reason=payment_declined", order["id"])
        else:
            log.info("order completed id=%s", order["id"])

        with cursor() as cur:
            cur.execute("UPDATE orders SET status = %s, processed_at = now() WHERE id = %s", (status, order["id"]))
            cur.execute("INSERT INTO order_events (order_id, event) VALUES (%s, %s)", (order["id"], status))

    processed.add(1, {"status": status})
    duration.record(time.time() - start, {"status": status})
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    params = pika.ConnectionParameters(os.getenv("RABBITMQ_HOST", "rabbitmq"), heartbeat=30)
    while True:
        try:
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            ch.queue_declare(queue="orders", durable=True)
            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue="orders", on_message_callback=handle)
            log.info("worker waiting for orders")
            ch.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            log.warning("rabbitmq unavailable, retrying in 3s")
            time.sleep(3)


if __name__ == "__main__":
    main()
