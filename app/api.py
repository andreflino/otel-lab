"""Orders API: stores orders in Postgres and publishes an `order.created` event to RabbitMQ."""
import json
import logging
import os
import random
import uuid

import pika
from flask import Flask, jsonify, request
from opentelemetry import metrics, trace

from db import chaos_sleep, cursor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("api")
logging.getLogger("pika").setLevel(logging.WARNING)

meter = metrics.get_meter("api")
orders_created = meter.create_counter("orders_created", description="Orders accepted by the API")

app = Flask(__name__)


def publish(event: dict) -> None:
    # A short-lived connection per publish keeps the example simple (and thread-safe).
    conn = pika.BlockingConnection(pika.ConnectionParameters(os.getenv("RABBITMQ_HOST", "rabbitmq")))
    try:
        ch = conn.channel()
        ch.queue_declare(queue="orders", durable=True)
        ch.basic_publish(exchange="", routing_key="orders", body=json.dumps(event, default=str),
                         properties=pika.BasicProperties(content_type="application/json", delivery_mode=2))
    finally:
        conn.close()


@app.post("/orders")
def create_order():
    data = request.get_json(force=True)
    item, qty = data.get("item", "keyboard"), int(data.get("qty", 1))

    # Simulated dependency failure (~5%) so there are errors to look at.
    if random.random() < 0.05:
        log.error("order rejected: inventory service unavailable item=%s", item)
        return jsonify(error="inventory unavailable"), 503

    order_id = uuid.uuid4().hex[:8]
    trace.get_current_span().set_attribute("order.id", order_id)
    with cursor() as cur:
        cur.execute("INSERT INTO orders (id, item, qty) VALUES (%s, %s, %s) RETURNING *", (order_id, item, qty))
        order = cur.fetchone()
        cur.execute("INSERT INTO order_events (order_id, event) VALUES (%s, 'created')", (order_id,))

    publish({"type": "order.created", "order": order})
    orders_created.add(1, {"item": item})
    log.info("order created id=%s item=%s qty=%s", order_id, item, qty)
    return jsonify(order), 201


@app.get("/orders")
def list_orders():
    with cursor() as cur:
        chaos_sleep(cur)
        cur.execute("SELECT * FROM orders ORDER BY created_at DESC LIMIT 20")
        return jsonify(cur.fetchall())


@app.get("/orders/<order_id>")
def get_order(order_id):
    trace.get_current_span().set_attribute("order.id", order_id)
    with cursor() as cur:
        cur.execute("SELECT * FROM orders WHERE id = %s", (order_id,))
        order = cur.fetchone()
        if not order:
            return jsonify(error="not found"), 404
        # Deliberately slow: order_events has ~2M rows and no index on order_id (full table scan).
        # Fix it with: CREATE INDEX ON order_events (order_id);
        cur.execute("SELECT event, created_at FROM order_events WHERE order_id = %s ORDER BY created_at", (order_id,))
        order["history"] = cur.fetchall()
        return jsonify(order)


@app.get("/settings/slow")
def get_slow():
    with cursor() as cur:
        cur.execute("SELECT value FROM settings WHERE key = 'slow_ms'")
        return jsonify(slow_ms=cur.fetchone()["value"])


@app.post("/settings/slow")
def set_slow():
    ms = max(0, min(int(request.get_json(force=True).get("slow_ms", 0)), 10000))
    with cursor() as cur:
        cur.execute("UPDATE settings SET value = %s WHERE key = 'slow_ms'", (ms,))
    log.warning("chaos: slow query delay set to %sms", ms)
    return jsonify(slow_ms=ms)


@app.get("/health")
def health():
    return {"ok": True}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)
