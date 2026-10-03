"""Signup service: demonstrates the dual-write problem and the transactional outbox fix.

Signing up must (1) store the user in Postgres and (2) publish `user.signed_up` to Kafka so the
email service sends a confirmation email. Postgres and Kafka can't share a transaction, so:

  naive-db-first     INSERT user, COMMIT, then publish  -> crash in between = LOST email
  naive-kafka-first  publish, then INSERT user          -> DB failure after  = GHOST email
  outbox             INSERT user + INSERT outbox row in ONE transaction; outbox_relay.py publishes later

Knobs (stored in Redis, changed from the web UI): signup:mode, signup:failure_rate.
"""
import json
import logging
import random
import secrets
import uuid

from flask import Flask, jsonify, request
from opentelemetry import metrics, propagate, trace

from cache import r
from db import cursor
from events import producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("signup")
logging.getLogger("kafka").setLevel(logging.WARNING)

TOPIC = "user-signups"
MODES = ("naive-db-first", "naive-kafka-first", "outbox")
LOST_AFTER_SECONDS = 30  # a user without a sent email after this long counts as "lost"

tracer = trace.get_tracer("signup")
meter = metrics.get_meter("signup")
signups = meter.create_counter("signups", description="Signup attempts by mode and result")

app = Flask(__name__)
kafka = None


class SimulatedCrash(Exception):
    pass


def setup_schema():
    with cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id         uuid PRIMARY KEY,
                email      text NOT NULL,
                token      text NOT NULL UNIQUE,
                confirmed  boolean NOT NULL DEFAULT false,
                mode       text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT now()
            );
            -- The transactional outbox: written in the same transaction as the user row.
            CREATE TABLE IF NOT EXISTS outbox (
                id           bigserial PRIMARY KEY,
                topic        text NOT NULL,
                key          text NOT NULL,
                payload      jsonb NOT NULL,
                traceparent  jsonb NOT NULL DEFAULT '{}',
                created_at   timestamptz NOT NULL DEFAULT now(),
                published_at timestamptz
            );
            CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
            -- Owned by the email service; it records every email it sent (also its idempotency key).
            CREATE TABLE IF NOT EXISTS sent_emails (
                event_id   uuid PRIMARY KEY,
                user_id    uuid NOT NULL,
                email      text NOT NULL,
                sent_at    timestamptz NOT NULL DEFAULT now()
            );
            CREATE TABLE IF NOT EXISTS email_log (
                id         bigserial PRIMARY KEY,
                event_id   uuid NOT NULL,
                user_id    uuid NOT NULL,
                sent_at    timestamptz NOT NULL DEFAULT now()
            );
        """)


def settings() -> dict:
    return {
        "mode": r.get("signup:mode") or "outbox",
        "failure_rate": float(r.get("signup:failure_rate") or 0),
        "relay_crash_rate": float(r.get("signup:relay_crash_rate") or 0),
        "idempotent": r.get("signup:idempotent") != "0",
    }


def maybe_crash(where: str, rate: float):
    if random.random() < rate:
        trace.get_current_span().add_event("simulated crash", {"where": where})
        raise SimulatedCrash(f"simulated crash {where}")


def publish(event: dict):
    global kafka
    kafka = kafka or producer()
    kafka.send(TOPIC, key=event["email"], value=event).get(timeout=10)  # wait for the broker ack


def insert_user(cur, user: dict, mode: str):
    cur.execute("INSERT INTO users (id, email, token, mode) VALUES (%s, %s, %s, %s)",
                (user["user_id"], user["email"], user["token"], mode))


@app.post("/signup")
def signup():
    body, cfg = request.get_json(force=True), settings()
    email = body["email"].strip().lower()
    mode = body.get("mode") or cfg["mode"]
    span = trace.get_current_span()
    span.set_attributes({"signup.mode": mode, "user.email": email})

    # The event carries everything the email service needs, including a unique event id (idempotency key).
    event = {"event_id": str(uuid.uuid4()), "type": "user.signed_up", "user_id": str(uuid.uuid4()),
             "email": email, "token": secrets.token_urlsafe(16)}
    try:
        if mode == "naive-db-first":
            with cursor() as cur:
                insert_user(cur, event, mode)
            # <-- the user is committed; if we die here, nobody will ever send the email
            maybe_crash("after DB commit, before Kafka publish", cfg["failure_rate"])
            publish(event)

        elif mode == "naive-kafka-first":
            publish(event)
            # <-- the email is on its way; if the DB write fails now, it points at a user that doesn't exist
            maybe_crash("after Kafka publish, before DB insert", cfg["failure_rate"])
            with cursor() as cur:
                insert_user(cur, event, mode)

        elif mode == "outbox":
            carrier = {}
            propagate.inject(carrier)  # keep the trace going when the relay publishes later
            with cursor() as cur:      # ONE transaction: both rows commit, or neither does
                insert_user(cur, event, mode)
                cur.execute("INSERT INTO outbox (topic, key, payload, traceparent) VALUES (%s, %s, %s, %s)",
                            (TOPIC, email, json.dumps(event), json.dumps(carrier)))
                maybe_crash("inside the transaction (rolls back both writes)", cfg["failure_rate"])
        else:
            return jsonify(error=f"unknown mode {mode}"), 400

    except SimulatedCrash as e:
        signups.add(1, {"mode": mode, "result": "crashed"})
        log.error("signup crashed email=%s mode=%s: %s", email, mode, e)
        return jsonify(error=str(e), mode=mode), 500

    signups.add(1, {"mode": mode, "result": "ok"})
    log.info("signup ok email=%s mode=%s user_id=%s", email, mode, event["user_id"])
    return jsonify(user_id=event["user_id"], email=email, mode=mode), 201


@app.get("/confirm/<token>")
def confirm(token):
    with cursor() as cur:
        cur.execute("UPDATE users SET confirmed = true WHERE token = %s RETURNING email", (token,))
        row = cur.fetchone()
    if not row:
        # This is what a GHOST email looks like to a real user.
        log.error("confirmation for unknown token=%s (ghost email: user was never saved)", token)
        return jsonify(error="account does not exist"), 404
    log.info("account confirmed email=%s", row["email"])
    return jsonify(confirmed=row["email"])


def consistency() -> dict:
    """Reconcile users (signup's data) against sent_emails (email service's data)."""
    with cursor() as cur:
        cur.execute("""
            SELECT
              (SELECT count(*) FROM users) AS users,
              (SELECT count(*) FROM users WHERE confirmed) AS confirmed,
              (SELECT count(*) FROM users u WHERE u.created_at < now() - make_interval(secs => %s)
                 AND NOT EXISTS (SELECT 1 FROM sent_emails s WHERE s.user_id = u.id)) AS lost,
              (SELECT count(*) FROM sent_emails s
                 WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.id = s.user_id)) AS ghost,
              (SELECT count(*) FROM (SELECT user_id FROM email_log GROUP BY user_id HAVING count(*) > 1) d)
                 AS duplicated,
              (SELECT count(*) FROM outbox WHERE published_at IS NULL) AS outbox_pending
        """, (LOST_AFTER_SECONDS,))
        return dict(cur.fetchone())


def _observe(_options):
    c = consistency()
    for kind in ("lost", "ghost", "duplicated"):
        yield metrics.Observation(c[kind], {"kind": kind})


meter.create_observable_gauge("signup_inconsistencies", callbacks=[_observe],
                              description="Users without an email (lost), emails without a user (ghost), "
                                          "users emailed more than once (duplicated)")


@app.get("/signup/status")
def status():
    return jsonify(settings() | consistency())


@app.post("/signup/settings")
def update_settings():
    body = request.get_json(force=True)
    if body.get("mode") in MODES:
        r.set("signup:mode", body["mode"])
    for k in ("failure_rate", "relay_crash_rate"):
        if k in body:
            r.set(f"signup:{k}", max(0.0, min(float(body[k]), 1.0)))
    if "idempotent" in body:
        r.set("signup:idempotent", "1" if body["idempotent"] else "0")
    log.warning("signup settings changed: %s", settings())
    return jsonify(settings())


@app.post("/signup/reset")
def reset():
    with cursor() as cur:
        cur.execute("TRUNCATE users, outbox, sent_emails, email_log")
    log.warning("signup data reset")
    return jsonify(ok=True)


setup_schema()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, threaded=True)
