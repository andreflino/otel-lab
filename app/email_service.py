"""Email service: Kafka consumer group `email-service` that sends signup confirmation emails via SMTP (Mailpit).

Idempotency: the event id is the primary key of `sent_emails`. A redelivered event (from an outbox relay
crash or a consumer restart) hits ON CONFLICT DO NOTHING and is skipped. Turn idempotency off in the UI
to see duplicate emails land in Mailpit.
"""
import logging
import os
import smtplib
from email.message import EmailMessage

from opentelemetry import metrics, trace

from cache import r
from db import cursor
from events import consumer_for, trace_context

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("email-service")
logging.getLogger("kafka").setLevel(logging.WARNING)

SMTP_HOST = os.getenv("SMTP_HOST", "mailpit")
PUBLIC_URL = os.getenv("PUBLIC_URL", "http://localhost:8000")

tracer = trace.get_tracer("email-service")
meter = metrics.get_meter("email-service")
emails = meter.create_counter("emails", description="Confirmation emails by result (sent / duplicate_skipped)")


def send_email(to: str, link: str):
    with tracer.start_as_current_span("smtp send", kind=trace.SpanKind.CLIENT) as span:
        span.set_attributes({"server.address": SMTP_HOST, "server.port": 1025})
        msg = EmailMessage()
        msg["From"], msg["To"], msg["Subject"] = "OTel Lab Shop <no-reply@otel-lab.local>", to, "Confirm your account"
        msg.set_content(f"Welcome! Confirm your account here:\n\n{link}\n")
        with smtplib.SMTP(SMTP_HOST, 1025, timeout=10) as smtp:
            smtp.send_message(msg)


for msg in consumer_for("user-signups", "email-service"):
    event = msg.value
    with tracer.start_as_current_span("handle user.signed_up", context=trace_context(msg),
                                      kind=trace.SpanKind.CONSUMER) as span:
        span.set_attributes({"event.id": event["event_id"], "user.email": event["email"]})

        if r.get("signup:idempotent") != "0":
            with cursor() as cur:
                cur.execute("""INSERT INTO sent_emails (event_id, user_id, email) VALUES (%s, %s, %s)
                               ON CONFLICT (event_id) DO NOTHING""",
                            (event["event_id"], event["user_id"], event["email"]))
                first_time = cur.rowcount == 1
            if not first_time:
                emails.add(1, {"result": "duplicate_skipped"})
                span.set_attribute("email.duplicate", True)
                log.warning("duplicate event skipped event_id=%s email=%s", event["event_id"], event["email"])
                continue
        else:
            with cursor() as cur:  # still record it (for the consistency checker), but never skip
                cur.execute("""INSERT INTO sent_emails (event_id, user_id, email) VALUES (%s, %s, %s)
                               ON CONFLICT (event_id) DO NOTHING""",
                            (event["event_id"], event["user_id"], event["email"]))

        send_email(event["email"], f"{PUBLIC_URL}/confirm/{event['token']}")
        with cursor() as cur:
            cur.execute("INSERT INTO email_log (event_id, user_id) VALUES (%s, %s)",
                        (event["event_id"], event["user_id"]))
        emails.add(1, {"result": "sent"})
        log.info("confirmation email sent email=%s event_id=%s partition=%s offset=%s",
                 event["email"], event["event_id"], msg.partition, msg.offset)
