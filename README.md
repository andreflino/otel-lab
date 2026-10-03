# OTel Lab

A small event-driven system for learning **observability (OpenTelemetry)** and **distributed-systems
patterns** hands-on. Everything produces correlated **traces, metrics and logs**, and every component has
a built-in way to break it so you can watch the failure in Grafana.

What you can learn with it:

| Topic | Where |
|---|---|
| Distributed tracing across HTTP, RabbitMQ and Kafka | every flow |
| Slow-query hunting (missing index, `pg_stat_statements`, slow-query logs) | order detail page |
| Caching with Redis (cache-aside, TTL, invalidation, hit ratio) | order detail page |
| Queue vs. log: RabbitMQ work queue vs. Kafka topic with consumer groups, partitions, lag | orders → worker → Kafka |
| **The dual-write problem and the transactional outbox** (lost / ghost / duplicate messages, idempotency) | signup lab |

## Architecture

```
                    ┌──────────────────────── Orders ─────────────────────────┐
loadgen ─▶ web ─HTTP─▶ api ──publish──▶ RabbitMQ ──consume──▶ worker ──▶ Kafka "order-events"
             │          │  ▲                                    │             ├─▶ notifier   (group "notifier")
             │          ▼  │ cache-aside                        │             └─▶ analytics  (group "analytics") ─▶ Redis ZSET
             │        Postgres ◀── Redis ◀── invalidate ────────┘
             │
             │      ┌──────────────────────── Signup lab ─────────────────────────────────┐
             └─HTTP─▶ signup ──(naive: direct publish)──────────────▶ Kafka "user-signups" ─▶ email-service ─SMTP─▶ Mailpit
                       │                                                ▲                        │
                       └─(outbox: users + outbox rows, 1 tx)─▶ Postgres ─▶ outbox-relay ─────────┘   (idempotent via sent_emails)

Telemetry: every service ─OTLP─▶ otel-collector ─▶ Tempo (traces), Loki (logs); Prometheus scrapes the collector (:8889)
           and the exporters: postgres (:9187), rabbitmq (:15692), redis (:9121), kafka (:9308)
```

| Service | Port | What it does |
|---|---|---|
| `web` | 8000 | Shop UI, order pages, chaos + cache switches, **signup lab** page |
| `api` | 5000 | Orders REST API. Postgres + Redis cache-aside. Publishes `order.created` to RabbitMQ |
| `worker` | – | RabbitMQ consumer. Processes orders, invalidates cache, publishes outcome to Kafka |
| `notifier` | – | Kafka consumer group `notifier`. Simulates order-status emails |
| `analytics` | – | Kafka consumer group `analytics`. Live "top items" ranking in a Redis sorted set |
| `signup` | 5001 | Signup API with three write strategies (see below) and a consistency checker |
| `outbox-relay` | – | Publishes `outbox` rows to Kafka (at-least-once) |
| `email-service` | – | Kafka consumer group `email-service`. Idempotent confirmation emails over SMTP |
| `loadgen` | – | Shops, opens order pages, signs users up and clicks confirmation links |
| `postgres` | 5432 | Source of truth. Logs every query slower than 50 ms |
| `redis` | – | Cache (TTL keys, `volatile-lru`) + analytics data structures + runtime switches |
| `rabbitmq` | 15672 | Work queue for orders. Management UI (`guest`/`guest`) |
| `kafka` | – | Single-node KRaft broker. Topics `order-events`, `user-signups` (3 partitions) |
| `kafka-ui` | 8080 | Browse topics, partitions, messages, consumer groups and lag |
| `mailpit` | 8025 | Fake inbox: read confirmation emails and click their links |
| `otel-collector` | 8889 | Receives OTLP, fans out to Tempo / Loki, exposes metrics |
| `*-exporter` | 9121, 9187, 9308 | Redis, Postgres, Kafka metrics for Prometheus |

Instrumentation is mostly **zero-code** (`opentelemetry-instrument`: Flask, requests, psycopg2, redis, pika,
kafka-python, jinja2, logging), plus manual spans and metrics where they teach something.

## The signup lab: dual write vs. transactional outbox

Signing up must **store the user** (Postgres) **and publish an event** (Kafka) so a confirmation email is sent.
Postgres and Kafka cannot share a transaction, so any crash between the two writes leaves them disagreeing.

| Mode | Flow | Failure between the writes causes |
|---|---|---|
| `naive-db-first` | INSERT user, COMMIT → publish | **Lost email**: user exists, never gets an email, can never confirm |
| `naive-kafka-first` | publish → INSERT user | **Ghost email**: email sent for a user that doesn't exist (link → 404) |
| `outbox` | INSERT user **+** INSERT outbox row in **one transaction** → `outbox-relay` publishes later | Nothing inconsistent. Relay crashes cause **duplicates** (at-least-once), so the consumer must be **idempotent** |

Switches on `http://<host>:8000/signup`: mode, crash rate between the writes, outbox-relay crash rate,
email idempotency on/off. The page (and the `signup_inconsistencies` metric) shows **lost**, **ghost**
and **duplicated** counts computed by reconciling `users` against `sent_emails`.

Expected results (10 signups each):

| Setup | Lost | Ghost | Duplicates |
|---|---|---|---|
| `naive-db-first`, crash 100% | all | 0 | 0 |
| `naive-kafka-first`, crash 100% | 0 | all | 0 |
| `outbox`, crash 100% | 0 (both writes roll back, client gets 500) | 0 | 0 |
| `outbox`, relay crash 30%, idempotency **on** | 0 | 0 | 0 (duplicates detected and skipped) |
| `outbox`, relay crash 30%, idempotency **off** | 0 | 0 | some users get 2+ emails |

## Running it

Requirements: Docker with Compose, and an observability backend with Tempo (OTLP gRPC :4317),
Loki 3.x (OTLP on :3100, `allow_structured_metadata: true`) and Prometheus. About 2.5 GB RAM.

```bash
cp .env.example .env        # set MONITORING_HOST, PUBLIC_URL and MAILPIT_URL
docker compose up -d --build
```

The first start seeds 2M rows into Postgres (~15 s). Prometheus scrape jobs:

```yaml
  - job_name: otel-lab            # app metrics via the collector
    static_configs: [{ targets: ["<lab-host>:8889"] }]
  - job_name: otel-lab-postgres
    static_configs: [{ targets: ["<lab-host>:9187"] }]
  - job_name: otel-lab-rabbitmq
    static_configs: [{ targets: ["<lab-host>:15692"] }]
  - job_name: otel-lab-redis
    static_configs: [{ targets: ["<lab-host>:9121"] }]
  - job_name: otel-lab-kafka
    static_configs: [{ targets: ["<lab-host>:9308"] }]
```

Recommended backend extras: Tempo **metrics generator** (`service-graphs`, `span-metrics`) remote-writing to
Prometheus (`--web.enable-remote-write-receiver`) for the service map; Grafana data-source links Tempo ↔ Loki
(`trace_id`) and Tempo → Prometheus; ship Postgres container logs (journald) to Loki for slow-query logs.

## Exercises

1. **Follow an order end to end.** Tempo: `{resource.service.name="notifier"}`. One trace: loadgen → web → api
   → RabbitMQ → worker → Kafka → notifier and analytics.
2. **Logs ↔ traces.** Loki: `{service_name="worker"} |= "failed"` → View trace, and back.
3. **Slow query.** Find the slow `SELECT ... order_events` span, confirm with `pg_stat_statements` and the
   Postgres slow log, fix with `CREATE INDEX ON order_events (order_id);`.
4. **Cache.** Open the same order page twice: "served from database" then "cache". Turn the cache off and
   compare latency. Comment out `cache.invalidate()` in `worker.py` to see stale statuses.
5. **Chaos.** Slow queries 2000 ms → latency climbs, RabbitMQ queue backs up, then drains.
6. **Kafka consumer groups.** `docker compose stop notifier` → its lag grows (Kafka UI or
   `kafka_consumergroup_lag`) while `analytics` keeps up. Start it: it resumes from its committed offset.
7. **Dual write.** Run the signup-lab table above and watch `signup_inconsistencies` in Grafana.
8. **Kafka outage.** `docker compose stop kafka`. `naive-db-first` signups fail after saving the user (lost);
   `outbox` signups succeed and `outbox_backlog` grows. Start Kafka and watch the backlog drain.

Useful queries:

```promql
histogram_quantile(0.95, sum by (le, service_name) (rate(http_server_duration_milliseconds_bucket[5m])))
sum by (result) (rate(cache_requests_total[5m]))                 # cache hit / miss / bypass
sum by (consumergroup, topic) (kafka_consumergroup_lag)          # Kafka consumer lag
sum(rabbitmq_queue_messages_ready)                               # RabbitMQ backlog
max by (kind) (signup_inconsistencies)                           # lost / ghost / duplicated
max by (measure) (outbox_backlog)                                # outbox rows + oldest age
sum by (result) (rate(emails_total[5m]))                         # sent vs duplicate_skipped
topk(5, sum by (queryid) (rate(pg_stat_statements_seconds_total[5m])))
```

```logql
{service_name=~"api|worker"} | detected_level="error"
{service_name="email-service"} |= "duplicate"
{container="otel-lab-postgres-1"} |= "duration"
```

## Lessons baked into the code

- **psycopg2 spans vanish** if you pass `cursor_factory` to `conn.cursor()`; set it on the connection (`db.py`).
- **Kafka consumer spans don't become the active context** in kafka-python's instrumentation; handlers must
  extract the trace context from the message headers or they start a new trace (`events.trace_context`).
- **The outbox relay commits one row per transaction.** Batching rows in one transaction means one crash rolls
  back the whole batch's "published" marks, and at high failure rates the relay never makes progress
  (`outbox_relay.py`).
- **Trace context is stored in the outbox row** so the async publish continues the original request's trace.

> All credentials here (`shop`/`shop`, `guest`/`guest`) are local lab defaults. Don't expose these ports to the internet.
