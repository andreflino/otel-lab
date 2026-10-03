# OTel Lab

A tiny event-driven shop for learning observability with OpenTelemetry. Every request produces
**traces, metrics and logs** that are correlated with each other, from the browser down to the database.

```
loadgen ──▶ web (Flask UI) ──HTTP──▶ api (Flask) ──publish──▶ RabbitMQ ──consume──▶ worker
                                        │                                          │
                                        └──────────────▶ Postgres ◀────────────────┘

every service ──OTLP──▶ otel-collector ──▶ Tempo (traces)
                                       ──▶ Loki (logs, via OTLP)
                                       ──▶ :8889 ◀── scraped by Prometheus (metrics)
postgres-exporter :9187 ◀── scraped by Prometheus (DB metrics, pg_stat_statements)
rabbitmq          :15692 ◀── scraped by Prometheus (queue depth, consumers)
```

| Service | What it does |
|---|---|
| `web` | HTML page listing orders, an order form, order detail pages and a "slow queries" chaos switch |
| `api` | REST API: creates orders in Postgres and publishes an `order.created` event |
| `worker` | Consumes events, simulates payment + stock reservation, marks orders completed/failed |
| `loadgen` | Browses and places orders every 1–4 s so there's always data |
| `postgres` | Order store. Logs every query slower than 50 ms |
| `rabbitmq` | Message broker (management UI on :15672, `guest`/`guest`) |
| `otel-collector` | Receives OTLP from all services and fans it out |
| `postgres-exporter` | Postgres metrics for Prometheus |

Instrumentation is **zero-code** (`opentelemetry-instrument`): Flask, requests, psycopg2, pika and
logging are instrumented automatically. A few manual spans (`process_order`, `charge_payment`,
`reserve_stock`) and custom metrics (`orders_created`, `orders_processed`, `order_processing_seconds`)
show how to add your own.

## Built-in problems to find

| Problem | Where | How it shows up |
|---|---|---|
| ~5% of orders rejected (503) | `api` | Error spans, ERROR logs, 5xx rate |
| ~10% of payments declined | `worker` | `process_order` spans with error status, `orders_processed_total{status="failed"}` |
| Missing index on `order_events.order_id` (2M rows) | order detail page | Slow `SELECT` spans, Postgres slow-query logs, `pg_stat_statements` |
| Chaos knob: extra `pg_sleep` on queries | web UI buttons (off / 500 ms / 2000 ms) | Latency spikes everywhere downstream |

## Running it

Requirements: Docker with Compose, and an observability backend with Tempo (OTLP gRPC :4317),
Loki 3.x (OTLP :3100, `allow_structured_metadata: true`) and Prometheus.

```bash
cp .env.example .env        # set MONITORING_HOST to the machine running Tempo + Loki
docker compose up -d --build
```

Open the shop at `http://<host>:8000`. The first start takes ~15 s longer while Postgres seeds 2M rows.

Add these scrape jobs to Prometheus:

```yaml
  - job_name: otel-lab
    static_configs:
      - targets: ["<lab-host>:8889"]
  - job_name: otel-lab-postgres
    static_configs:
      - targets: ["<lab-host>:9187"]
  - job_name: otel-lab-rabbitmq
    static_configs:
      - targets: ["<lab-host>:15692"]
```

Optional but recommended:

- **Tempo metrics generator** (`service-graphs`, `span-metrics`) with remote-write to Prometheus
  (`--web.enable-remote-write-receiver`) gives you a service map and RED metrics from traces.
- **Grafana data source links**: Tempo → Loki (traces to logs), Loki derived field on `trace_id`
  (logs to traces), Tempo service map → Prometheus.
- Postgres logs go to journald (`logging.driver: journald`); ship them with promtail/Alloy to see
  slow-query logs in Loki.

## Exercises

1. **Follow one order end to end.** In Grafana → Explore → Tempo, search `service.name = worker`.
   Open a trace: it starts in `loadgen`, goes through `web` and `api`, crosses RabbitMQ
   (`orders send` → `orders receive`) and ends in `worker`. Context crosses the queue in AMQP headers.
2. **Logs ↔ traces.** In Loki, query `{service_name="worker"} |= "failed"`, expand a line and jump to its trace.
3. **Find the slow query.** Open a few order detail pages. Find the slow `SELECT ... order_events` span,
   then confirm with `pg_stat_statements` metrics and the Postgres slow-query logs. Fix it:
   ```bash
   docker compose exec postgres psql -U shop -c "CREATE INDEX ON order_events (order_id);"
   ```
   and watch the latency drop.
4. **Chaos.** Click "2000 ms" in the UI. Watch p95 latency of `web`, `api` and `worker` climb,
   and the RabbitMQ queue depth grow while the worker falls behind. Turn it off and watch it drain.
5. **Kill the consumer.** `docker compose stop worker`: orders stay `pending`, messages pile up
   in RabbitMQ (`http://<host>:15672`). Start it again and watch it catch up.
6. **Service map.** With the Tempo metrics generator enabled, open Tempo → Service Graph.

Useful PromQL:

```promql
# Request rate and p95 latency per service (from the HTTP instrumentation)
sum by (service_name) (rate(http_server_duration_milliseconds_count[1m]))
histogram_quantile(0.95, sum by (le, service_name) (rate(http_server_duration_milliseconds_bucket[5m])))

# Order outcomes
sum by (status) (rate(orders_processed_total[5m]))

# Messages waiting in RabbitMQ (rises when the worker falls behind)
sum(rabbitmq_queue_messages_ready)

# Slowest statements by total time spent
topk(5, sum by (queryid) (rate(pg_stat_statements_seconds_total[5m])))
```

> All credentials in this repo (`shop`/`shop`, `guest`/`guest`) are local lab defaults. Don't expose
> these ports to the internet.
