CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE TABLE orders (
    id           text PRIMARY KEY,
    item         text NOT NULL,
    qty          int  NOT NULL,
    status       text NOT NULL DEFAULT 'pending',
    created_at   timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz
);
CREATE INDEX orders_created_at_idx ON orders (created_at DESC);

-- Intentionally NO index on order_id: lookups by order are full table scans.
-- Fix later with:  CREATE INDEX ON order_events (order_id);
CREATE TABLE order_events (
    id         bigserial PRIMARY KEY,
    order_id   text NOT NULL,
    event      text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- ~2M rows of historical noise so the missing index actually hurts.
INSERT INTO order_events (order_id, event, created_at)
SELECT substr(md5(g::text), 1, 8),
       (ARRAY['created', 'completed', 'failed'])[1 + g % 3],
       now() - (g || ' seconds')::interval
FROM generate_series(1, 2000000) g;
ANALYZE order_events;

-- Chaos knob: extra pg_sleep (ms) added to some queries. Set from the web UI.
CREATE TABLE settings (key text PRIMARY KEY, value int NOT NULL);
INSERT INTO settings VALUES ('slow_ms', 0);
