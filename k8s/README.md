# OTel Lab on Kubernetes (k3s)

The same system as `compose.yml`, as Kubernetes manifests (Kustomize). Tested on a single-node k3s.

## What's in here

| File | Objects |
|---|---|
| `config.yaml` | `lab-config` ConfigMap (OTel + service addresses), `lab-secrets` Secret (lab defaults), `telemetry-backend` ConfigMap (where traces/logs go) |
| `postgres.yaml`, `kafka.yaml` | StatefulSets with 5 Gi PVCs (k3s `local-path`). Kafka is single-node KRaft; `kafka.yaml` also has Kafka UI |
| `redis.yaml`, `rabbitmq.yaml`, `mailpit.yaml` | Deployments + Services |
| `exporters.yaml`, `kube-state-metrics.yaml` | Postgres, Redis, Kafka exporters and cluster-state metrics |
| `otel-collector.yaml` | Collector with `k8sattributes` (pod / deployment / node on every signal) that also scrapes the exporters; metrics on NodePort **30889** |
| `apps.yaml` | 9 app Deployments (one image, `args` picks the service), each with a `wait-for-deps` init container |
| `ingress.yaml` | `shop`, `mail`, `kafka`, `rabbitmq` `.lab.home.arpa` via Traefik |

## Deploy

```bash
# 1. a cluster (single node is fine, ~4 GB RAM for the lab)
curl -sfL https://get.k3s.io | sh -s - --write-kubeconfig-mode 644

# 2. local settings in a git-ignored overlay
mkdir -p k8s-local && cat > k8s-local/kustomization.yaml <<'EOF'
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources: [../k8s]
patches:
  - target: { kind: ConfigMap, name: telemetry-backend }
    patch: |
      - { op: replace, path: /data/MONITORING_HOST, value: "<your Tempo/Loki host>" }
EOF

# 3. apply (the flag lets the Postgres init script be shared with docker compose)
kubectl kustomize --load-restrictor LoadRestrictionsNone k8s-local | kubectl apply -f -
kubectl -n otel-lab get pods -w
```

Point `*.lab.home.arpa` (or your own names in `ingress.yaml` and `lab-config`) at the node, and add a Prometheus
scrape job for `<node-ip>:30889`.

The app image is built by `.github/workflows/image.yml` and published as `ghcr.io/<owner>/otel-lab-app`.
Roll out a new build with `kubectl -n otel-lab rollout restart deploy -l tier=app`.

## Gotchas solved here

- **`enableServiceLinks: false` on every pod.** Kubernetes injects `<SERVICE>_PORT=tcp://...` env vars; the
  `apache/kafka` image turns every `KAFKA_*` variable into broker config and fails to start.
- **RabbitMQ uses a TCP readiness probe.** An exec probe (`rabbitmq-diagnostics ping`) runs as root at startup and
  creates `.erlang.cookie` owned by root, so RabbitMQ can't read it and crash-loops with `eacces`.
- **Init containers instead of crash loops.** Apps wait for Postgres, RabbitMQ, Redis and Kafka; the outbox relay and
  email service also wait until the signup service has created its tables.
- **Exporter metrics skip `k8sattributes`.** They are scraped by the collector, so the "source pod" would be the
  collector itself; they go through a separate `metrics/infra` pipeline.
