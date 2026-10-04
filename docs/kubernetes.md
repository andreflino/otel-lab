# Running the lab on Kubernetes (k3s)

A complete walkthrough: from an empty VM to the whole lab running on a single-node k3s cluster, monitored
end to end. The manifests are in [`k8s/`](../k8s/README.md).

Placeholders used below: `<node-ip>` (the k3s VM), `<monitoring-host>` (where Tempo, Loki and Prometheus run),
`<dns-server>` (your local DNS, e.g. Pi-hole).

## 1. The VM

Any Linux VM works. Mine is a Proxmox VM with 4 vCPU (CPU type `host`), 6 GB RAM and an 80 GB disk, Ubuntu 24.04.

- **RAM:** the full lab uses ~3.3 GB on the node; 6 GB leaves headroom, 8 GB is comfortable.
- **Disk:** Ubuntu's installer often gives the root LV only half the disk. Grow it:
  `sudo lvextend -r -l +100%FREE /dev/ubuntu-vg/ubuntu-lv`
- **IP:** reserve the VM's address in your DHCP server; DNS names and Prometheus depend on it.

## 2. Install k3s

```bash
curl -sfL https://get.k3s.io | sudo INSTALL_K3S_EXEC="--write-kubeconfig-mode 644 --node-name k3s" sh -
mkdir -p ~/.kube && cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
kubectl get nodes
```

In ~30 seconds you have a working cluster with CoreDNS, Traefik (ingress), ServiceLB, the local-path storage
provisioner and metrics-server. If `helm-install-traefik` shows `Error` once, ignore it: it raced the API server
and retries on its own.

### Access from your workstation

```bash
ssh <node> 'cat /etc/rancher/k3s/k3s.yaml' \
  | sed -e 's#https://127.0.0.1:6443#https://<node-ip>:6443#' -e 's/: default$/: k3s-lab/' \
  > ~/.kube/k3s-lab.yaml && chmod 600 ~/.kube/k3s-lab.yaml
export KUBECONFIG=~/.kube/k3s-lab.yaml
kubectl get nodes
```

k3s's API certificate already includes the node IP, so no TLS changes are needed.

## 3. How k3s works

k3s is certified Kubernetes in **one binary and one process**. The API server, scheduler, controller-manager,
kubelet and kube-proxy all run inside the `k3s` systemd service. State is stored in **SQLite**
(`/var/lib/rancher/k3s/server/db/`), not etcd, unless you run several servers.

| Piece | In k3s |
|---|---|
| Container runtime | Embedded containerd (`docker ps` won't show pods; use `sudo k3s crictl ps`) |
| Pod network | Flannel VXLAN, pods in `10.42.0.0/16` |
| Services | `10.43.0.0/16`; API at `10.43.0.1`, CoreDNS at `10.43.0.10` |
| Ingress | Traefik, exposed on the node's `:80`/`:443` by ServiceLB |
| Storage | `local-path`: each PVC is a directory under `/var/lib/rancher/k3s/storage/` |
| Ports | API `:6443`, kubelet `:10250`, NodePorts `30000-32767` |

A browser request goes: DNS → node `:80` (ServiceLB) → Traefik → matches the Ingress host → Service → a pod that
passed its readiness probe.

## 4. The app image

Kubernetes pulls images; it doesn't build them. `.github/workflows/image.yml` builds `app/` on every push and
publishes `ghcr.io/<owner>/otel-lab-app:latest` and `:<short-sha>`. For a public repo the package is public, so
no pull secret is needed.

## 5. Deploy

```bash
git clone https://github.com/andreflino/otel-lab.git && cd otel-lab
mkdir -p k8s-local && cat > k8s-local/kustomization.yaml <<'EOF'
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources: [../k8s]
patches:
  - target: { kind: ConfigMap, name: telemetry-backend }
    patch: |
      - { op: replace, path: /data/MONITORING_HOST, value: "<monitoring-host>" }
EOF
kubectl kustomize --load-restrictor LoadRestrictionsNone k8s-local | kubectl apply -f -
kubectl -n otel-lab get pods -w
```

`k8s-local/` is git-ignored, so your addresses never reach the repo. The first start takes a few minutes
(image pulls, Postgres seeding 2M rows).

Then:

- point `shop`, `mail`, `kafka` and `rabbitmq` `.lab.home.arpa` at `<node-ip>` in your DNS
  (Pi-hole v6: `pihole-FTL --config dns.hosts '[...]'`, appending to the existing list);
- add a Prometheus job for `<node-ip>:30889` (the in-cluster collector re-exposes app and infrastructure metrics);
- optionally install `prometheus-node-exporter` and promtail on the node. To ship pod logs, grant promtail
  read access with an ACL (`setfacl -R -m u:promtail:rX /var/log/pods` plus the same with `-d`), not a group.

## 6. Compose → Kubernetes

| Docker Compose | Kubernetes |
|---|---|
| a service | Deployment (stateless) or StatefulSet (own disk) + Service |
| `build:` | image in a registry |
| `command:` | `args:` (the image's ENTRYPOINT is `opentelemetry-instrument python`) |
| `environment:` / `.env` | ConfigMap `lab-config` + Secret `lab-secrets` |
| named volume | PVC from `volumeClaimTemplates` |
| `depends_on: service_healthy` | `wait-for-deps` init container + readiness probes |
| published ports | Ingress (by hostname) or NodePort |
| `restart: unless-stopped` | built in: kubelet restarts containers, controllers replace pods |

The collector adds **`k8sattributes`**: every span, metric and log gets `k8s.namespace.name`, `k8s.pod.name`,
`k8s.deployment.name` and `k8s.node.name`. Try `{resource.k8s.deployment.name="worker"}` in Tempo.

## 7. Operating it

```bash
kubectl get nodes -o wide
kubectl get --raw='/readyz?verbose' | tail -1          # "readyz check passed"
kubectl -n kube-system get pods
kubectl top node && kubectl -n otel-lab top pods --sort-by=memory
kubectl get events -A --sort-by=.lastTimestamp | tail -20

kubectl -n otel-lab get pods -o wide
kubectl -n otel-lab describe pod <pod>                 # probes, OOMKilled, pulls, scheduling
kubectl -n otel-lab logs deploy/worker -f
kubectl -n otel-lab logs <pod> --previous              # the crashed container
kubectl -n otel-lab logs <pod> -c wait-for-deps        # init container
kubectl -n otel-lab exec -it postgres-0 -- psql -U shop

kubectl -n otel-lab rollout restart deploy -l tier=app # pull the newest image for every app
kubectl -n otel-lab rollout undo deploy/api
kubectl -n otel-lab scale deploy/notifier --replicas=0 # Kafka lag exercise
kubectl -n otel-lab delete pod -l app=worker           # watch it come back
```

| Pod status | Meaning |
|---|---|
| `Init:0/1` | waiting for dependencies (`logs <pod> -c wait-for-deps`) |
| `Running`, `0/1` ready | readiness probe failing, no traffic |
| `CrashLoopBackOff` | app exits (`logs --previous`) |
| `OOMKilled` | memory limit reached |
| `ImagePullBackOff` | wrong image/tag or private package |

### Availability

Single node: Kubernetes heals **pods** (restarts, replacements), but a VM or disk failure takes everything down.
The manifests are in git, so the lab can be rebuilt; Postgres and Kafka data can't. Real HA needs 3 servers
(embedded etcd), replicated storage and multiple replicas.

## 8. Gotchas

- **`enableServiceLinks: false`** everywhere: Kubernetes injects `KAFKA_PORT=tcp://...`, and the apache/kafka
  image turns every `KAFKA_*` variable into broker config.
- **RabbitMQ needs a TCP readiness probe.** An exec probe (`rabbitmq-diagnostics ping`) runs as root at startup,
  creates `.erlang.cookie` owned by root, and RabbitMQ crash-loops with `eacces`.
- **Init containers instead of crash loops** while Postgres and Kafka start.
- **Labels on the Deployment, not just the pod template,** if you want `kubectl ... deploy -l ...` to work.
- **Exporter metrics skip `k8sattributes`**: they're scraped by the collector, so the "source pod" would be the
  collector itself.

## 9. Next steps

KEDA autoscaling on queue depth and consumer lag · 3 outbox relays to prove `SKIP LOCKED` · Headlamp dashboard ·
CloudNativePG and Strimzi operators · OpenTelemetry Operator auto-instrumentation · GitOps with Argo CD or Flux ·
a second node · backups of the SQLite datastore and PVCs.
