# Deployment — Kubernetes, the Flink Operator, Helm, Argo CD (Phase 7)

> Status: **implemented**. `helm/flowfleet/` (chart), `argocd/` (GitOps),
> `k8s/` (operator + monitoring values, kind cluster), `scripts/k8s.sh`.
> CI renders the chart and validates every manifest against real CRD schemas.

Phases 1–6 run as `docker-compose.yml`. Phase 7 is the same topology as a Helm
chart, with the Flink jobs managed by the **Flink Kubernetes Operator** and the
whole thing reconciled by **Argo CD**.

## What runs where

```
                         ┌─────────────────── namespace: flowfleet ───────────────────┐
  Argo CD ──sync──▶      │  postgres (STS)     kafka (STS, KRaft)   schema-registry     │
  (app-of-apps)          │  timescaledb (STS)  clickhouse (STS)                         │
                         │  api (Deploy x2)    location-generator   location-consumer   │
                         │                                                             │
  Flink K8s Operator ──▶ │  FlinkDeployment "flowfleet-session"  (JM + on-demand TMs)   │
  (namespace:            │    ├── FlinkSessionJob driver-state                          │
   flink-operator)       │    ├── FlinkSessionJob driver-speed                          │
                         │    ├── FlinkSessionJob geofence                              │
                         │    ├── FlinkSessionJob anomaly                               │
                         │    ├── FlinkSessionJob timescale-sink                        │
                         │    └── FlinkSessionJob clickhouse-sink                       │
                         │  flink-artifacts (serves the job jar over HTTP)              │
                         └─────────────────────────────────────────────────────────────┘
  kube-prometheus-stack (namespace: monitoring) ── scrapes ServiceMonitors / PodMonitors
```

One shared `ConfigMap` (`flowfleet-env`) carries Kafka / Schema Registry / DB
coordinates; one `Secret` carries credentials. Every workload is
`envFrom` both — a single place to repoint.

## Why the Flink Kubernetes Operator

A bare Flink job on Kubernetes is a Deployment you built by hand: you `flink run`
to submit, and every lifecycle event (upgrade, rescale, JM crash, config change)
is a manual `savepoint` + `run --fromSavepoint` dance. The operator makes the job
a **custom resource with a reconcile loop**:

| Operation | Without operator | With operator |
|-----------|------------------|---------------|
| Submit | `flink run -c … jar` | `kubectl apply` a `FlinkSessionJob` |
| Config / image change | stop-with-savepoint, redeploy, `--fromSavepoint` | edit the CR — operator does it |
| Rescale | cancel + resubmit at new parallelism | bump `spec.job.parallelism` |
| JM pod dies | manual; hope HA was set | operator reconciles to spec |
| Roll back | find the old savepoint, resubmit | `kubectl apply` the previous CR |
| GitOps | not feasible | the CR *is* desired state — Argo owns it |

`upgradeMode: savepoint` (our default): on any spec change the operator runs
**stop-with-savepoint**, then resubmits `--fromSavepoint`. Clean, exactly-once,
requires the job healthy enough to snapshot. `last-state` (restore from the last
checkpoint, no clean stop) is the escape hatch for a wedged job — **not available
for session jobs**, only application `FlinkDeployment`s.

## Session mode vs application mode

The chart runs **session mode**: one `FlinkDeployment` (the cluster) and six
`FlinkSessionJob`s sharing it.

| | session (this chart) | application (per-job `FlinkDeployment`) |
|---|---|---|
| JobManagers | 1 | 1 per job |
| TaskManagers | created on demand for slot requests | per job |
| Isolation | shared JVM/slots — a bad job can hurt neighbours | full — separate clusters |
| Jar delivery | **remote** URI, operator downloads + uploads | `local://` from the job image |
| Fits on a laptop kind | yes (one cluster) | ~6× the JM overhead |
| Flink's recommendation for prod pipelines | — | ✅ |

Session mode was chosen so the whole platform fits one kind node and to exercise
the `FlinkSessionJob` path. Moving a job to application mode is a `FlinkDeployment`
with a `job:` block and `jarURI: local:///opt/flink/usrlib/flowfleet-flink-jobs.jar`.

### The artifact server

`FlinkSessionJob.spec.job.jarURI` must be a URI Flink's filesystem layer can
fetch (`http`, `s3`, `hdfs`, …). `local://` resolves on the **operator** pod, not
our image, so it doesn't work here. The chart ships `flink-artifacts` — an
init-container copies `flowfleet-flink-jobs.jar` out of the `flowfleet/flink`
image, a `busybox httpd` serves it, and `jarURI` is the **fully-qualified**
`http://<release>-flink-artifacts.<namespace>.svc.cluster.local/flowfleet-flink-jobs.jar`
(the operator resolves it on its own pod, in the `flink-operator` namespace — a
bare service name wouldn't resolve). In a real cluster this is S3/GCS or a Maven
repo; set `flink.jarURI` and the server is skipped.

## State, checkpoints, HA

`flink.storage.type`:

* **pvc** (default, kind) — one `ReadWriteOnce` PVC mounted at `/flink-data` on
  the JM and TMs; checkpoints / savepoints / HA metadata are `file://` subpaths.
  Fine on a single node.
* **s3** — `s3://<bucket>/{checkpoints,savepoints,ha}`; the real-cluster choice
  (any node can restore; no RWO single-attach limit). Needs the `flink-s3`
  plugin in the image and `s3.*` config.

`flink.highAvailability.enabled` turns on `high-availability.type: kubernetes`
(leader election + JM metadata in ConfigMaps). Required before
`jobManager.replicas > 1`; the operator rejects multi-replica JM without it.

The session cluster's `serviceAccount` (`flowfleet-flink`) has a Role allowing
pods/configmaps/services/deployments in the namespace — in native mode the
JobManager itself creates TaskManager pods and the HA ConfigMaps.

## Argo CD — app-of-apps with sync waves

`argocd/root.yaml` is one `Application` pointing at `argocd/applications/`, which
holds one `Application` per layer:

```
wave -2  flink-kubernetes-operator   (CRDs + controller)   [needs cert-manager]
wave -1  kube-prometheus-stack       (ServiceMonitor/PodMonitor CRDs, Grafana)
wave  0  flowfleet                   (the platform chart)
```

Waves guarantee the CRDs exist before the CRs that reference them. Within the
chart, ordering is left to Kubernetes: schema-registry crash-loops until Kafka is
`Ready`, the sink jobs error until the databases accept connections — all
self-healing, no init ordering to maintain.

`20-flowfleet.yaml` sets `ignoreDifferences` for the fields the operator writes
back into the CRs (savepoint history, internal Flink version, trigger nonces) so
Argo doesn't report permanent drift and fight the operator.

## Local cluster

`scripts/k8s.sh up` → `k8s/README.md`. `make k8s-up` / `k8s-jobs` / `k8s-ui` /
`k8s-savepoint` / `k8s-down`.

## The experiment

`docs/experiments/phase-7-savepoint-upgrade.md` — rescale a job through the
operator, watch stop-with-savepoint → restore, and confirm exactly-once via the
downstream row counts. Interview questions: `docs/interview-questions/phase-7.md`.
