# FlowFleet on Kubernetes (Phase 7)

The same stack as `docker-compose.yml`, but as a Helm chart, with the Flink jobs
run by the **Flink Kubernetes Operator** and the whole thing delivered by
**Argo CD**.

```
helm/flowfleet/     the platform chart — datastores, Kafka, Schema Registry, the
                    Spring services, TimescaleDB + ClickHouse, and the Flink
                    session cluster (FlinkDeployment) + one FlinkSessionJob per job
argocd/             app-of-apps: Flink Operator (wave -2) -> kube-prometheus-stack
                    (wave -1) -> flowfleet chart (wave 0)
k8s/operators/      Helm values for the Flink Kubernetes Operator
k8s/monitoring/     Helm values for kube-prometheus-stack
k8s/kind-cluster.yaml   local single-node cluster (reach services via port-forward)
scripts/k8s.sh      kind up/down, build+load images, operators, deploy, savepoint demo
```

## Local cluster (kind)

Prereqs: `docker`, `kind`, `kubectl`, `helm`.

```bash
make k8s-up        # kind create + cert-manager + Flink Operator + build/load images + helm install
make k8s-jobs      # FlinkDeployment / FlinkSessionJob status
make k8s-ui        # port-forward the Flink UI to localhost:8081
make k8s-down
```

`make k8s-up` runs `scripts/k8s.sh up`, which:

1. creates the `flowfleet` kind cluster (`k8s/kind-cluster.yaml`);
2. builds the 5 images and `kind load`s them (no registry needed — the chart uses
   `pullPolicy: IfNotPresent` under `values-kind.yaml`);
3. installs **cert-manager** (the operator's webhook needs it) and the
   **Flink Kubernetes Operator** (`watchNamespaces: [flowfleet]`);
4. `helm upgrade --install flowfleet ./helm/flowfleet -f values-kind.yaml`.

The Flink jobs need a *remote* jar URI (the operator resolves `local://` on its
own pod, not ours), so the chart ships a tiny in-cluster artifact server that
copies the jar out of `flowfleet/flink` and serves it over HTTP — the
self-contained stand-in for S3/GCS. Set `flink.jarURI` to skip it.

## Why the Flink Kubernetes Operator

| | Without the operator | With the operator |
|---|---|---|
| Deploy a job | `flink run` against a session cluster, by hand | `kubectl apply` a `FlinkSessionJob` |
| Upgrade a job | stop-with-savepoint, redeploy, `--fromSavepoint` by hand | change the CR — operator does savepoint → restore |
| JM crash | manual restart, hope HA was configured | operator reconciles to desired state |
| Rescale | cancel + resubmit with new parallelism | bump `parallelism` in the CR |
| GitOps | not really possible | the CR *is* the desired state — Argo syncs it |

The operator turns "a running Flink job" into a declarative resource with a
reconcile loop, which is what makes savepoint-based upgrades and GitOps possible.

## Session vs application mode

This chart uses **session mode**: one `FlinkDeployment` (the cluster) + six
`FlinkSessionJob`s sharing it. Trade-off:

* **session** — one JobManager, TaskManagers created on demand, jobs share slots.
  Cheaper for many small jobs (fits on a laptop kind). A bad job can still
  disrupt neighbours; jar must be fetched remotely.
* **application** — one `FlinkDeployment` *per job*, each its own JM+TM,
  `local://` jar from the image, full isolation. The production default for
  independent pipelines — costs one JM per job.

Switching a job to application mode is a `FlinkDeployment` with a `job:` block;
see `docs/interview-questions/phase-7.md`.

## Argo CD

```bash
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
kubectl apply -f argocd/project.yaml
kubectl apply -f argocd/root.yaml          # app-of-apps
```

`argocd/root.yaml` points Argo at `argocd/applications/`, which holds one
`Application` per layer with `argocd.argoproj.io/sync-wave` ordering so the CRDs
exist before the CRs that need them. `20-flowfleet.yaml` has `ignoreDifferences`
for the fields the operator writes back (savepoint history, internal version) so
Argo doesn't show permanent drift.

## The savepoint-upgrade experiment

`docs/experiments/phase-7-savepoint-upgrade.md` — change a job, watch the
operator take a savepoint, redeploy, and restore state; verify with the
downstream row counts.

```bash
make k8s-savepoint JOB=driver-state          # trigger one, print its location
scripts/k8s.sh upgrade driver-state 3         # rescale -> savepoint -> restore
```
