# Interview questions — Phase 7 (Kubernetes, Flink Operator, Helm, Argo CD)

Answerable from `helm/flowfleet/`, `argocd/`, `k8s/`, `scripts/k8s.sh`, and the
savepoint-upgrade experiment log.

## The Flink Kubernetes Operator

* **What does it actually give you over `flink run`?** It makes a Flink job a
  declarative custom resource with a reconcile loop. Upgrades, rescales, config
  changes and JM-crash recovery become "edit the CR"; the operator does the
  stop-with-savepoint / restore / pod management. Without it every one of those is
  a manual savepoint dance, and GitOps is impossible.
* **`FlinkDeployment` vs `FlinkSessionJob`?** `FlinkDeployment` is a Flink cluster
  (application mode = cluster + its one job; session mode = just the cluster).
  `FlinkSessionJob` is a job submitted to an existing session `FlinkDeployment`.
* **`upgradeMode`:**
  * `stateless` — redeploy, no state carried.
  * `savepoint` — stop-with-savepoint, redeploy `--fromSavepoint`. Clean,
    exactly-once, needs the job healthy and `state.savepoints.dir` set.
  * `last-state` — restore from the last checkpoint without a clean stop; the
    escape hatch for a wedged job. **Application `FlinkDeployment` only** — not
    session jobs.
* **How does the operator take the savepoint on upgrade?** It calls the running
  job's `stop-with-savepoint` via the JM REST API, records the location in the
  CR status (`savepointInfo`), then submits the new spec pointing at it.
* **native vs standalone mode?** native = the JobManager talks to the K8s API and
  creates TaskManager pods on demand (elastic, fewer moving parts). standalone =
  the operator creates a fixed TM Deployment (works without giving Flink K8s
  access; needed for reactive/standalone autoscaling). This chart uses native.

## Session vs application mode

* **Why session here?** Six jobs share one JobManager and an on-demand TM pool —
  fits a single kind node. Application mode is one JM per job (~6× the JM RAM).
* **Trade-off?** Session = cheaper, shared blast radius, jar must be fetched from
  a remote URI. Application = full isolation, independent lifecycle/scaling,
  `local://` jar from the image — Flink's recommendation for production pipelines.
* **Why can't `FlinkSessionJob` use a `local://` jar?** The operator resolves the
  URI with Flink's filesystem layer *on the operator pod*, then uploads the jar
  to the session cluster. `local://` would look on the operator's filesystem, not
  our image. So the chart serves the jar over HTTP from a tiny in-cluster pod
  (the stand-in for S3/GCS).

## State & HA on Kubernetes

* **Where do checkpoints/savepoints live?** `flink.storage`: a `ReadWriteOnce`
  PVC (`file:///flink-data/...`, fine on one node) or `s3://` (any node can
  restore — the real-cluster choice; RWO can only attach to one node).
* **JobManager HA?** `high-availability.type: kubernetes` — leader election and
  JM metadata (latest checkpoint pointer, job graph) in ConfigMaps, so a new JM
  pod resumes without losing the running job. Required before `jobManager.replicas > 1`.
* **What RBAC does a Flink job need?** In native mode the JM creates TM pods and
  HA ConfigMaps, so its ServiceAccount needs create/patch/delete on
  pods/configmaps/services (+ deployments) in the namespace — `flink-rbac.yaml`.

## Helm

* **Chart layout choice?** One first-party template per component, no vendored
  subcharts — the chart `helm lint`s and `helm template | kubeconform`s offline
  in CI with no repo access. Datastores are plain single-replica StatefulSets
  that mirror compose; a real deployment swaps in the Bitnami/operator charts.
* **How is config shared?** One `ConfigMap` (`flowfleet-env`) + one `Secret`,
  every workload `envFrom` both. `checksum/config` pod annotations roll the
  Deployments when the ConfigMap changes.
* **The DB init SQL is in two places — why?** `database/*/init.sql` is canonical
  (compose mounts it); the chart needs a copy under `files/` for `.Files.Get`.
  `make helm-sync` copies them and CI fails if they drift (`git diff --exit-code`).
* **`values-kind.yaml`?** A second value set: tiny resource requests, `pullPolicy:
  IfNotPresent` (images are `kind load`ed, never pulled), parallelism 1, no
  ServiceMonitors (kind usually has no Prometheus Operator).

## Argo CD / GitOps

* **App-of-apps?** One root `Application` points at `argocd/applications/`; each
  file there is an `Application` for one layer. Adding a component = commit a file.
* **Sync waves?** `argocd.argoproj.io/sync-wave` orders the roots: Flink Operator
  (-2) → kube-prometheus-stack (-1) → flowfleet chart (0), so CRDs exist before
  the CRs that use them.
* **Why `ignoreDifferences` on the Flink CRs?** The operator writes back into the
  spec/status (savepoint history, `$internal.flink.version`, trigger nonces).
  Without ignoring those, Argo shows permanent drift and self-heal fights the
  operator.
* **`ServerSideApply=true` on the operator app?** Its CRDs exceed the 262 kB
  `last-applied-configuration` annotation limit that client-side apply uses.
* **How do you roll back a bad job upgrade?** `git revert` the CR change; Argo
  re-syncs the previous `parallelism`/image, the operator restores from the
  matching savepoint. The savepoint history (`kubernetes.operator.savepoint.history.*`)
  bounds how far back you can go.

## The 3 a.m. question: "a Flink job won't come back after an upgrade"

`kubectl -n flowfleet get flinksessionjob <job> -o yaml` → `.status`:
`lifecycleState` / `jobStatus.state` / `error`. Common causes: savepoint dir not
writable (PVC full / wrong perms), state-schema incompatibility (the operator
restores from a savepoint the new code can't read → `reconciliationStatus` error,
fall back to `last-state` or a fresh `stateless` deploy), parallelism > available
slots (no TM can be scheduled — check TM pod events / resource quota), or the
artifact server down (jar 404 on submit).
