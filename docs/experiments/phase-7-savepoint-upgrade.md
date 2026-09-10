# Experiment: savepoint-based upgrade & rescale via the Flink Operator

**Setup:** `make k8s-up` (kind + cert-manager + Flink Operator + images + chart),
then wait for the jobs to build state:

```bash
make k8s-jobs                       # all FlinkSessionJobs -> lifecycleState: STABLE
kubectl -n flowfleet get pods
make k8s-ui                         # Flink UI on localhost:8081
```

Let `driver-state` run a few minutes so it has meaningful keyed state (one
`ValueState` entry per active driver), and the `timescale-sink` job has written
rows.

```bash
kubectl -n flowfleet exec sts/flowfleet-timescaledb -- \
  psql -U flowfleet -d flowfleet -c "select count(*) from driver_state;"
```

## 1. Rescale a job — operator does stop-with-savepoint → restore

```bash
scripts/k8s.sh upgrade driver-state 3      # parallelism 1 -> 3
```

That patches `spec.job.parallelism`. The operator:

1. calls `stop-with-savepoint` on the running job (JM REST API);
2. records the savepoint in `status.jobStatus.savepointInfo`;
3. resubmits the job at parallelism 3 with `--fromSavepoint <that path>`.

**Watch it:**

```bash
kubectl -n flowfleet get flinksessionjob flowfleet-driver-state -w
#   lifecycleState:  STABLE -> UPGRADING -> DEPLOYING -> STABLE
kubectl -n flowfleet get flinksessionjob flowfleet-driver-state \
  -o jsonpath='{.status.jobStatus.savepointInfo.lastSavepoint.location}{"\n"}'
```

In the Flink UI the job's parallelism is now 3 and "Restored from savepoint" shows
the path.

**Verify exactly-once across the upgrade:**

```bash
# row count must not jump by more than the ~30s of events between the last
# checkpoint and the savepoint (which the idempotent sink then collapses on replay)
kubectl -n flowfleet exec sts/flowfleet-timescaledb -- \
  psql -U flowfleet -d flowfleet -c \
  "select count(*), max(event_time) from driver_state;"
```

The `driver-state` output for any given `(driver_id, event_time)` is identical
before and after — `ON CONFLICT DO NOTHING` absorbs the small reprocessed tail.

## 2. Config change — same flow, triggered by a spec edit

```bash
kubectl -n flowfleet patch flinkdeployment flowfleet-session --type=merge \
  -p '{"spec":{"flinkConfiguration":{"execution.checkpointing.interval":"15s"}}}'
```

Changing the session `FlinkDeployment` recreates the cluster; each
`FlinkSessionJob` is savepointed and restored onto the new cluster. Same
`lifecycleState` transitions, same state-preservation guarantee.

## 3. Manual savepoint (backup, not upgrade)

```bash
make k8s-savepoint JOB=geofence
kubectl -n flowfleet get flinksessionjob flowfleet-geofence \
  -o jsonpath='{.status.jobStatus.savepointInfo.savepointHistory[*].location}{"\n"}'
```

`kubernetes.operator.savepoint.history.max.count/age` (in
`k8s/operators/flink-operator-values.yaml`) bound how many are kept — old ones
are disposed automatically.

## 4. Roll back

```bash
scripts/k8s.sh upgrade driver-state 1      # back to parallelism 1
```

Under Argo CD this is `git revert` of the CR change — Argo re-syncs the previous
spec and the operator restores from the matching savepoint.

## 5. Break it on purpose — incompatible state

Rename a keyed-state descriptor in `DriverStateFunction`, rebuild
(`make k8s-images`), bump the job's `restart-nonce` annotation. The operator takes
the savepoint fine but the new code can't restore it:

```
status.reconciliationStatus.state: ROLLED_BACK   (or error)
```

Recovery: set that job's `upgradeMode: stateless` (accept the state loss) or
restore from an *older* compatible savepoint via `spec.job.initialSavepointPath`.

## Results (fill in from your run)

```
date:
1. rescale 1->3:   savepoint size ___ | downtime ___ s | driver_state rows before/after: ___ / ___
                   distinct (driver_id,event_time) rows changed: 0
2. ckpt interval:  cluster recreate ___ s | all 6 jobs restored: y/n
3. manual sp:      geofence savepoint location: ___
5. bad state:      reconciliationStatus: ___ | recovered via: stateless / older savepoint
```
