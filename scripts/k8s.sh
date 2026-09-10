#!/usr/bin/env bash
# FlowFleet on Kubernetes (local kind cluster).
#
#   scripts/k8s.sh up              # kind create + operators + build/load images + helm install
#   scripts/k8s.sh images          # (re)build the 5 images and load them into kind
#   scripts/k8s.sh deploy          # helm upgrade --install only (cluster must exist)
#   scripts/k8s.sh jobs            # show FlinkDeployment / FlinkSessionJob status
#   scripts/k8s.sh savepoint <job> # trigger a savepoint for one job, print its location
#   scripts/k8s.sh upgrade <job> <parallelism>   # change a job -> operator savepoints + restores
#   scripts/k8s.sh ui | grafana    # port-forward the Flink UI / Grafana
#   scripts/k8s.sh status
#   scripts/k8s.sh down
set -euo pipefail

CLUSTER=flowfleet
NS=flowfleet
RELEASE=flowfleet
CHART="$(cd "$(dirname "$0")/.." && pwd)/helm/flowfleet"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CERT_MANAGER_VERSION=v1.16.2
FLINK_OPERATOR_VERSION=1.10.0
IMAGES=(
  "flowfleet/api:0.1.0|services/api/Dockerfile"
  "flowfleet/location-generator:0.1.0|services/location-generator/Dockerfile"
  "flowfleet/location-consumer:0.1.0|services/location-consumer/Dockerfile"
  "flowfleet/kafka-connect:0.1.0|services/connect/Dockerfile"
  "flowfleet/flink:0.1.0|flink/Dockerfile"
)

need() { command -v "$1" >/dev/null || { echo "missing: $1" >&2; exit 1; }; }

cmd_images() {
  need docker; need kind
  for spec in "${IMAGES[@]}"; do
    img="${spec%%|*}"; dockerfile="${spec##*|}"
    echo "==> build $img"
    docker build -q -f "$REPO_ROOT/$dockerfile" -t "$img" "$REPO_ROOT"
    echo "==> kind load $img"
    kind load docker-image "$img" --name "$CLUSTER"
  done
}

cmd_operators() {
  need kubectl; need helm
  echo "==> cert-manager $CERT_MANAGER_VERSION"
  kubectl apply -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
  kubectl -n cert-manager rollout status deploy/cert-manager-webhook --timeout=180s

  echo "==> Flink Kubernetes Operator $FLINK_OPERATOR_VERSION"
  helm repo add flink-operator "https://downloads.apache.org/flink/flink-kubernetes-operator-${FLINK_OPERATOR_VERSION}/" >/dev/null
  helm repo update flink-operator >/dev/null
  kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
  helm upgrade --install flink-kubernetes-operator flink-operator/flink-kubernetes-operator \
    -n flink-operator --create-namespace \
    -f "$REPO_ROOT/k8s/operators/flink-operator-values.yaml"
  kubectl -n flink-operator rollout status deploy/flink-kubernetes-operator --timeout=180s
}

cmd_deploy() {
  need helm
  helm upgrade --install "$RELEASE" "$CHART" \
    -n "$NS" --create-namespace \
    -f "$CHART/values-kind.yaml" \
    --wait --timeout 10m
  echo
  echo "jobs:"; cmd_jobs
}

cmd_up() {
  need kind; need kubectl; need helm
  if ! kind get clusters | grep -qx "$CLUSTER"; then
    kind create cluster --name "$CLUSTER" --config "$REPO_ROOT/k8s/kind-cluster.yaml"
  fi
  cmd_images
  cmd_operators
  cmd_deploy
}

cmd_jobs() {
  kubectl -n "$NS" get flinkdeployment,flinksessionjob -o wide 2>/dev/null || \
    echo "(Flink CRDs not installed yet — run: scripts/k8s.sh operators)"
}

sessionjob() { echo "${RELEASE}-flowfleet-${1}"; }

cmd_savepoint() {
  local job="${1:?job name (driver-state|driver-speed|geofence|anomaly|timescale-sink|clickhouse-sink)}"
  local name; name="$(sessionjob "$job")"
  local nonce; nonce="$(( $(date +%s) % 100000 ))"
  echo "==> triggering savepoint for $name (nonce=$nonce)"
  kubectl -n "$NS" patch flinksessionjob "$name" --type=merge \
    -p "{\"spec\":{\"job\":{\"savepointTriggerNonce\":$nonce}}}"
  echo "==> watch:  kubectl -n $NS get flinksessionjob $name -o jsonpath='{.status.jobStatus.savepointInfo.lastSavepoint}'"
  sleep 5
  kubectl -n "$NS" get flinksessionjob "$name" \
    -o jsonpath='{.status.jobStatus.savepointInfo}{"\n"}' || true
}

cmd_upgrade() {
  local job="${1:?job name}" par="${2:?new parallelism}"
  local name; name="$(sessionjob "$job")"
  echo "==> setting $name parallelism -> $par (operator: stop-with-savepoint, redeploy from it)"
  kubectl -n "$NS" patch flinksessionjob "$name" --type=merge \
    -p "{\"spec\":{\"job\":{\"parallelism\":$par}}}"
  kubectl -n "$NS" wait --for=jsonpath='{.status.lifecycleState}'=STABLE \
    "flinksessionjob/$name" --timeout=300s || true
  kubectl -n "$NS" get flinksessionjob "$name" -o wide
}

cmd_ui()      { echo "http://localhost:8081"; kubectl -n "$NS" port-forward "svc/$(sessionjob session)-rest" 8081:8081; }
cmd_grafana() { echo "http://localhost:3000 (admin/flowfleet)"; kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80; }

cmd_status() {
  kubectl -n "$NS" get pods,svc,flinkdeployment,flinksessionjob 2>/dev/null
}

cmd_down() { kind delete cluster --name "$CLUSTER"; }

case "${1:-}" in
  up)          cmd_up ;;
  images)      cmd_images ;;
  operators)   cmd_operators ;;
  deploy)      cmd_deploy ;;
  jobs)        cmd_jobs ;;
  savepoint)   shift; cmd_savepoint "$@" ;;
  upgrade)     shift; cmd_upgrade "$@" ;;
  ui)          cmd_ui ;;
  grafana)     cmd_grafana ;;
  status)      cmd_status ;;
  down)        cmd_down ;;
  *) grep -E '^#( |$)' "$0" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
