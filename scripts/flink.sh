#!/usr/bin/env bash
# Submit / list / cancel FlowFleet Flink jobs on the local session cluster.
set -euo pipefail

JAR=/opt/flink/usrlib/flowfleet-flink-jobs.jar
JM="docker compose exec -T flink-jobmanager"
CMD="${1:-list}"

declare -A JOBS=(
  [driver-state]=com.flowfleet.flink.driverstate.DriverStateJob
  [driver-speed]=com.flowfleet.flink.speed.DriverSpeedJob
  [geofence]=com.flowfleet.flink.geofence.GeofenceJob
  [anomaly]=com.flowfleet.flink.anomaly.AnomalyJob
)

submit() {
  local name="$1"; local class="${JOBS[$name]:-}"
  [ -n "$class" ] || { echo "unknown job: $name (${!JOBS[*]})" >&2; exit 1; }
  echo "submitting $name ($class)"
  $JM flink run -d -c "$class" "$JAR"
}

case "$CMD" in
  submit)      submit "${2:?job name}" ;;
  submit-all)  for j in "${!JOBS[@]}"; do submit "$j"; done ;;
  list)        $JM flink list ;;
  cancel)      $JM flink cancel "${2:?job id}" ;;
  cancel-all)  $JM flink list -r | awk '/RUNNING/{print $4}' | xargs -r -n1 $JM flink cancel ;;
  *)           echo "usage: $0 {submit <name>|submit-all|list|cancel <id>|cancel-all}" >&2; exit 1 ;;
esac
