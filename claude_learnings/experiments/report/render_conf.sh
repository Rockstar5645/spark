#!/usr/bin/env bash
# Render report/conf-templates/* into a real SPARK_CONF_DIR and print its path.
#   render_conf.sh debug     -> for debug_run.sh (local[4], Delta from ~/.m2)
#   render_conf.sh cluster   -> for spark-shell against the standalone cluster
# Output goes to $EXTERN_ROOT/spark_conf_<mode>/ (outside the repo), regenerated every time.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/env.sh"
MODE=${1:-debug}
OUT=$EXTERN_ROOT/spark_conf_$MODE
mkdir -p "$OUT"
for f in "$EXPERIMENTS/report/conf-templates/"*; do
  name=$(basename "$f" .template)
  envsubst '${EXTERN_ROOT} ${LAN_IP} ${JAVA_HOME} ${DELTA_SPARK_JAR} ${DELTA_STORAGE_JAR}' < "$f" > "$OUT/$name"
done
echo "$OUT"
