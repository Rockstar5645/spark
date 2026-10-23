#!/usr/bin/env bash
# Render celeborn/conf-templates/* into $CELEBORN_SRC/dist/conf/ (after building dist/).
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/env.sh"
OUT=$CELEBORN_SRC/dist/conf
[[ -d "$OUT" ]] || { echo "no $OUT: build celeborn dist first (see README.md)"; exit 1; }
for f in "$EXPERIMENTS/celeborn/conf-templates/"*; do
  envsubst '${EXTERN_ROOT} ${LAN_IP} ${JAVA_HOME}' < "$f" > "$OUT/$(basename "$f" .template)"
done
ls -la "$OUT"/celeborn-defaults.conf "$OUT"/celeborn-env.sh
