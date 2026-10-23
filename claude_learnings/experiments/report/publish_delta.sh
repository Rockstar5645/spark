#!/usr/bin/env bash
# Rebuild the local delta checkout and publish it to ~/.m2 as $DELTA_LOCAL_VERSION (jar + sources
# jar). Run after editing anything in $DELTA_SRC. Both consumers read that one copy:
#   - spark/examples/pom.xml depends on it (provided), so Metals maps Delta classes to source
#     and breakpoints in Delta files bind;
#   - the rendered spark-defaults.conf puts the same jars in spark.jars, so the debug run
#     executes exactly what the debugger shows.
# Writes only ~/.m2/repository/io/delta/*/$DELTA_LOCAL_VERSION/ and delta's own target/ dirs.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/env.sh"
cd "$DELTA_SRC"
./build/sbt -Dsbt.supershell=false "set ThisBuild / version := \"$DELTA_LOCAL_VERSION\"" \
  storage/publishM2 spark/publishM2 < /dev/null
ls -la "$DELTA_SPARK_JAR" "${DELTA_SPARK_JAR%.jar}-sources.jar"
