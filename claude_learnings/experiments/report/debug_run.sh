#!/usr/bin/env bash
# Run ReportWalkthrough (spark/examples/.../sql/ReportWalkthrough.scala) under the debugger.
#
#   claude_learnings/experiments/report/debug_run.sh          # rebuild (delta if changed, examples, assembly), then run
#   claude_learnings/experiments/report/debug_run.sh --no-build
#
# local[4]: driver + scheduler + 4 task threads in ONE JVM, so one debugger sees everything,
# including executor-side code (shuffle writers/readers).
# suspend=y: the JVM waits at startup until VS Code attaches ("Attach to spark-shell driver
# (61060)"), so breakpoints on the first lines of main() are never missed.
# localhost: the debug port is not reachable from the LAN (jdwp = remote code execution).
#
# Never build while Metals/Bloop is compiling the same spark checkout: two builds writing
# the same project trash each other's output (NoSuchFileException under .bloop/, then a full
# ~8-10 min Bloop recompile).
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/env.sh"

JAR=$SPARK_SRC/examples/target/scala-2.12/jars/spark-examples_2.12-3.5.9-SNAPSHOT.jar
cd "$SPARK_SRC"

if [[ "${1:-}" != "--no-build" ]]; then
  # Delta: re-publish the local checkout (jar + sources jar) only if a source file is newer than
  # the published jar. spark.jars and the examples build target both read that jar.
  # Do NOT reflexively "Metals: Import build" after a re-publish: every import so far has cost a
  # full Bloop recompile. Import only if the debugger shows stale Delta lines.
  if [[ ! -f "$DELTA_SPARK_JAR" ]] || [[ -n "$(find "$DELTA_SRC/spark/src/main" "$DELTA_SRC/storage/src/main" \
      -type f -newer "$DELTA_SPARK_JAR" -print -quit)" ]]; then
    echo "== delta sources changed since last publish: rebuilding + publishing $DELTA_LOCAL_VERSION =="
    "$EXPERIMENTS/report/publish_delta.sh"
  else
    echo "== delta unchanged since last publish, skipping =="
  fi

  # examples/package = ReportWalkthrough.scala. assembly/package = Spark itself: without
  # SPARK_PREPEND_CLASSES, spark-submit loads core/sql/catalyst from assembly/target/.../jars,
  # so edits to Spark source only take effect after the assembly is rebuilt.
  echo "== building examples + assembly (picks up edits to ReportWalkthrough.scala AND Spark) =="
  ./build/sbt examples/package assembly/package < /dev/null
fi

# Own conf dir, generated from the templates. Deliberately WITHOUT SPARK_PREPEND_CLASSES: with it,
# every module's target/classes goes on the classpath, including sql/hive's, which registers
# HiveFileFormat as a data source while the assembly has no hive-exec jar, so the service loader
# dies with ClassNotFoundException FileSinkDesc.
CONF=$("$EXPERIMENTS/report/render_conf.sh" debug)
export SPARK_CONF_DIR=$CONF

echo "== waiting for debugger on localhost:$DEBUG_PORT (attach from VS Code now) =="
exec ./bin/spark-submit \
  --master 'local[4]' \
  --driver-memory 4g \
  --class org.apache.spark.examples.sql.ReportWalkthrough \
  --driver-java-options "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=localhost:$DEBUG_PORT" \
  "$JAR"
