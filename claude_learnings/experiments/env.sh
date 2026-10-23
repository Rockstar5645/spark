#!/usr/bin/env bash
# Shared settings for every script under claude_learnings/experiments/. Source it; don't run it.
# Everything machine-specific lives here, overridable from the environment.
#
# Layout assumed (all siblings under EXTERN_ROOT):
#   $EXTERN_ROOT/spark                 this repo, branch akhil-trace
#   $EXTERN_ROOT/delta                 delta-io/delta fork, branch supported-delta (v3.3.2 based)
#   $EXTERN_ROOT/celeborn              apache/celeborn fork (v0.7.0), only for experiments/celeborn
#   $EXTERN_ROOT/saved_delta_tables    synthetic tables written by report/setup_tables.scala
#   $EXTERN_ROOT/{spark_work,spark_tmp,spark_logs,spark_events,celeborn_data}   runtime dirs

_EXP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# experiments/ -> claude_learnings/ -> spark/ -> EXTERN_ROOT
export EXTERN_ROOT="${EXTERN_ROOT:-$(cd "$_EXP_DIR/../../.." && pwd)}"
export EXPERIMENTS="$_EXP_DIR"
export SPARK_SRC="${SPARK_SRC:-$EXTERN_ROOT/spark}"
export DELTA_SRC="${DELTA_SRC:-$EXTERN_ROOT/delta}"
export CELEBORN_SRC="${CELEBORN_SRC:-$EXTERN_ROOT/celeborn}"
export REPORT_TABLES_ROOT="${REPORT_TABLES_ROOT:-$EXTERN_ROOT/saved_delta_tables/report}"

# IP the standalone cluster / Celeborn bind to. 127.0.0.1 = only this machine can reach the UIs.
# Set it to your LAN IP if you want to open the UIs from another machine.
export LAN_IP="${LAN_IP:-127.0.0.1}"

# Spark 3.5 + Delta 3.3 are built and run on JDK 17.
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"

# Delta built from the local checkout and published to ~/.m2 under this version
# (report/publish_delta.sh). Both the examples build target and spark.jars use it.
export DELTA_LOCAL_VERSION="${DELTA_LOCAL_VERSION:-3.3.2-akhil}"
export DELTA_SPARK_JAR="$HOME/.m2/repository/io/delta/delta-spark_2.12/$DELTA_LOCAL_VERSION/delta-spark_2.12-$DELTA_LOCAL_VERSION.jar"
export DELTA_STORAGE_JAR="$HOME/.m2/repository/io/delta/delta-storage/$DELTA_LOCAL_VERSION/delta-storage-$DELTA_LOCAL_VERSION.jar"

# Port plan: 61000-61199, outside the Linux ephemeral range (32768-60999).
export DEBUG_PORT="${DEBUG_PORT:-61060}"
