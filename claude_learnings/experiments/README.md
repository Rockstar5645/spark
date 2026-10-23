# Experiments: reading Spark + Delta (+ Celeborn) under a debugger

Scripts for stepping through Spark 3.5 and Delta 3.3 line by line, in one JVM, from VS Code.
Everything machine-specific is in `env.sh` (paths derive from where this repo is checked out;
override any variable from the environment).

## Layout

All repos are siblings under one directory, `EXTERN_ROOT`:

```
EXTERN_ROOT/
  spark/        this repo, branch akhil-trace (branch-3.5 + notes + examples/ReportWalkthrough.scala)
  delta/        fork of delta-io/delta, branch supported-delta (v3.3.2 + merge instrumentation)
  celeborn/     fork of apache/celeborn @ v0.7.0 (only for celeborn/)
  saved_delta_tables/report/{sales,telemetry}   written by report/setup_tables.scala
  .vscode/      launch.json + settings.json, copied from vscode/
```

| dir | what |
|---|---|
| `env.sh` | shared settings, sourced by every script |
| `report/` | the main loop: `ReportWalkthrough` (a failure-rate report over two Delta tables) under the debugger |
| `plans/` | `logical` vs `analyzed` plan for DataFrame API vs SQL text |
| `celeborn/` | built-in sort shuffle vs Celeborn, plus the groupBy trace notes |
| `vscode/` | Metals attach config and the settings that made it work |

## Setup from scratch

JDK 17. Then:

```bash
cd $EXTERN_ROOT
git clone -b akhil-trace  git@github.com:Rockstar5645/spark.git
git clone -b supported-delta git@github.com:Rockstar5645/delta.git
source spark/claude_learnings/experiments/env.sh

# 1. Spark (~5 min). examples/ has a provided dep on delta-spark $DELTA_LOCAL_VERSION, so
#    publish Delta first or the examples module won't resolve.
spark/claude_learnings/experiments/report/publish_delta.sh        # Delta -> ~/.m2 (~1-2 min)
(cd spark && ./build/sbt examples/package assembly/package)

# 2. Tables (once). From spark/:
./bin/spark-shell --master 'local[4]' --jars "$DELTA_SPARK_JAR,$DELTA_STORAGE_JAR" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
scala> :paste ../claude_learnings/experiments/report/setup_tables.scala
```

**`:paste`, never `:load`.** `:load` runs each complete line on its own, so a multi-line chain
(`val units = spark.range(N)` then `.withColumn(...)` lines) gets split and the `.withColumn`
applies to the previous result (seen: `UNRESOLVED_COLUMN serial_number`, and a table written
with only two columns).

## VS Code + Metals

1. Open VS Code on `EXTERN_ROOT`. Copy `vscode/launch.json` and `vscode/settings.json` into
   `EXTERN_ROOT/.vscode/`.
2. Put `vscode/remote-or-user-settings.json` into **user** settings (or Remote settings over SSH).
   In workspace settings they were silently ignored.
3. "Metals: Import build" once (`customProjectRoot: spark`). First compile is ~8-10 min.

Costs learned the hard way:
- **Every "Import build" has cost a full Bloop recompile** (~8-10 min). Only import after a build
  file changed. "Restart build server" (NOT "clean and restart") restarts Bloop and keeps output.
- **Never run an sbt build of spark while Metals/Bloop is compiling it.** They collide
  (`NoSuchFileException` under `.bloop/`) and the next compile is from scratch.
- Attaching compiles the `examples` target first. It's seconds when nothing changed.

## The debug loop (`report/`)

```bash
report/debug_run.sh             # publish delta if its sources changed, build examples+assembly, run
report/debug_run.sh --no-build  # just run (only if nothing changed: stale jar = lines off by N)
```

It runs `spark-submit --master local[4]` with jdwp on `localhost:61060`, `suspend=y`, so the JVM
waits for VS Code ("Attach to spark-shell driver (61060)"). local mode = driver, scheduler and
task threads in ONE JVM, so one debugger sees executor-side code too.

- **Stale jar = wrong lines.** If the source changed since the last build, highlights and
  breakpoints are off by however many lines were added.
- Delta is stepped through from the **sources jar** published by `publish_delta.sh`, built from
  your own `delta/` checkout. Breakpoints set in the opened Delta files bind.
- Conf comes from `report/conf-templates/`, rendered by `report/render_conf.sh` into
  `EXTERN_ROOT/spark_conf_<mode>/`. No `SPARK_PREPEND_CLASSES`: it put sql/hive classes on the
  classpath without hive-exec, which fails with `ClassNotFoundException FileSinkDesc`.
- To end: Ctrl-C the script. The VS Code Stop button only detaches (the program runs to the end).

## Standalone cluster + Celeborn (`celeborn/`)

Only needed for the shuffle comparison; the debug loop doesn't use it.

```bash
export LAN_IP=127.0.0.1                     # or your LAN IP to reach the UIs from elsewhere
CONF=$(report/render_conf.sh cluster); export SPARK_CONF_DIR=$CONF
$SPARK_SRC/sbin/start-master.sh && $SPARK_SRC/sbin/start-worker.sh spark://$LAN_IP:61077
# Celeborn: build dist, then render its conf
(cd $CELEBORN_SRC && build/make-distribution.sh --sbt-enabled -Pspark-3.5)
celeborn/install_conf.sh
(cd $CELEBORN_SRC/dist && sbin/start-master.sh && sbin/start-worker.sh)
celeborn/celeborn_compare.py celeborn/celeborn_groupby.scala
```

| process | UI |
|---|---|
| Spark master / worker | `:61080` / `:61081` |
| driver (while running) | `:61040` |
| Celeborn master / worker | `:61098` / `:61096` |

Both `sbin/` dirs have a `start-master.sh`: run each from its own directory.
The Celeborn client jar must be on `spark.executor.extraClassPath` too (the ShuffleManager is
created at executor JVM start, before `spark.jars` are fetched).

## Findings so far (verified live unless marked)

- `groupBy("x")` (String) hits `Dataset.groupBy(col1: String, cols: String*)`, not the Column
  overload. `max(string)` forces `SortAggregate` (non-fixed-width buffer).
- `spark.read.format("delta").load(path)` takes the **V1** path: `DeltaTableV2` is not
  `SupportsRead`, so `DataSourceV2Utils.loadV2Source` returns `None` and
  `DataFrameReader.loadV1Source` -> `DataSource.resolveRelation` -> `DeltaDataSource.createRelation`.
- The first Delta I/O happens inside `new DeltaLog(...)` (`DeltaLog.scala:131`
  `createSnapshotAtInit`): `_last_checkpoint`, then a listing of `_delta_log/` from version 0
  (`SnapshotManagement.listFrom`). `DeltaLog` objects are cached per JVM, keyed by path.
- Protocol + Metadata come from the latest `.crc` file (no Spark job) when it exists
  (`readProtocolAndMetadataFromChecksum.enabled`, default true). The data file list is NOT in the
  crc; it needs a log replay (`Snapshot.stateReconstruction`) later.
