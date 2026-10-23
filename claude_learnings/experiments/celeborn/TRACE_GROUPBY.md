# Tracing `celeborn_groupby.scala` through Spark

All paths below are relative to `extern_repos/spark/`. Every trace line is
`logError("=== AKHIL [Sn] ...")`. ERROR level is used only so the lines show at the shell's
default WARN log level. They are not real errors.

**Lines 18-25 of the script run nothing.** `range`, `withColumn`, `groupBy` and `agg` each
return a new Dataset that wraps a bigger *logical plan*. Nothing touches data until
`.collect()` on line 26. That's Spark's laziness, and the trace order below makes it visible.

## Order the traces fire in

| # | where | runs on | what it means |
|---|---|---|---|
| S1 | `sql/core/.../sql/Dataset.scala` `groupBy` | driver | wraps the DataFrame plus the grouping columns. **No plan node yet** |
| S2 | `sql/core/.../sql/RelationalGroupedDataset.scala` `toDF` | driver | `.agg(...)` creates the logical `Aggregate` node. Fires twice: once for line 24, once for line 25's outer `.agg` |
| S3 | `Dataset.scala` `withAction` | driver | **`.collect()` triggers it. Everything before this was lazy** |
| S4 | `sql/core/.../execution/QueryExecution.scala` `optimizedPlan` | driver | the logical plan after Catalyst's rule rewrites |
| S5 | `.../execution/SparkStrategies.scala` `Aggregation` | driver | strategy matching: logical `Aggregate` → physical operators. Twice, one per Aggregate |
| S6 | `.../execution/aggregate/AggUtils.scala` `createAggregate` | driver | ×4: **Partial** then **Final** for each Aggregate. Final has `requiredChildDistribution=Some(group_key)` |
| S7 | `.../execution/exchange/EnsureRequirements.scala` | driver | Final needs rows grouped by key, but its child doesn't provide that, so a **shuffle is inserted** here. This is where the shuffle is born |
| S8 | `QueryExecution.scala` `executedPlan` | driver | the final physical tree. Read it bottom-up |
| S9 | `.../execution/aggregate/HashAggregateExec.scala` `doProduceWithKeys` | driver | **generates Java source** for the hash-agg loop. The per-row work on executors is that generated code |
| S10 | `.../execution/exchange/ShuffleExchangeExec.scala` `shuffleDependency` | driver | registers the shuffle with the ShuffleManager. The handle class shows built-in or Celeborn |
| — | `core/.../scheduler/DAGScheduler.scala` (existing `[6b]` `[7]` `[8]`) | driver | splits the job into stages at each shuffle and submits tasks |
| S11 | `core/.../shuffle/ShuffleWriteProcessor.scala` `write` | **executor** | map task: **the seam where the ShuffleManager is used**. Prints the writer class |
| — | `core/.../shuffle/sort/SortShuffleWriter.scala` (existing `[10a]` `[10b]`) | executor | only in the built-in run, and only when the sort writer is chosen |
| S12 | `sql/core/.../execution/ShuffledRowRDD.scala` `compute` | **executor** | reduce task: gets a reader from the ShuffleManager. Prints the reader class |
| — | `core/.../shuffle/BlockStoreShuffleReader.scala` (existing `[11]`) | executor | only in the built-in run: fetches blocks from every mapper |

**Driver traces** (S1–S10, [6b]–[8]) print in the `spark-shell` terminal.
**Executor traces** (S11, S12, [10x], [11]) do NOT. They're in
`extern_repos/spark_work/<app-id>/<executor-id>/stderr`.

## Verified 2026-10-12: tiny built-in run (100k rows, 1k keys)

Log: `logs/trace_tiny_builtin.log`. Executor traces: `spark_work/app-20261012235421-0003/*/stderr`.
Result: groups=1000, rows=100000.

Corrections, from what actually fired:

- **Line 23 hits a different `groupBy` than the one I first traced.** `groupBy("group_key")` takes a
  String, so it goes to the `groupBy(col1: String, cols: String*)` overload
  (`Dataset.scala` ~1810), not `groupBy(cols: Column*)`. That's why S1 printed `cols=`
  (empty). That S1 came from line 25's outer `.agg`, which is `groupBy()` with no columns.
  `[S1s]` is now on the String overload. It was added after this run and isn't verified yet.
- **S9 never fired.** The physical plan uses **`SortAggregate`** for the keyed groupBy, not
  HashAggregate. S6 shows `useHash=false`, because `max(payload)` over a string has a
  non-fixed-width buffer, which HashAggregate can't hold (`Aggregate.supportsHashAggregate`).
  The plan is: Sort → SortAggregate(partial) → **Exchange hashpartitioning(group_key, 16)** →
  Sort → SortAggregate(final). Only the global count at the end is a HashAggregate, and with
  `keys=[]` that uses `doProduceWithoutKeys`. So S9 can't fire in this script. Use
  `sum`/`count` only (no `max(string)`) to get HashAggregate.
- **Two shuffles**, not one: shuffleId=0 is `hashpartitioning(group_key, 16)` (the real
  one), and shuffleId=1 is `SinglePartition`, which funnels the 16 results to one task for the
  global count.
- Writer: `BypassMergeSortShuffleWriter` (≤200 partitions, no map-side combine), **not**
  `SortShuffleWriter`. So the existing `[10a]/[10b]` traces don't fire either.
- Reader: `BlockStoreShuffleReader`, as expected.

## What to compare between the two runs

- S10 handle: `BaseShuffleHandle`/`BypassMergeSortShuffleHandle` (built-in) vs a Celeborn handle.
- S11 writer: `SortShuffleWriter`/`BypassMergeSortShuffleWriter` vs Celeborn's `HashBasedShuffleWriter`.
- S12 reader: `BlockStoreShuffleReader` vs `CelebornShuffleReader`.
- S1–S9 should be **identical**: Celeborn changes only the shuffle transport, never the plan.

## Not traced (yet)

- The generated Java itself. Dump it with `df.queryExecution.debug.codegen()`.
- Celeborn's side: client push (`HashBasedShuffleWriter.write0`) and worker receive (`PushDataHandler.handlePushData`). Next step.
