# Spark Sort-Merge Join, Shuffle, and Spill — End-to-End Trace

A consolidated walkthrough of a single sort-merge join, from physical plan
through Tungsten sort/spill to shuffle write/fetch — traced against real metrics
from `join_understanding.scala`. The through-line: **the Exchange (shuffle) is
the fundamental cost center of any non-trivial Spark job.**

The query under study:

```scala
val joined = a.join(b, a("join_key") === b("join_key"))
  .where(col("a.value") > 50)
  .select(col("a.join_key"), col("a.value").alias("value_a"), col("b.value").alias("value_b"))
joined.count()
```

- Two Delta tables, `join_key` = unique sha256 hex string (high-entropy,
  compression-hostile, skew-free by construction).
- Config forcing the plan we want: `autoBroadcastJoinThreshold=-1` (guarantee a
  shuffle join), `adaptive.enabled=false` (static plan), `shuffle.partitions=4`
  (few fat partitions → force spill), `--driver-memory 4g`.

---

## 1. The punchline first: Exchange is the cost center

Narrow transformations (`map`, `filter`, `select`, `ColumnarToRow`) are nearly
free — they **pipeline**: one row in → maybe one row out → forget it. They hold
no state, never spill, and fuse into a single generated `processNext()` loop
(whole-stage codegen).

Everything expensive — **join, `groupBy`, `distinct`, `repartition`, window,
`orderBy`** — is expensive because it forces an **Exchange** (a shuffle). The
Exchange is where four costs stack:

1. **Serialize + write** every surviving row to local disk (map side)
2. **Sort / spill** to fit the partition in memory (Tungsten external sort)
3. **Fetch** each reducer's slice across the network (reduce side)
4. **Deserialize + merge** the fetched runs back into sorted order

### The refinement that matters

"Exchange is the bottleneck" is 80% true. The precise statement:

> The shuffle is the cost center, and its severity is set by
> **bytes-shuffled × skew.**

- **Volume** — how many bytes cross the Exchange. The best shuffle optimization
  is *shuffling less*: push filters down, project columns away early, use
  partial (map-side) aggregation. In this query we shuffled `join_key` only —
  never the 256-byte `payload` (pruned at the parquet scan) and A even dropped
  `value` after the filter used it.
- **Skew** — is that volume *even* across partitions? A balanced 5 GB shuffle is
  fine; a 5 GB shuffle where one partition holds 4 GB is a disaster (one task
  runs forever, the join buffer spills or OOMs). Unique sha256 keys gave perfect
  balance here — which is *why* nothing went wrong. In production, **skew is the
  #1 shuffle killer**, and it is invisible in "total bytes."

### Reframe of "the bottleneck"

Spark is almost never CPU-bound on the sort/hash itself — it sorts 10M rows in
~2s. The pain is **data movement + the memory pressure it creates**: moving bytes
through the Exchange, and not having enough memory to hold a partition (forcing
spill I/O). The bottleneck is I/O and memory, not comparisons.

---

## 2. The physical plan, and why stages break at Exchanges

Formatted plan (`explain("formatted")`), tree portion:

```
* HashAggregate (16)            <- final count
+- Exchange (15)                <- SinglePartition shuffle of 4 partial counts
   +- * HashAggregate (14)      <- partial_count per partition
      +- * Project (13)         <- Output: []  (count needs no columns)
         +- * SortMergeJoin (12)
            :- * Sort (6)                       ┐ reduce side (stage 3)
            :  +- Exchange (5)   <- SHUFFLE A   ┘ map side ends here (stage 1)
            :     +- * Project (4)              <- drop `value` after filter
            :        +- * Filter (3)            <- value>50 (49% survive)
            :           +- * ColumnarToRow (2)
            :              +- Scan parquet (1)  <- A
            +- * Sort (11)                      ┐ reduce side (stage 3)
               +- Exchange (10)  <- SHUFFLE B   ┘ map side ends here (stage 2)
                  +- * Filter (9)               <- isnotnull(join_key) only
                     +- * ColumnarToRow (8)
                        +- Scan parquet (7)     <- B
```

**Key facts learned:**

- `*(N)` is the **WholeStageCodegen id**, NOT the stage number. Operators sharing
  `*(N)` were fused into one generated Java method.
- **Stages break at Exchanges**, not at `*(N)` changes. The three Exchanges (5),
  (10), (15) define four stages.
- The **Sorts `*(2)`/`*(4)` are on the REDUCE side** (stage 3), after the fetch —
  NOT the map side. The map side only scans/filters/partition-writes; it ships
  *unsorted* rows. Sorting happens post-fetch, in the same task as the join.
- Column ids (`#0` for A's `join_key`, `#6` for B's) thread through the entire
  plan and disambiguate the two tables even when paths are truncated. More
  reliable than the `Location:` path.
- `count()` is a **two-phase aggregation**: partial counts per partition (14) →
  tiny SinglePartition shuffle of 4 longs (15) → sum (16). You shuffle counts,
  not rows. `Project (13) Output: []` — column pruning taken to the extreme: the
  count needs no column values, so zero columns survive the join.

### What the optimizer did (Analyzed → Optimized)

Three rules, all aimed at shrinking bytes into the Exchange:

1. **Predicate pushdown** — `value > 50` moved below the join, onto A's scan.
   Filtering ~half of A *before* the shuffle. This is literally what saved A from
   spilling (see §4).
2. **Inferred `isnotnull` filters** — synthesized on both join keys, because an
   inner join can never match a null key. B got `isnotnull(join_key)` even
   though it had no `value` filter.
3. **Column pruning** — `payload` dropped at the scan (`ReadSchema` has only
   `join_key,value`); A's `value` dropped after the filter. The shuffle moves
   the minimum: one string column.

---

## 3. Streaming vs blocking operators (the unifying lens)

The distinction that explains fusion, spill, AND stage boundaries:

- **Streaming (pipelined):** can emit output after seeing ONE input row; holds
  ~no state. `Filter`, `Project`, `ColumnarToRow`, `map`, `Union`. These fuse
  into one codegen loop and **never spill** (nothing to spill).
- **Blocking (pipeline-breaking):** cannot emit its first output row until it has
  consumed all (or a large chunk of) input; must **accumulate**. `Sort` (the min
  could arrive last), `Aggregate`, the join's match buffer. These break codegen
  fusion and **can spill**.

The academic term is **"pipeline breaker"** (Neumann, VLDB 2011, *Efficiently
Compiling Efficient Query Plans for Modern Hardware* — the paper Spark's codegen
is modeled on). In the source: `BlockingOperatorWithCodegen`
(`WholeStageCodegenExec.scala:423`). `SortExec` and `HashAggregateExec` mix it in.

Three symptoms of the same idea:
- **Codegen fusion** extends exactly as far as operators stay streaming.
- **Spill** happens only in blocking operators (they accumulate).
- **Stage boundaries** fall at `Exchange`, the ultimate blocker (reduce side
  can't start until *all* map output exists — a scheduler barrier, stronger than
  an in-operator block).

---

## 4. Tungsten sort + spill (the reduce side)

The Sort operators (`*(2)`/`*(4)`) accumulate a full partition, then sort it.
This IS Project Tungsten — the `Unsafe*` classes (`org.apache.spark.util.
collection.unsafe.sort.UnsafeExternalSorter`). The `unsafe` prefix IS the
Tungsten branding. (Contrast: `ExternalSorter` without `unsafe` = the older,
object-based sorter used by the RDD API.)

### How the sorter works

1. **A row "arrives"** = `inputadapter.next()` hands back an `UnsafeRow` that is a
   **reused, mutable pointer** onto memory the producer will overwrite on the
   next `next()`.
2. Because Sort must accumulate ALL rows, it **copies** each row's bytes into its
   own **pages** (`Platform.copyMemory`) — big MB-scale memory slabs allocated
   from the `TaskMemoryManager` (NOT OS 4KB pages; Spark's own coarse allocation
   unit, often off-heap to dodge GC). Rows are packed back-to-back:
   `[len][bytes][len][bytes]...`.
3. It sorts a **`(recordAddress, prefix)` pointer array**, NOT the rows. The
   `prefix` is the first 8 bytes of the sort key packed into a long — compared
   first (cache-resident, no pointer chase); full-key comparison only on a prefix
   tie. Rows never move; only 16-byte pointer entries get reordered.
4. **Spill trigger** is NOT a fixed size threshold — it's **memory-manager
   back-pressure**: when a new page can't be granted (execution pool exhausted,
   possibly by *other* consumers in the same task), the manager calls `spill()`.
   Spill = sort the in-memory pointer array → write a **sorted run** to a file in
   `spark.local.dir` → free the pages → keep inserting into fresh pages. Each
   spill file is individually sorted. (There's also a record-count safety cap:
   `numElementsForceSpillThreshold`.)
5. **Final merge** = a **lazy k-way priority-queue merge** (`UnsafeSorterSpillMerger`)
   over the k spill files + 1 in-memory run. Produces a sorted *iterator* streamed
   directly into the join. **No merged output file is written** — that would be
   wasted I/O.

Why this matters for GC: millions of tiny `Row` objects would drown the JVM in
garbage collection. Tungsten stores raw bytes in a handful of huge pages + one
pointer array — the GC barely notices.

### The measured spill (40M-row run, 4g driver, 4 partitions)

| Side | Rows sorted | peak memory/task | spill/task | sort time/task |
|------|-------------|------------------|------------|----------------|
| **A** | 19,600,000 (post-filter) | 544 MiB | **0** | 2.4 s |
| **B** | 40,000,000 (no filter) | 512 MiB | **1280 MiB** (5 GiB total) | 4.7 s |

**Why A didn't spill and B did — NOT competition for memory.** A and B sort in
sequence within a task; A frees its pages before B runs. The real reason: **B
sorts ~2× the rows** (40M vs 19.6M). The `value>50` pushdown halved A's input
*before* the sorter — predicate pushdown paying off, measurably.

**Reading the two columns together:**
- A: **higher peak (544) + zero spill** = "I just barely fit."
- B: **lower peak (512, capped) + high spill (1280)** = "I hit the ceiling and
  was forced to disk." The inversion (more data → *lower* peak) is the signature
  of spilling. B filled ~512MB, spilled a sorted run, freed, repeated (~2-3 runs),
  then k-way merged them.

B's sort time (~2× A's) = 2× the rows PLUS the spill tax (extra serialize + disk
write + disk read-back + merge). `sort time` is "in-memory only," so merge/IO
leaks into surrounding stage time — B's true penalty is a bit higher than shown.

---

## 5. The three independent spill sites

| Operator | Spills when… | This run |
|----------|--------------|----------|
| **Sort** (`*(2)`,`*(4)`) | total row count > memory | A: 0, **B: 5 GiB** |
| **SortMergeJoin buffer** (`smj_matches_0`, `ExternalAppendOnlyUnsafeRowArray`) | one *key* matches too many rows (SKEW) | **0** (unique keys → ≤1 match/key) |
| **Map-side write** (`SortShuffleWriter`) | >200 partitions, sort-based writer | n/a (bypass writer at 4 partitions) |

**The join buffer spilled 0 by construction.** The join buffers matched rows for
the *current key* in `smj_matches_0`; unique sha256 keys → at most 1 row per key
→ nothing to accumulate. This would light up on a **skewed** key (e.g. join on
`country_code` where 40% = "US"): the merge would buffer every "US" row at once →
spill or OOM. That's the classic skewed-join pain, and it shows here, in the
**join's** spill column — NOT the Sort's.

**The fix depends on which site spills:**
- **Sort spill** → more partitions or more memory (shrink rows-per-task).
- **Join-buffer spill** → that's key skew; partitions don't help. Need salting or
  AQE skew-join handling.

---

## 6. Map-side shuffle write — two writer strategies

At the Exchange, each row is hashed: `partition = hash(join_key) % numPartitions`.
Spark picks the writer via `SortShuffleWriter.shouldBypassMergeSort`:

- **`BypassMergeSortShuffleWriter`** — when `numPartitions ≤ 200`
  (`spark.shuffle.sort.bypassMergeThreshold`) and no map-side combine. Opens **one
  file per bucket** live, appends each row to its bucket's file as it streams by,
  then **concatenates** the bucket files into one `.data` + writes the `.index`.
  **No sort, NO spill** (never accumulates in memory). Cost is *many open file
  handles* — one per partition — which is why it's capped at 200.
- **`SortShuffleWriter`** — when partitions > 200 (or map-side combine needed).
  Inserts all rows into an `ExternalSorter`, sorts by partition-id, **can spill**,
  writes one merged partitioned file.

**This run used the bypass writer** (4 partitions ≤ 200) — which is why the
`[10a]/[10b]` traces in `SortShuffleWriter` never fired. To exercise
`SortShuffleWriter` and see map-side spill, set `shuffle.partitions ≥ 201`.

Each **map task** writes ONE `.data` file (all buckets concatenated) + one
`.index`. With M map tasks and R reduce partitions → **M × R blocks**.

### Map-side metrics (A, 40M run)

```
shuffle records written: 19,600,000     <- = filter output (49% of 40M)
data size:               1495.4 MiB     <- deserialized in-memory size
shuffle bytes written:   1197.3 MiB     <- serialized + LZ4-compressed (~20% smaller)
local blocks read:       104            <- 26 map tasks × 4 reducers
number of partitions:    4
```

- **`data size` > `shuffle bytes written`** (~20% gap): serialize + compress on
  write. Gap is small because sha256 keys are high-entropy (barely compressible).
- **104 blocks** (up from 16 at 4M): 3.2 GB / 128 MB (`spark.sql.files.maxPartitionBytes`)
  ≈ 26 input splits → **26 map tasks** (ran across 4 slots in ~7 waves) × 4
  reducers = 104. Only visible at scale — 40M crossed the 128MB-split threshold.
- **Time skew, NOT data skew:** scan time (min 157ms, med 630ms, **max 4.4s**) —
  28× spread despite even row counts. This is **GC / memory contention** under
  the 4g heap, not skewed keys. *Lesson: when min/max diverge 20×+ but row counts
  are even, suspect GC/memory pressure, not skew — a completely different fix.*

---

## 7. Reduce-side fetch (BlockStoreShuffleReader, the `[11]` path)

When a stage-3 reduce task for partition `p` starts:

1. **Ask the driver for LOCATIONS, not data.** `MapOutputTracker` (driver-side)
   knows, for every map output, which executor holds it and how big partition
   `p`'s slice is. Returns `blocksByAddress` = list of (executor, block, size).
2. **Fetch only partition `p`'s slice from each map output** — the byte range
   `[offset_p, offset_p + len_p)` from each `.data` file, located via the
   `.index`. NOT whole files.
3. **Throttled + streaming** (`ShuffleBlockFetcherIterator`): splits into **local**
   (same executor → read off local disk, no network) and **remote** (Netty,
   up to `maxBytesInFlight`=48MB, backpressured). Blocks stream in as
   `(blockId, InputStream)`; doesn't wait for all before yielding the first.
4. For a **SQL** shuffle, `keyOrdering`/`aggregator` are both `None`, so the
   reader returns rows **unsorted** — ordering is done by the separate `SortExec`
   downstream, which is why Sort is its own codegen stage.

**The fetch completes before the join runs**, driven by the blocking Sort: the
Sort's drain loop pulls the *entire* shuffle-read iterator to exhaustion (that's
what triggers fetching all of partition `p`'s blocks) before it can sort. So by
the time `SortMergeJoin.processNext` runs, both inputs are fully fetched + sorted.
The join never touches the network.

**In `local[4]`:** one JVM = one BlockManager = **all blocks are "local"**, read
directly from `spark_tmp`. No Netty, no network — same code path, local branch
only. Mechanically identical to a cluster minus the wire.

---

## 8. Full reconciliation (every number ties out)

```
40,000,000  rows scanned per table
    ↓ Filter value>50 (A only, 49% uniform)
19,600,000  A rows into shuffle  (= shuffle records written)
    ↓ join against B's 40,000,000 on 20,000,000 shared keys (overlap = N/2)
20,000,000 × 0.49 = 9,800,000  matches  (= SortMergeJoin output = final count)
```

- ColumnarToRow: 40M rows / 9,784 batches ≈ 4,088/batch (~4096 vectorized batch)
- A sort: 19.6M rows, fits in 544MB, no spill
- B sort: 40M rows, spills 5GB (2× A's data, same memory budget)
- Join buffer: 0 spill (unique keys)

---

## 9. Production tuning checklist (falls out of the above)

1. **Shuffle fewer bytes** — filter / project / partial-aggregate *before* the
   Exchange. Biggest lever.
2. **Keep partitions balanced** — watch min-vs-max task time for skew. Skew is
   invisible in total bytes and needs salting / AQE skew-join, not more memory.
3. **Right-size partitions** — too many → tiny-blocks fetch overhead
   (M×R explosion); too few → spill. **Let AQE coalesce at runtime** rather than
   hardcoding `shuffle.partitions`. (Disabled here only for a static, readable
   plan.)
4. **Avoid the shuffle entirely** — if one side is small, **broadcast** it
   (map-only join, no Exchange, no sort, no spill, no fetch). This is why
   `autoBroadcastJoinThreshold` exists and is tried automatically before falling
   back to sort-merge join.

### Why the external / push-based shuffle service exists

- **External shuffle service:** decouples shuffle-file lifetime from executor
  lifetime. Executors die (spot, dynamic scale-down, OOM); if files lived *in* the
  executor, losing it forces recompute. External service holds files *outside*,
  so reducers fetch even after the producer is gone. Original motivation =
  resilience + dynamic allocation, not speed.
- **Push-based shuffle (Magnet, SPARK-30602):** pre-*merges* each reducer's
  scattered blocks on the service, so reducers fetch a few big sequential blocks
  instead of thousands of tiny random ones — directly fixes the "too many small
  files" problem. (The `*_merged_*` metrics; 0 in a local run since it's off.)
- **Disaggregated shuffle (Celeborn/Cosco):** pushes this further for cloud/K8s
  with ephemeral local disk.

---

## Key file references (this Spark checkout)

- `sql/core/.../execution/WholeStageCodegenExec.scala:423` — `BlockingOperatorWithCodegen`
- `sql/core/.../execution/UnsafeExternalRowSorter.java` — SQL-layer sort wrapper (`sort()` @170)
- `core/.../util/collection/unsafe/sort/UnsafeExternalSorter.java` — real engine:
  `insertRecord` @473 (copy to pages), `spill` @203 (sort run → disk → free)
- `core/.../shuffle/sort/SortShuffleWriter.scala` — `shouldBypassMergeSort` @110
- `core/.../shuffle/BlockStoreShuffleReader.scala` — `read()` @72 (reduce-side fetch)
```
