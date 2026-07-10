// Sort-merge join + shuffle + spill, end to end, on two large Delta tables.
//
// Goal: watch the FULL shuffle machinery fire — map side writes sorted
// partition files (SortShuffleWriter, your [10a]/[10b] AKHIL traces), reduce
// side fetches them (BlockStoreShuffleReader, your [11] trace), and the
// sort-merge join spills to disk when a shuffle partition doesn't fit in
// execution memory.
//
// Run with (NOTE: use a SMALL driver memory here — counterintuitive, but we
// WANT spill, and 32g would just swallow the whole join in RAM):
//
//   SPARK_PREPEND_CLASSES=1 ./bin/spark-shell --master 'local[4]' \
//     --driver-memory 4g \
//     --jars '/home/asteralabs/second_home/fourth-repos/delta/spark/target/scala-2.12/delta-spark_2.12-3.3.2.jar,/home/asteralabs/second_home/fourth-repos/delta/storage/target/delta-storage-3.4.0-SNAPSHOT.jar' \
//     --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
//     --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
//     -i claude_learnings/join_understanding.scala
//
// local[4] = 4 task slots in ONE JVM. That's your "4 workers". Shuffle files
// are written to spark.local.dir on this machine and fetched back — exact same
// mechanics as a real cluster, just no network hop.

import org.apache.spark.sql.functions._

val basePath = "/home/asteralabs/second_home/fourth-repos/saved_delta_tables"
val tableA = s"$basePath/join_table_a"
val tableB = s"$basePath/join_table_b"

// ---------------------------------------------------------------------------
// SIZING — tune N and payloadRepeat to hit your target table size.
//   row ≈ 64 (sha256 hex) + 4 (value) + 4*payloadRepeat (payload) bytes.
//   With payloadRepeat=64  -> ~320 bytes/row.
//   N = 40,000,000 rows    -> ~12 GB/table (sha256 is high-entropy, barely
//                             compresses in parquet — realistic vs your ATE data).
//   Start smaller (N = 5,000,000, ~1.5 GB) to see spill fast, then scale up.
// ---------------------------------------------------------------------------
val N: Long = 40000000L        // rows per table
val payloadRepeat = 64         // bump to inflate row width
val overlap: Long = N / 2      // how many keys the two tables share -> match count

// ---------------------------------------------------------------------------
// CONFIG — force the plan we want to study.
// ---------------------------------------------------------------------------
// (1) Disable broadcast: with sha256 string keys and 12GB tables neither side
//     would broadcast anyway, but -1 GUARANTEES a shuffle (sort-merge) join
//     instead of Spark quietly broadcasting the smaller side.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

// (2) Disable AQE so the plan is STATIC and readable: the shuffle partition
//     count stays exactly what we set, no runtime coalescing / join-strategy
//     switching to confuse the DAG you're reading.
spark.conf.set("spark.sql.adaptive.enabled", "false")

// (3) FEW, FAT shuffle partitions. This is the spill lever: fewer partitions =
//     more bytes per partition = each reduce task's sort can't fit in execution
//     memory = spill to disk. With 4g driver and 8 partitions over ~12GB, the
//     sort-merge join's external sorter WILL spill. Crank to 4 for heavier spill,
//     raise to 200 to (mostly) avoid it and see the contrast.
spark.conf.set("spark.sql.shuffle.partitions", "8")

// ---------------------------------------------------------------------------
// BUILD THE TWO TABLES
//   artifact_id: sha256 hex string, unique within a table (mirrors your real
//                ATE keys — uniformly distributed, skipping-hostile, on purpose).
//   value:       0..99, deterministic from id, used by the post-join filter.
//   payload:     dead weight to reach the target size.
// Tables A and B overlap on the middle `overlap` keys so the join actually
// produces matches instead of an empty result.
// ---------------------------------------------------------------------------
def makeTable(startId: Long, count: Long) = {
  spark.range(startId, startId + count)
    .withColumn("artifact_id", sha2(col("id").cast("string"), lit(256)))
    .withColumn("value", pmod(col("id"), lit(100)).cast("int"))
    .withColumn("payload", repeat(substring(col("artifact_id"), 1, 4), payloadRepeat))
    .select("artifact_id", "value", "payload")
}

println(s"\n=== writing table A: $N rows -> $tableA ===")
makeTable(0L, N)
  .write.format("delta").mode("overwrite").save(tableA)

println(s"\n=== writing table B: $N rows (overlap=$overlap) -> $tableB ===")
// B starts at N-overlap so ids [N-overlap, N) are shared with A.
makeTable(N - overlap, N)
  .write.format("delta").mode("overwrite").save(tableB)

// ---------------------------------------------------------------------------
// THE JOIN + FILTER
// ---------------------------------------------------------------------------
val a = spark.read.format("delta").load(tableA).alias("a")
val b = spark.read.format("delta").load(tableB).alias("b")

// Inner join on the string key, then filter. Rename B's columns so the select
// isn't ambiguous.
val joined = a.join(b, a("artifact_id") === b("artifact_id"))
  .where(col("a.value") > 50)
  .select(col("a.artifact_id"), col("a.value").alias("value_a"), col("b.value").alias("value_b"))

// Look at the physical plan BEFORE running it. You want to see, bottom-up:
//   Scan parquet (both sides)
//     -> Exchange hashpartitioning(artifact_id, 8)   <- THE SHUFFLE (map-side write)
//       -> Sort [artifact_id ASC]                     <- sort each partition
//         -> SortMergeJoin [artifact_id]              <- merge the two sorted streams
// Each Exchange is where SortShuffleWriter runs on the map side and
// BlockStoreShuffleReader fetches on the reduce side.
println("\n=== PHYSICAL PLAN (read bottom-up) ===")
joined.explain(true)

// An ACTION to actually execute it. count() forces the whole shuffle+join+spill.
println("\n=== executing join (watch AKHIL [10a]/[10b] write, [11] read traces) ===")
val matched = joined.count()
println(s"\n=== matched rows after filter: $matched ===")

// ---------------------------------------------------------------------------
// WHAT TO LOOK AT while / after this runs:
//
//   Spark UI  http://localhost:4040
//     - SQL tab  -> click the query -> the DAG shows two Exchange nodes.
//         "shuffle write size" on the map stage = bytes SortShuffleWriter spilled
//         to the shuffle files; "shuffle read size" on the join stage = what
//         BlockStoreShuffleReader pulled back.
//     - Stages tab -> the join stage -> "Spill (Memory)" and "Spill (Disk)"
//         columns. Non-zero Disk spill = the external sorter overflowed
//         execution memory and wrote sorted runs to spark.local.dir. THAT is
//         the spill you wanted to see.
//
//   stderr AKHIL traces:
//     [10a]/[10b] SortShuffleWriter  -> map side writing sorted partition files
//     [11]        BlockStoreShuffleReader -> reduce side fetching those blocks
//
// Experiments:
//   - rerun with shuffle.partitions=4   -> heavier spill (fatter partitions)
//   - rerun with shuffle.partitions=200 -> little/no spill (thin partitions)
//   - rerun with --driver-memory 16g    -> spill shrinks/vanishes (more exec mem)
//   - flip autoBroadcastJoinThreshold back to default AFTER shrinking N to
//     ~100k rows -> plan becomes BroadcastHashJoin, NO shuffle at all. Good
//     contrast: broadcast avoids the shuffle entirely by shipping the small side.
// ---------------------------------------------------------------------------
println("\n=== done. Spark UI: http://localhost:4040 (SQL + Stages tabs) ===")
