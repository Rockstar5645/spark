// Delta commit-generator — produces a variety of _delta_log JSON commits so you
// can read real add / remove / metaData / protocol / commitInfo actions.
//
// Run with:
//   SPARK_PREPEND_CLASSES=1 ./bin/spark-shell --master 'local[4]' --driver-memory 32g \
//     --jars '/home/asteralabs/second_home/fourth-repos/delta/spark/target/scala-2.12/delta-spark_2.12-3.3.2.jar,/home/asteralabs/second_home/fourth-repos/delta/storage/target/delta-storage-3.4.0-SNAPSHOT.jar' \
//     --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
//     --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
//     -i claude_learnings/test.scala
//
// Then inspect:  ls saved_delta_tables/my_test_table/_delta_log/
//                cat saved_delta_tables/my_test_table/_delta_log/00000000000000000000.json | jq

import io.delta.tables._
import org.apache.spark.sql.functions._

val tablePath = "/home/asteralabs/second_home/fourth-repos/saved_delta_tables/my_test_table"

// Small so each commit writes just a few files — easy to read the JSON by hand.
// A couple of partitions keeps file counts low and predictable.
spark.conf.set("spark.sql.shuffle.partitions", "2")

// ---------------------------------------------------------------------------
// Commit 00000...0.json  — CREATE (initial write)
//   Expect: `protocol`, `metaData`, one `commitInfo`, and a few `add` actions.
//   This is the only commit that carries protocol + metaData.
// ---------------------------------------------------------------------------
spark.range(0, 20)
  .withColumn("value", (col("id") * 10))
  .withColumn("category", (col("id") % 2).cast("string"))  // partition column
  .write.format("delta")
  .partitionBy("category")        // gives you non-empty partitionValues in `add`
  .mode("overwrite")
  .save(tablePath)

// ---------------------------------------------------------------------------
// Commit 00000...1.json  — APPEND
//   Expect: only `add` actions (+ commitInfo). No removes — pure insert.
// ---------------------------------------------------------------------------
spark.range(20, 30)
  .withColumn("value", (col("id") * 10))
  .withColumn("category", (col("id") % 2).cast("string"))
  .write.format("delta")
  .mode("append")
  .save(tablePath)

val dt = DeltaTable.forPath(spark, tablePath)

// ---------------------------------------------------------------------------
// Commit 00000...2.json  — UPDATE
//   Expect: `remove` (the file(s) holding matched rows) + `add` (rewritten
//   file with new values). Delta never edits in place — it swaps whole files.
// ---------------------------------------------------------------------------
dt.update(
  condition = col("id") === 5,
  set = Map("value" -> lit(999)))

// ---------------------------------------------------------------------------
// Commit 00000...3.json  — DELETE
//   Expect: `remove` for files containing the deleted rows, and `add` for the
//   rewritten remainder (unless a whole file's rows all qualify -> pure remove).
// ---------------------------------------------------------------------------
dt.delete(col("id") === 25)

// ---------------------------------------------------------------------------
// Commit 00000...4.json  — MERGE (upsert)
//   Expect: remove + add. Matched keys update existing rows; unmatched keys
//   insert. This is the findTouchedFiles -> writeAllChanges path you traced.
// ---------------------------------------------------------------------------
val updates = spark.range(8, 15)
  .withColumn("value", lit(-1))
  .withColumn("category", (col("id") % 2).cast("string"))
  .select("id", "value", "category")

dt.as("t")
  .merge(updates.as("s"), "t.id = s.id")
  .whenMatched.update(Map("value" -> col("s.value")))
  .whenNotMatched.insertAll()
  .execute()

// ---------------------------------------------------------------------------
// Commit 00000...5.json  — OPTIMIZE (compaction)
//   Expect: several `remove` (the small files) + fewer `add` (compacted files).
//   No data changes — just file layout. Great contrast with the DML commits.
// ---------------------------------------------------------------------------
dt.optimize().executeCompaction()

// ===========================================================================
// SECOND TABLE — deletion vectors ON, so a DELETE marks rows instead of
// rewriting files. Diff this table's DELETE commit against my_test_table's
// commit ...3.json (plain rewrite) to see the two shapes side by side.
// ===========================================================================
val dvPath = "/home/asteralabs/second_home/fourth-repos/saved_delta_tables/my_test_table_dv"

// Commit ...0.json — CREATE (no partitioning, keeps it simple)
spark.range(0, 20)
  .withColumn("value", (col("id") * 10))
  .write.format("delta")
  .mode("overwrite")
  .save(dvPath)

val dtDv = DeltaTable.forPath(spark, dvPath)

// IMPORTANT for this custom Spark 3.5 build:
// Delta can locate deleted row positions via either (a) Spark's Parquet
// `_metadata.row_index` column, or (b) Delta's own `__delta_internal_row_index`.
// The default (a) requires the Parquet reader's row_index metadata field, which
// our instrumented Spark build doesn't expose cleanly -> "No such struct field
// row_index". Forcing (b) sidesteps it entirely; DVs still work the same.
spark.conf.set("spark.databricks.delta.deletionVectors.useMetadataRowIndex", "false")

// Commit ...1.json — enable the deletion-vectors table feature.
//   Expect: a `metaData` action whose configuration now includes
//   delta.enableDeletionVectors=true, AND a `protocol` bump adding
//   "deletionVectors" to readerFeatures/writerFeatures (min reader 3 /
//   writer 7). That protocol upgrade is what actually turns DVs on.
spark.sql(
  s"ALTER TABLE delta.`$dvPath` SET TBLPROPERTIES " +
  "('delta.enableDeletionVectors' = 'true')")

// Commit ...2.json — DELETE with DVs active.
//   Expect: NO full rewrite. Instead an `add` (or remove+add) carrying a
//   `deletionVector` struct: storageType, pathOrInlineDv, offset,
//   sizeInBytes, cardinality. The parquet on disk is untouched; a separate
//   `deletion_vector_*.bin` file appears alongside it. Contrast with
//   my_test_table's commit ...3.json which removed+re-added the data file.
dtDv.delete(col("id") === 5)

// Commit ...3.json — a second DELETE, different row. Watch whether it writes
//   a fresh DV or updates the existing one (cardinality grows).
dtDv.delete(col("id") === 12)

// ---------------------------------------------------------------------------
println("\n=== done. ===")
println("plain table:  " + tablePath + "/_delta_log/")
println("DV table:     " + dvPath + "/_delta_log/")
println("\n-- plain table history --")
dt.history().select("version", "operation", "operationParameters").show(false)
spark.read.format("delta").load(tablePath).orderBy("id").show(50)
println("\n-- DV table history --")
dtDv.history().select("version", "operation", "operationParameters").show(false)
spark.read.format("delta").load(dvPath).orderBy("id").show(50)
