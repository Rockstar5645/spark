# Building Spark + Delta and Running spark-shell (with instrumentation)

This documents the working setup for building an instrumented Spark 3.5 alongside
Delta 3.3.2, and launching `spark-shell` with Delta wired in — so we can trace
RDD/Delta execution with custom `AKHIL` debug logging.

## Environment

- **Spark**: `branch-3.5` (Scala 2.12, builds `spark-core_2.12-3.5.x-SNAPSHOT`),
  with debug instrumentation cherry-picked onto it.
- **Delta**: tag `v3.3.2` (matches Spark 3.5; the `master`/`supported-delta` builds
  target Spark 4.x which isn't published and won't link against this Spark).
- **Java**: OpenJDK 17 at `/usr/lib/jvm/java-17-openjdk-amd64`.

> Note: Spark 3.5 is Scala **2.12**. Delta must be built for 2.12 to match. The
> unified `spark-unified` Delta module does not compile on 2.12 (uses
> `scala.jdk.javaapi.CollectionConverters`, a 2.13 API) — building the `spark`
> subproject at tag `v3.3.2` produces the correct `delta-spark_2.12-3.3.2.jar`.

## Build Spark

```bash
cd /home/asteralabs/second_home/fourth-repos/spark
git checkout branch-3.5
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
./build/sbt core/clean core/package assembly/package
```

- `core/clean` is important: it purges stale Spark 4.2 (`master`) classes that
  otherwise cause `UnsupportedOperationException` / `sql.classic.DataFrameWriter`
  errors at runtime.
- `assembly/package` builds the full jar set that `spark-shell` loads.

## Build Delta

```bash
cd /home/asteralabs/second_home/fourth-repos/delta
git checkout v3.3.2
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
build/sbt spark/package
```

Produces:
- `spark/target/scala-2.12/delta-spark_2.12-3.3.2.jar`
- `storage/target/delta-storage-3.4.0-SNAPSHOT.jar`

If scalastyle blocks the build after adding instrumentation, disable it by
commenting out the `compileScalastyle` / `Compile / compile` wiring in
`project/Checkstyle.scala`.

## Run spark-shell with Delta

```bash
cd /home/asteralabs/second_home/fourth-repos/spark
SPARK_PREPEND_CLASSES=1 ./bin/spark-shell --master 'local[4]' \
  --driver-memory 32g \
  --jars '/home/asteralabs/second_home/fourth-repos/delta/spark/target/scala-2.12/delta-spark_2.12-3.3.2.jar,/home/asteralabs/second_home/fourth-repos/delta/storage/target/delta-storage-3.4.0-SNAPSHOT.jar' \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```

- `SPARK_PREPEND_CLASSES=1` puts freshly compiled `classes/` ahead of the jars,
  so instrumentation changes show up without re-jarring.
- zsh quirks: quote `'local[4]'` (glob) and the comma-separated `--jars` value.

## Example: trace a Delta merge

```scala
import io.delta.tables._
import org.apache.spark.sql.functions._

val tablePath = "/home/asteralabs/second_home/fourth-repos/delta_table_output"

// 1M-row Delta table
spark.range(1000000)
  .withColumn("key", col("id"))
  .withColumn("value", (rand() * 1000).cast("int"))
  .withColumn("category", (col("id") % 10).cast("string"))
  .write.format("delta").mode("overwrite").save(tablePath)

// Merge 10k random updates
val updates = spark.range(10000)
  .withColumn("key", (rand() * 1000000).cast("long"))
  .withColumn("new_value", (rand() * 9999).cast("int"))
  .select("key", "new_value")

DeltaTable.forPath(spark, tablePath).as("target")
  .merge(updates.as("source"), "target.key = source.key")
  .whenMatched.update(Map("value" -> col("source.new_value")))
  .whenNotMatched.insertExpr(Map(
    "key" -> "source.key", "value" -> "source.new_value", "category" -> "'new'"))
  .execute()
```

The `AKHIL` traces fire from both Spark core (parallelize/map/DAGScheduler/shuffle)
and the Delta merge path (`findTouchedFiles` / `writeAllChanges` in
`ClassicMergeExecutor.scala`).
