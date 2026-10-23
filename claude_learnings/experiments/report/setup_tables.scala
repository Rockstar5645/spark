// ONE-TIME SETUP: write the two synthetic Delta tables used by report_walkthrough.scala.
// Re-run only if you want fresh data. It overwrites both tables, schema included.
//
// From extern_repos/spark/:
//   ./bin/spark-shell
//   scala> :paste ../claude_learnings/experiments/report/setup_tables.scala
//
// Use :paste, NOT :load. :load runs each complete line on its own, so `val units = spark.range(N)`
// runs alone and the following `.withColumn` lines get applied to the previous RESULT instead
// (seen 2026-10-15: UNRESOLVED_COLUMN serial_number). :paste compiles the file as one unit.
//
// This IS an action: it runs jobs on the executors and writes files.
import org.apache.spark.sql.functions._

val root = sys.env.getOrElse("REPORT_TABLES_ROOT",
  sys.error("set REPORT_TABLES_ROOT (source claude_learnings/experiments/env.sh)"))
val salesPath = s"$root/sales"
val telemetryPath = s"$root/telemetry"

// Products are (manufacturer, part_number). Part numbers deliberately COLLIDE across
// manufacturers (Acme X100 != Bolt X100), which is why the join needs both columns.
// Each sold unit has a serial_number; telemetry is readings from those serials, ~2% errors.
val manufacturers = Seq("Acme", "Bolt", "Cirrus", "Dyna")
val N_UNITS = 200000L      // units sold
val READINGS = 5           // telemetry readings per unit

val units = spark.range(N_UNITS)
  .withColumn("manufacturer", element_at(typedLit(manufacturers), (col("id") % 4 + 1).cast("int")))
  .withColumn("part_number", concat(lit("X"), (col("id") % 50 + 100).cast("string")))
  .withColumn("serial_number", concat(lit("SN"), col("id").cast("string")))

units
  .withColumn("sale_date", date_add(lit("2026-01-01"), (col("id") % 90).cast("int")))
  .withColumn("price", (col("id") % 40 + 10).cast("double"))
  .drop("id")
  .write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(salesPath)

units
  .crossJoin(spark.range(READINGS).withColumnRenamed("id", "reading"))
  .withColumn("temp_c", lit(60.0) + (hash(col("serial_number"), col("reading")) % 30).cast("double"))
  .withColumn("error_code", when(abs(hash(col("serial_number"), col("reading"))) % 250 === 0, lit("E42")))
  .drop("id", "reading")
  .write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(telemetryPath)

val s = spark.read.format("delta").load(salesPath)
val t = spark.read.format("delta").load(telemetryPath)
println(s"SETUP done: sales rows=${s.count()} cols=${s.columns.mkString(",")}")
println(s"            telemetry rows=${t.count()} errors=${t.where(col("error_code").isNotNull).count()} cols=${t.columns.mkString(",")}")
