// Product failure-rate report: read, build, plan, run. Safe to re-run any number of times.
// Needs the tables from setup_tables.scala (run that once first).
//
// QUESTION: for each product (manufacturer, part_number), what fraction of the units we sold
// later threw an error in the field?
//
// From extern_repos/spark/ (debugger: see README.md):
//   scala> :paste ../claude_learnings/experiments/report/report_walkthrough.scala      // whole thing
// or paste one STEP at a time with :paste ... Ctrl-D, to step through slowly.
//
// Everything here runs in the DRIVER (this spark-shell JVM) until STEP 6.
// Breakpoints go in the SPARK source, not in this file. This file is compiled by the REPL into
// synthetic classes, so the debugger can't map lines here. E.g. STEP 2a -> Dataset.scala:1812.
import org.apache.spark.sql.functions._

val root = sys.env.getOrElse("REPORT_TABLES_ROOT",
  sys.error("set REPORT_TABLES_ROOT (source claude_learnings/experiments/env.sh)"))

// STEP 1 -- open the tables. NOT an action: reads only the Delta log + schema, no rows.
val sales = spark.read.format("delta").load(s"$root/sales")
val telemetry = spark.read.format("delta").load(s"$root/telemetry")
println("STEP 1: sales.queryExecution.logical =\n" + sales.queryExecution.logical)

// STEP 2 -- unitsSold. groupBy returns a holder (no plan node); agg creates the Aggregate node.
val grouped = sales.groupBy("manufacturer", "part_number")
println("STEP 2a: groupBy returned a " + grouped.getClass.getSimpleName + " (not a Dataset)")

val unitsSold = grouped.agg(count(lit(1)).as("units"), sum("price").as("revenue"))
println("STEP 2b: unitsSold.queryExecution.logical =\n" + unitsSold.queryExecution.logical)

// STEP 3 -- failures. Same shape as unitsSold, plus a Filter for rows that errored.
val failures = telemetry.where(col("error_code").isNotNull)
  .groupBy("manufacturer", "part_number")
  .agg(countDistinct("serial_number").as("failed_units"))
println("STEP 3: failures.queryExecution.logical =\n" + failures.queryExecution.logical)

// STEP 4 -- report. A Join with two children, then a Project, then a Sort.
val report = unitsSold.join(failures, Seq("manufacturer", "part_number"))
  .withColumn("failure_rate", col("failed_units") / col("units"))
  .orderBy(col("failure_rate").desc)
println("STEP 4: report.queryExecution.logical =\n" + report.queryExecution.logical)

// STEP 5 -- the plans. Reading these forces optimize + plan, still no data.
println("STEP 5a: optimizedPlan =\n" + report.queryExecution.optimizedPlan)
println("STEP 5b: executedPlan (physical, read bottom-up) =\n" + report.queryExecution.executedPlan)

// STEP 6 -- ACTION. Jobs run on the executors now.
report.show(10, false)
