/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.examples.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// scalastyle:off println

// akhil-learnings: product failure-rate report, compiled into Spark so a breakpoint on any line
// here binds, and stepping goes straight into Spark. Runs in local mode, so the driver,
// scheduler, and task threads are all in ONE JVM and one debugger sees everything.
// Tables come from experiments/report/setup_tables.scala (run that once first).
//
// Run: experiments/report/debug_run.sh  (waits for the VS Code debugger to attach)
object ReportWalkthrough {
  // Set by experiments/report/debug_run.sh. Kept out of the source so the public fork carries
  // no local paths.
  val root = sys.env.getOrElse("REPORT_TABLES_ROOT",
    sys.error("set REPORT_TABLES_ROOT to the dir holding the sales/ and telemetry/ tables"))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("ReportWalkthrough").getOrCreate()

    // STEP 1 -- open the tables. NOT an action: reads only the Delta log + schema, no rows.
    val sales = spark.read.format("delta").load(s"$root/sales")
    val telemetry = spark.read.format("delta").load(s"$root/telemetry")
    println("STEP 1: sales.queryExecution.logical =\n" + sales.queryExecution.logical)

    // STEP 2 -- unitsSold. groupBy returns a holder (no plan node); agg creates the Aggregate.
    val grouped = sales.groupBy("manufacturer", "part_number")
    println("STEP 2a: groupBy returned a " + grouped.getClass.getSimpleName)

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
    println("STEP 5b: executedPlan =\n" + report.queryExecution.executedPlan)

    // STEP 6 -- ACTION. Tasks run as threads in this JVM (local mode).
    report.show(10, false)

    spark.stop()
  }
}
// scalastyle:on println
