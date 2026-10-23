// Print the same query's tree before and after analysis.
// Run from extern_repos/spark/:  ./bin/spark-shell --master 'local[1]' -i ../claude_learnings/experiments/plans/show_logical_vs_analyzed.scala
//
// The DataFrame API analyzes after EVERY call (withColumn, groupBy...), so df.queryExecution.logical
// is already resolved -- logical and analyzed print identically. Verified 2026-10-13.
// SQL text is parsed first and resolved second, so it shows the unresolved tree.
spark.range(10).createOrReplaceTempView("nums")

val q = """
  SELECT sha2(cast(id % 3 AS string), 256) AS group_key, count(1) AS n
  FROM nums
  GROUP BY group_key
"""
val parsed = spark.sessionState.sqlParser.parsePlan(q)   // text -> tree, nothing looked up yet
val df = spark.sql(q)

println("PLANDUMP ===== 1. parsed (unresolved) =====")
println(parsed.treeString)
println("PLANDUMP ===== 2. analyzed =====")
println(df.queryExecution.analyzed.treeString)
println("PLANDUMP ===== types after analysis =====")
df.queryExecution.analyzed.output.foreach(a => println(s"  ${a.name}#${a.exprId.id} : ${a.dataType}"))
println("PLANDUMP ===== end =====")
System.exit(0)
