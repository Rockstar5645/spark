// groupBy that forces a real shuffle, to compare Spark's built-in sort shuffle against Celeborn.
// Run both modes with celeborn_compare.py, or by hand from extern_repos/spark/:
//
//   ./bin/spark-shell -i ../claude_learnings/experiments/celeborn/celeborn_groupby.scala                                     # built-in
//   ./bin/spark-shell --properties-file "$SPARK_CONF_DIR"/celeborn.conf -i ../claude_learnings/experiments/celeborn/celeborn_groupby.scala  # Celeborn
//
// Data is synthetic: ROWS ids, each mapped to sha256(id) and to one of KEYS groups.
import org.apache.spark.sql.functions._

val ROWS = 40000000L   // ~4 GB of shuffle input at 64-char keys + payload
val KEYS = 2000000L    // distinct group keys -> every reducer gets real work
spark.conf.set("spark.sql.adaptive.enabled", "false")      // static plan, readable DAG
spark.conf.set("spark.sql.shuffle.partitions", "16")

val shuffleMgr = sc.getConf.get("spark.shuffle.manager", "sort (built-in)")
sc.setJobGroup("groupby", s"groupBy over $ROWS rows, shuffle manager = $shuffleMgr")

val df = spark.range(ROWS)
  .withColumn("group_key", sha2(pmod(col("id"), lit(KEYS)).cast("string"), 256))
  .withColumn("payload", sha2(col("id").cast("string"), 256))

val t0 = System.nanoTime
val r = df.groupBy("group_key")
  .agg(count(lit(1)).as("n"), max("payload").as("mx"))
  .agg(count(lit(1)).as("groups"), sum("n").as("rows"), sum(crc32(col("mx"))).as("h"))
  .collect()(0)
println(f"AKHIL_RESULT shuffleManager=$shuffleMgr groups=${r.getLong(0)} rows=${r.getLong(1)} " +
        f"hash=${r.getLong(2)} secs=${(System.nanoTime - t0) / 1e9}%.1f")
