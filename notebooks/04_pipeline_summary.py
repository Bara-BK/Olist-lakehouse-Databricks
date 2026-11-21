# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04 - Pipeline summary and quality gate
# MAGIC Last task of the Job. It does two things:
# MAGIC 1. **Logs row counts per layer** (bronze / silver / gold) into `workspace.ops.pipeline_run_log`
# MAGIC 2. **Runs quality gate checks** and stores the results in `workspace.ops.pipeline_check_log`.
# MAGIC    If any check fails, the notebook raises an error so the Job run is marked as **failed**.
# MAGIC
# MAGIC Job parameters used: `run_id` (set to `{{job.run_id}}` in the Job; defaults to `manual` when run by hand).

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"
OPS = f"{CATALOG}.ops"
RUN_LOG = f"{OPS}.pipeline_run_log"
CHECK_LOG = f"{OPS}.pipeline_check_log"

MAX_REJECT_RATE_PCT = 1.0

dbutils.widgets.text("run_id", "manual", "run_id (set by the Job)")
run_id = dbutils.widgets.get("run_id").strip() or "manual"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {RUN_LOG} (
        run_id     STRING,
        run_ts     TIMESTAMP,
        layer      STRING,
        table_name STRING,
        row_count  BIGINT
    )
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CHECK_LOG} (
        run_id     STRING,
        run_ts     TIMESTAMP,
        check_name STRING,
        passed     BOOLEAN,
        detail     STRING
    )
""")
print(f"run_id = {run_id}")

# COMMAND ----------

# 1. Row counts per layer
rows = []
for layer, schema in [("bronze", BRONZE), ("silver", SILVER), ("gold", GOLD)]:
    for t in spark.sql(f"SHOW TABLES IN {schema}").collect():
        name = t["tableName"]
        n = spark.table(f"{schema}.{name}").count()
        rows.append((run_id, layer, name, n))

(
    spark.createDataFrame(rows, "run_id string, layer string, table_name string, row_count long")
    .withColumn("run_ts", F.current_timestamp())
    .select("run_id", "run_ts", "layer", "table_name", "row_count")
    .write.mode("append")
    .saveAsTable(RUN_LOG)
)

this_run = spark.table(RUN_LOG).filter(F.col("run_id") == run_id)
display(this_run.orderBy("layer", "table_name"))

# COMMAND ----------

# 2. Quality gate
results = []   # (check_name, passed, detail)

def check(name, passed, detail):
    results.append((name, bool(passed), detail))
    print(f"{'PASS' if passed else 'FAIL'}  {name:34} {detail}")

s_orders = spark.table(f"{SILVER}.orders")
s_items = spark.table(f"{SILVER}.order_items")
g_orders = spark.table(f"{GOLD}.fact_orders")
g_items = spark.table(f"{GOLD}.fact_order_items")

# Gold row counts match Silver
a, b = s_orders.count(), g_orders.count()
check("gold_orders_match_silver", a == b, f"silver={a:,} gold={b:,}")

a, b = s_items.count(), g_items.count()
check("gold_items_match_silver", a == b, f"silver={a:,} gold={b:,}")

# Revenue reconciles to the cent
a = s_items.agg(F.sum("price")).first()[0]
b = g_items.agg(F.sum("price")).first()[0]
check("revenue_reconciles", a == b, f"silver={a} gold={b}")

# Keys are unique
d = g_orders.groupBy("order_id").count().filter("count > 1").count()
check("fact_orders_unique_key", d == 0, f"duplicate keys={d}")

d = g_items.groupBy("order_id", "order_item_id").count().filter("count > 1").count()
check("fact_order_items_unique_key", d == 0, f"duplicate keys={d}")

# No order item without its order
o = g_items.join(g_orders.select("order_id"), "order_id", "left_anti").count()
check("no_orphan_order_items", o == 0, f"orphans={o}")

# Quarantine rate stays small
bronze_tables = ["orders", "order_items", "order_payments", "order_reviews",
                 "customers", "products", "sellers", "geolocation"]
bronze_total = sum(spark.table(f"{BRONZE}.{t}").count() for t in bronze_tables)
rejected_total = spark.table(f"{SILVER}.rejected_rows").count()
rate = 100.0 * rejected_total / bronze_total if bronze_total else 0.0
check("rejected_rate_below_threshold", rate < MAX_REJECT_RATE_PCT,
      f"rejected={rejected_total:,} of {bronze_total:,} = {rate:.4f}% (limit {MAX_REJECT_RATE_PCT}%)")

# COMMAND ----------

(
    spark.createDataFrame(
        [(run_id, n, p, d) for n, p, d in results],
        "run_id string, check_name string, passed boolean, detail string",
    )
    .withColumn("run_ts", F.current_timestamp())
    .select("run_id", "run_ts", "check_name", "passed", "detail")
    .write.mode("append")
    .saveAsTable(CHECK_LOG)
)

failed = [n for n, p, _ in results if not p]
if failed:
    raise Exception(f"Quality gate FAILED: {failed}")
print(f"Quality gate passed ({len(results)} checks)")