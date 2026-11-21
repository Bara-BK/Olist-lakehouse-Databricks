# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05 - Delta Lake features demo
# MAGIC Shows the Delta features that matter for this project, using the real tables where it is safe
# MAGIC and a **copy** (`workspace.default.demo_orders`) where the demo changes data.
# MAGIC
# MAGIC 1. Transaction log and **time travel** on `silver.orders`
# MAGIC 2. **RESTORE** after an accidental delete (on the copy)
# MAGIC 3. **Schema enforcement** (a write with the wrong columns is rejected)
# MAGIC 4. **CHECK constraints** (a row with an invalid value is rejected)
# MAGIC 5. **OPTIMIZE** (small-file compaction) on `silver.orders`
# MAGIC
# MAGIC Run the cells in order. Section 6 removes the demo copy.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SILVER = f"{CATALOG}.silver"
DEMO = f"{CATALOG}.default.demo_orders"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Transaction log and time travel

# COMMAND ----------

history = spark.sql(f"DESCRIBE HISTORY {SILVER}.orders")
display(history.select("version", "timestamp", "operation", "operationMetrics").orderBy("version"))

# COMMAND ----------

# Row count of silver.orders at every version: the table "as it was" after each batch
versions = sorted(r["version"] for r in history.select("version").collect())
for v in versions:
    n = spark.sql(f"SELECT COUNT(*) AS n FROM {SILVER}.orders VERSION AS OF {v}").first()["n"]
    print(f"version {v}: {n:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Accidental delete, then RESTORE (on a copy)

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE {DEMO} AS SELECT * FROM {SILVER}.orders")
print("copy created:", spark.table(DEMO).count(), "rows")

# Remember the version right after creation: this is the version we will restore to
good_version = spark.sql(f"DESCRIBE HISTORY {DEMO}").agg(F.max("version")).first()[0]
print("good version:", good_version)

# COMMAND ----------

# Oops: delete every delivered order
spark.sql(f"DELETE FROM {DEMO} WHERE order_status = 'delivered'")
print("after DELETE :", spark.table(DEMO).count(), "rows")

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {DEMO}").select("version", "operation", "operationMetrics"))

# COMMAND ----------

# Go back to the state right after the copy was created
spark.sql(f"RESTORE TABLE {DEMO} TO VERSION AS OF {good_version}")
print("after RESTORE:", spark.table(DEMO).count(), "rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schema enforcement
# MAGIC Delta rejects a write whose columns do not match the table.

# COMMAND ----------

wrong = spark.createDataFrame(
    [("o-1", "c-1", "surprise")],
    "order_id string, customer_id string, surprise_column string",
)
try:
    wrong.write.format("delta").mode("append").saveAsTable(DEMO)
    print("UNEXPECTED: the write was accepted")
except Exception as e:
    print("Write rejected as expected:")
    print(str(e)[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. CHECK constraint
# MAGIC The table itself refuses invalid order statuses, whatever job writes to it.

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {DEMO}
    ADD CONSTRAINT valid_order_status
    CHECK (order_status IN ('delivered','shipped','canceled','unavailable','invoiced','processing','created','approved'))
""")

bad_row = spark.table(f"{SILVER}.orders").limit(1).withColumn("order_status", F.lit("banana"))
try:
    bad_row.write.format("delta").mode("append").saveAsTable(DEMO)
    print("UNEXPECTED: the row was accepted")
except Exception as e:
    print("Row rejected as expected:")
    print(str(e)[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. OPTIMIZE (small-file compaction)
# MAGIC Every MERGE adds new small files. OPTIMIZE rewrites them into fewer, larger files.

# COMMAND ----------

before = spark.sql(f"DESCRIBE DETAIL {SILVER}.orders").select("numFiles", "sizeInBytes").first()
print(f"before OPTIMIZE: {before['numFiles']} files, {before['sizeInBytes']:,} bytes")

# COMMAND ----------

display(spark.sql(f"OPTIMIZE {SILVER}.orders"))

# COMMAND ----------

after = spark.sql(f"DESCRIBE DETAIL {SILVER}.orders").select("numFiles", "sizeInBytes").first()
print(f"after OPTIMIZE : {after['numFiles']} files, {after['sizeInBytes']:,} bytes")

# The data is unchanged, only the file layout
print("rows:", spark.table(f"{SILVER}.orders").count())

# COMMAND ----------

# MAGIC %md
# MAGIC **Notes for the README**
# MAGIC - This dataset is small, so OPTIMIZE has little effect on speed. The point is the technique: MERGE-heavy tables
# MAGIC   accumulate small files, and compaction fixes it.
# MAGIC - Partitioning is deliberately **not** used: partitioning small tables creates many tiny files. For larger tables,
# MAGIC   liquid clustering (`CLUSTER BY`) is the modern alternative to partitioning and Z-ORDER.
# MAGIC - `VACUUM` deletes old files and limits how far back time travel can go. It was not run here, the default
# MAGIC   retention (7 days) is kept.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cleanup

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {DEMO}")
print("demo table removed")