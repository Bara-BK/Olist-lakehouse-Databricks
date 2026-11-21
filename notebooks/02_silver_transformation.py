# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver transformation
# MAGIC Cleans the Bronze Delta tables and writes trusted tables to `workspace.silver`.
# MAGIC
# MAGIC **Silver rules**
# MAGIC - Cast strings to proper types (timestamps, integers, decimals). Uses `try_*` functions, so a bad value becomes NULL instead of crashing the job
# MAGIC - Trim and standardize text (lower-case statuses and cities, upper-case states)
# MAGIC - **Data quality rules** per table. Rows that fail go to `silver.rejected_rows` with the reason and the original raw row, they are never silently dropped
# MAGIC - Remove duplicates on the business key (latest Bronze load wins)
# MAGIC - Transactional tables are **upserted with `MERGE INTO`**; static tables are rebuilt each run
# MAGIC - Every run is logged in `silver.silver_log` (rows in, rejected, duplicates, inserted, updated)
# MAGIC - Cleaning functions, quality rules and deduplication live in the shared notebook `olist_transforms` (loaded with `%run`) so that the pytest tests check the same code
# MAGIC
# MAGIC **Usage**: set the `batch` widget to `static`, `batch_1`, `batch_2`, `batch_3`, or `all`, then Run all.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
REJECTS = f"{SILVER}.rejected_rows"
LOG_TABLE = f"{SILVER}.silver_log"

dbutils.widgets.text("batch", "all", "batch: all | static | batch_1 | batch_2 | batch_3")
requested = dbutils.widgets.get("batch").strip()

VALID = ["all", "static", "batch_1", "batch_2", "batch_3"]
assert requested in VALID, f"Invalid batch '{requested}'. Use one of {VALID}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {REJECTS} (
        table_name  STRING,
        batch_id    STRING,
        reason      STRING,
        row_json    STRING,
        rejected_at TIMESTAMP
    )
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
        batch_id           STRING,
        table_name         STRING,
        rows_in            BIGINT,
        rows_rejected      BIGINT,
        duplicates_removed BIGINT,
        rows_valid         BIGINT,
        rows_inserted      BIGINT,
        rows_updated       BIGINT,
        processed_at       TIMESTAMP
    )
""")
print(f"Requested: {requested}")

# COMMAND ----------
# MAGIC %run ./olist_transforms

# COMMAND ----------

def write_rejects(rejected, table, folder):
    # Delete first so re-running a batch does not duplicate its rejected rows
    spark.sql(f"DELETE FROM {REJECTS} WHERE table_name = '{table}' AND batch_id = '{folder}'")
    (
        rejected.select(
            F.lit(table).alias("table_name"),
            F.lit(folder).alias("batch_id"),
            F.col("_failed").alias("reason"),
            F.col("_raw_json").alias("row_json"),
            F.current_timestamp().alias("rejected_at"),
        )
        .write.mode("append")
        .saveAsTable(REJECTS)
    )


def merge_into(df, table, keys):
    """Upsert into the Silver table. Returns (rows_inserted, rows_updated)."""
    target = f"{SILVER}.{table}"
    if not spark.catalog.tableExists(target):
        df.limit(0).write.format("delta").saveAsTable(target)

    view = f"updates_{table}"
    df.createOrReplaceTempView(view)
    on_clause = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    result = spark.sql(f"""
        MERGE INTO {target} AS t
        USING {view} AS s
        ON {on_clause}
        WHEN MATCHED AND s.bronze_ingestion_ts >= t.bronze_ingestion_ts THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).collect()[0]
    return result["num_inserted_rows"], result["num_updated_rows"]


def log_run(folder, table, rows_in, rows_rejected, dups, rows_valid, inserted, updated):
    (
        spark.createDataFrame(
            [(folder, table, rows_in, rows_rejected, dups, rows_valid, inserted, updated)],
            "batch_id string, table_name string, rows_in long, rows_rejected long, "
            "duplicates_removed long, rows_valid long, rows_inserted long, rows_updated long",
        )
        .withColumn("processed_at", F.current_timestamp())
        .write.mode("append")
        .saveAsTable(LOG_TABLE)
    )


def process(table, folder):
    spec = SPECS[table]

    # 1. Read this batch from Bronze and keep the raw row as JSON for the quarantine table
    raw = spark.table(f"{BRONZE}.{table}").filter(F.col("batch_id") == folder)
    biz_cols = [c for c in raw.columns if c not in ("source_file", "batch_id", "ingestion_ts")]
    raw = raw.withColumn("_raw_json", F.to_json(F.struct(*biz_cols)))
    rows_in = raw.count()

    # 2. Cast and standardize, then apply the quality rules
    if table == "products":
        typed = spec["transform"](raw, spark.table(f"{BRONZE}.category_translation"))
    else:
        typed = spec["transform"](raw)
    valid, rejected = split_by_rules(typed, RULES[table])
    rows_rejected = rejected.count()
    write_rejects(rejected, table, folder)

    # 3. Deduplicate on the business key
    valid_count = valid.count()
    if spec["keys"]:
        valid = deduplicate(valid, spec["keys"])
    dups = valid_count - valid.count() if spec["keys"] else 0

    # 4. Drop helper columns and shape the final table
    final = valid.drop("_raw_json")
    if "post" in spec:
        final = spec["post"](final)
    final = final.withColumn("silver_updated_ts", F.current_timestamp())
    rows_valid = final.count()

    # 5. Write: MERGE for transactional tables, rebuild for static tables
    if spec["kind"] == "static":
        (
            final.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{SILVER}.{table}")
        )
        inserted, updated = None, None
    else:
        inserted, updated = merge_into(final, table, spec["keys"])

    log_run(folder, table, rows_in, rows_rejected, dups, rows_valid, inserted, updated)
    print(
        f"{folder:8} {table:15} in={rows_in:>9,}  rejected={rows_rejected:>4,}  "
        f"dups={dups:>3,}  valid={rows_valid:>9,}  inserted={inserted}  updated={updated}"
    )

# COMMAND ----------

folders = ["static", "batch_1", "batch_2", "batch_3"] if requested == "all" else [requested]

for folder in folders:
    tables = STATIC_TABLES if folder == "static" else BATCH_TABLES
    for table in tables:
        process(table, folder)

print("Done")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks
# MAGIC Run these after loading `static` and all 3 batches.

# COMMAND ----------

# Run log: one row per table per run
display(spark.table(LOG_TABLE).orderBy("processed_at", "table_name"))

# COMMAND ----------

# Reconciliation: every Bronze row must be accounted for (kept, rejected, or removed as a duplicate)
rows = []
for t in [t for t in SPECS if t != "geolocation"]:
    bronze_rows = spark.table(f"{BRONZE}.{t}").count()
    silver_rows = spark.table(f"{SILVER}.{t}").count()
    rejected_rows = spark.table(REJECTS).filter(F.col("table_name") == t).count()
    rows.append((t, bronze_rows, silver_rows, rejected_rows, bronze_rows - silver_rows - rejected_rows))

display(spark.createDataFrame(
    rows, "table string, bronze_rows long, silver_rows long, rejected long, duplicates_removed long"
))

g_bronze = spark.table(f"{BRONZE}.geolocation").count()
g_rejected = spark.table(REJECTS).filter(F.col("table_name") == "geolocation").count()
g_silver = spark.table(f"{SILVER}.geolocation").count()
print(f"geolocation: {g_bronze:,} bronze GPS points - {g_rejected:,} rejected -> {g_silver:,} zip code rows")

# COMMAND ----------

# What was quarantined, and why
display(
    spark.table(REJECTS)
    .groupBy("table_name", "reason")
    .count()
    .orderBy("table_name", "reason")
)

# COMMAND ----------

display(spark.table(REJECTS).limit(5))

# COMMAND ----------

# Key uniqueness: every count below must be 0
for t in BATCH_TABLES + ["customers", "products", "sellers"]:
    keys = SPECS[t]["keys"]
    dup_keys = spark.table(f"{SILVER}.{t}").groupBy(*keys).count().filter("count > 1").count()
    print(f"{t:15} duplicate keys: {dup_keys}")

# COMMAND ----------

# Referential integrity: orphan rows (child rows without a parent). Expected 0 for this dataset.
CHECKS = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("order_items", "seller_id", "sellers", "seller_id"),
    ("order_payments", "order_id", "orders", "order_id"),
    ("order_reviews", "order_id", "orders", "order_id"),
]
for child, child_key, parent, parent_key in CHECKS:
    c = spark.table(f"{SILVER}.{child}")
    p = spark.table(f"{SILVER}.{parent}")
    orphans = c.join(p, c[child_key] == p[parent_key], "left_anti").count()
    print(f"{child}.{child_key} -> {parent}.{parent_key}: {orphans} orphans")

# COMMAND ----------

# Typed schema and a sample
spark.table(f"{SILVER}.orders").printSchema()
display(spark.table(f"{SILVER}.orders").limit(5))

# COMMAND ----------

# Delta history of Silver orders: one MERGE per batch, with inserted/updated row counts
display(
    spark.sql(f"DESCRIBE HISTORY {SILVER}.orders")
    .select("version", "timestamp", "operation", "operationMetrics")
)

# COMMAND ----------

# OPTIONAL: full reset to re-run Day 3 from scratch. Uncomment, run once, then re-comment.
# for t in list(SPECS.keys()) + ["rejected_rows", "silver_log"]:
#     spark.sql(f"DROP TABLE IF EXISTS {SILVER}.{t}")
