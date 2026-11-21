# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 - Bronze ingestion
# MAGIC Loads the raw Olist CSVs from the landing Volume into Delta tables in `workspace.bronze`.
# MAGIC
# MAGIC **Bronze rules**
# MAGIC - Raw fidelity: all source columns kept as `string` (typing and cleaning happen in Silver)
# MAGIC - Explicit schemas, so a changed file layout is caught instead of silently inferred
# MAGIC - Metadata added to every row: `source_file`, `batch_id`, `ingestion_ts`
# MAGIC - Transactional tables are **appended** per batch; static tables are loaded once
# MAGIC - Idempotent: a log table prevents loading the same batch twice
# MAGIC
# MAGIC **Usage**: set the `batch` widget to `static`, `batch_1`, `batch_2`, `batch_3`, or `all`, then Run all.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

CATALOG = "workspace"
BASE = f"/Volumes/{CATALOG}/landing/raw"
BRONZE = f"{CATALOG}.bronze"
LOG_TABLE = f"{BRONZE}.ingestion_log"

dbutils.widgets.text("batch", "all", "batch: all | static | batch_1 | batch_2 | batch_3")
requested = dbutils.widgets.get("batch").strip()

VALID = ["all", "static", "batch_1", "batch_2", "batch_3"]
assert requested in VALID, f"Invalid batch '{requested}'. Use one of {VALID}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
        batch_id    STRING,
        table_name  STRING,
        rows_loaded BIGINT,
        ingested_at TIMESTAMP
    )
""")
print(f"Requested: {requested}")

# COMMAND ----------

# Explicit schemas (all strings in Bronze). Column names match the source files exactly,
# including the original misspellings such as product_name_lenght.

def all_strings(columns):
    return StructType([StructField(c, StringType(), True) for c in columns])

SCHEMAS = {
    "orders": all_strings([
        "order_id", "customer_id", "order_status", "order_purchase_timestamp",
        "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date",
    ]),
    "order_items": all_strings([
        "order_id", "order_item_id", "product_id", "seller_id",
        "shipping_limit_date", "price", "freight_value",
    ]),
    "order_payments": all_strings([
        "order_id", "payment_sequential", "payment_type",
        "payment_installments", "payment_value",
    ]),
    "order_reviews": all_strings([
        "review_id", "order_id", "review_score", "review_comment_title",
        "review_comment_message", "review_creation_date", "review_answer_timestamp",
    ]),
    "customers": all_strings([
        "customer_id", "customer_unique_id", "customer_zip_code_prefix",
        "customer_city", "customer_state",
    ]),
    "products": all_strings([
        "product_id", "product_category_name", "product_name_lenght",
        "product_description_lenght", "product_photos_qty", "product_weight_g",
        "product_length_cm", "product_height_cm", "product_width_cm",
    ]),
    "sellers": all_strings([
        "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state",
    ]),
    "geolocation": all_strings([
        "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
        "geolocation_city", "geolocation_state",
    ]),
    "category_translation": all_strings([
        "product_category_name", "product_category_name_english",
    ]),
}

# table name -> source file name
BATCH_TABLES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
}
STATIC_TABLES = {
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

# COMMAND ----------

def already_loaded(table, folder):
    return (
        spark.table(LOG_TABLE)
        .filter((F.col("table_name") == table) & (F.col("batch_id") == folder))
        .limit(1)
        .count()
        > 0
    )


def read_raw(table, folder, file_name):
    reader = spark.read.schema(SCHEMAS[table]).option("header", True)
    if table == "order_reviews":
        # Review comments contain line breaks inside quotes. Without multiLine the
        # file is split into too many rows.
        reader = (
            reader.option("multiLine", True)
            .option("quote", '"')
            .option("escape", '"')
        )
    return (
        reader.csv(f"{BASE}/{folder}/{file_name}")
        .select("*", F.col("_metadata.file_path").alias("source_file"))
        .withColumn("batch_id", F.lit(folder))
        .withColumn("ingestion_ts", F.current_timestamp())
    )


def log_load(folder, table, rows):
    (
        spark.createDataFrame(
            [(folder, table, rows)],
            "batch_id string, table_name string, rows_loaded long",
        )
        .withColumn("ingested_at", F.current_timestamp())
        .write.mode("append")
        .saveAsTable(LOG_TABLE)
    )


def ingest(table, folder, file_name):
    if already_loaded(table, folder):
        print(f"SKIP   {folder:8} {table:22} already loaded")
        return

    df = read_raw(table, folder, file_name)
    rows = df.count()

    writer = df.write.format("delta")
    if folder == "static":
        writer = writer.mode("overwrite").option("overwriteSchema", "true")
    else:
        writer = writer.mode("append")
    writer.saveAsTable(f"{BRONZE}.{table}")

    log_load(folder, table, rows)
    print(f"LOADED {folder:8} {table:22} {rows:>10,} rows")


# COMMAND ----------

folders = ["static", "batch_1", "batch_2", "batch_3"] if requested == "all" else [requested]

for folder in folders:
    tables = STATIC_TABLES if folder == "static" else BATCH_TABLES
    for table, file_name in tables.items():
        ingest(table, folder, file_name)

print("Done")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks
# MAGIC Run these after loading. Final expected totals once `static` and all 3 batches are loaded:
# MAGIC orders 99,441 | order_items 112,650 | order_payments 103,886 | order_reviews 99,224 |
# MAGIC customers 99,441 | products 32,951 | sellers 3,095 | geolocation 1,000,163 | category_translation 71

# COMMAND ----------

display(spark.table(LOG_TABLE).orderBy("ingested_at"))

# COMMAND ----------

EXPECTED = {
    "orders": 99441, "order_items": 112650, "order_payments": 103886,
    "order_reviews": 99224, "customers": 99441, "products": 32951,
    "sellers": 3095, "geolocation": 1000163, "category_translation": 71,
}

for table, expected in EXPECTED.items():
    if spark.catalog.tableExists(f"{BRONZE}.{table}"):
        actual = spark.table(f"{BRONZE}.{table}").count()
        status = "OK" if actual == expected else "not complete / mismatch"
        print(f"{table:22} {actual:>10,} / {expected:>10,}  {status}")
    else:
        print(f"{table:22} not loaded yet")

# COMMAND ----------

# Rows per batch (orders)
display(
    spark.table(f"{BRONZE}.orders")
    .groupBy("batch_id")
    .count()
    .orderBy("batch_id")
)

# COMMAND ----------

# Sample with metadata columns
display(spark.table(f"{BRONZE}.orders").limit(5))

# COMMAND ----------

# Delta history: one APPEND per batch
display(spark.sql(f"DESCRIBE HISTORY {BRONZE}.orders").select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# OPTIONAL: full reset to re-run Day 2 from scratch. Uncomment, run once, then re-comment.
# for t in list(SCHEMAS.keys()) + ["ingestion_log"]:
#     spark.sql(f"DROP TABLE IF EXISTS {BRONZE}.{t}")