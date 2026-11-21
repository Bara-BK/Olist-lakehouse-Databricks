# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 06 - Export the Gold star schema for Power BI
# MAGIC Writes the Gold dimension and fact tables as CSV files into the Volume `workspace.landing.exports`.
# MAGIC Download them from **Catalog -> workspace -> landing -> exports** and load them in Power BI Desktop.
# MAGIC
# MAGIC The tables are small (largest: ~112k rows), so a single CSV per table through pandas is fine.

# COMMAND ----------

CATALOG = "workspace"
GOLD = f"{CATALOG}.gold"
EXPORT_DIR = f"/Volumes/{CATALOG}/landing/exports"

TABLES = ["dim_date", "dim_customer", "dim_seller", "dim_product", "fact_orders", "fact_order_items"]

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.exports")

# COMMAND ----------

for t in TABLES:
    pdf = spark.table(f"{GOLD}.{t}").toPandas()
    path = f"{EXPORT_DIR}/{t}.csv"
    pdf.to_csv(path, index=False)
    print(f"{t:18} {len(pdf):>9,} rows -> {path}")

# COMMAND ----------

display(dbutils.fs.ls(EXPORT_DIR))