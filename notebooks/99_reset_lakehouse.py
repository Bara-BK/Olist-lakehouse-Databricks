# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 99 - Reset the lakehouse (optional)
# MAGIC Drops **all tables** in `workspace.bronze`, `workspace.silver` and `workspace.gold` so the pipeline can be
# MAGIC rebuilt from the raw CSV files in the landing Volume. The Volume and the `ops` log tables are **not** touched.
# MAGIC
# MAGIC Safety: nothing happens unless the `confirm` widget is set to `YES`.

# COMMAND ----------

CATALOG = "workspace"
SCHEMAS = ["bronze", "silver", "gold"]

dbutils.widgets.text("confirm", "NO", "Type YES to drop all bronze/silver/gold tables")
confirm = dbutils.widgets.get("confirm").strip()

# COMMAND ----------

if confirm != "YES":
    print("Nothing dropped. Set the confirm widget to YES and run again to reset.")
else:
    for schema in SCHEMAS:
        for t in spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect():
            spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{schema}.{t['tableName']}")
            print(f"dropped {schema}.{t['tableName']}")
    print("Reset complete. Run the Job with batch = all to rebuild everything.")