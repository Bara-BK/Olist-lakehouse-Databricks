# Databricks notebook source
catalog = "workspace"   # if your Catalog sidebar shows a different default catalog, use that name
spark.sql(f"USE CATALOG {catalog}")

for schema in ["landing", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

spark.sql("CREATE VOLUME IF NOT EXISTS landing.raw")
print("Schemas and volume ready")

# COMMAND ----------

base = "/Volumes/workspace/landing/raw"   # adjust the catalog name if you changed it above

for folder in ["static", "batch_1", "batch_2", "batch_3"]:
    print(folder, [f.name for f in dbutils.fs.ls(f"{base}/{folder}")])

df = spark.read.option("header", True).csv(f"{base}/batch_1/olist_orders_dataset.csv")
print(df.count())
df.printSchema()
display(df.limit(5))