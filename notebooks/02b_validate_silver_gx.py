# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02b - Validate Silver with Great Expectations
# MAGIC Runs between `silver` and `gold` in the Job. It validates every Silver table with the suites in `dq_expectations`
# MAGIC and stores one row per expectation in `workspace.ops.dq_results`.
# MAGIC
# MAGIC - **critical** expectation fails -> the notebook raises an error -> the Job stops and Gold is not rebuilt
# MAGIC - **warning** expectation fails -> logged only
# MAGIC
# MAGIC Job parameter used: `run_id` (defaults to `manual` when run by hand).

# COMMAND ----------

# MAGIC %pip install -q great_expectations==1.25.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SILVER = f"{CATALOG}.silver"
OPS = f"{CATALOG}.ops"
DQ_RESULTS = f"{OPS}.dq_results"

dbutils.widgets.text("run_id", "manual", "run_id (set by the Job)")
run_id = dbutils.widgets.get("run_id").strip() or "manual"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DQ_RESULTS} (
        run_id             STRING,
        run_ts             TIMESTAMP,
        table_name         STRING,
        expectation        STRING,
        column_name        STRING,
        severity           STRING,
        success            BOOLEAN,
        observed_value     STRING,
        unexpected_count   BIGINT,
        unexpected_percent DOUBLE,
        element_count      BIGINT
    )
""")
print(f"run_id = {run_id}")

# COMMAND ----------
# MAGIC %run ./dq_expectations

# COMMAND ----------

# Smoke test: does GX work directly on a Spark DataFrame in this compute?
# If not (for example with Spark Connect on serverless), fall back to pandas.
# Safe here because every Silver table is small (largest ~113k rows).
USE_PANDAS = False
try:
    validate_table(spark.table(f"{SILVER}.sellers").limit(10),
                   "smoke_test", [gxe.ExpectColumnValuesToNotBeNull(column="seller_id")])
    print("GX engine: Spark DataFrame")
except Exception as e:
    USE_PANDAS = True
    print(f"GX engine: pandas fallback ({type(e).__name__}: {str(e)[:150]})")

# COMMAND ----------

def to_pandas(df):
    # DECIMAL columns become Python Decimal objects in pandas; cast them to double first
    for f in df.schema.fields:
        if f.dataType.typeName() == "decimal":
            df = df.withColumn(f.name, F.col(f.name).cast("double"))
    return df.toPandas()


all_rows = []
for table, expectations in build_expectations().items():
    df = spark.table(f"{SILVER}.{table}")
    if USE_PANDAS:
        df = to_pandas(df)
    rows = validate_table(df, table, expectations, use_pandas=USE_PANDAS)
    all_rows.extend(rows)
    n_fail = sum(1 for r in rows if not r["success"])
    print(f"{table:15} {len(rows):>2} expectations, {n_fail} failed")

# COMMAND ----------

# Store the results (one row per expectation per run)
schema = ("table_name string, expectation string, column_name string, severity string, success boolean, "
          "observed_value string, unexpected_count long, unexpected_percent double, element_count long")
cols = ["table_name", "expectation", "column_name", "severity", "success",
        "observed_value", "unexpected_count", "unexpected_percent", "element_count"]

def _num(v, cast):
    return None if v is None else cast(v)

records = [
    (r["table_name"], r["expectation"], r["column_name"], r["severity"], r["success"],
     r["observed_value"], _num(r["unexpected_count"], int), _num(r["unexpected_percent"], float),
     _num(r["element_count"], int))
    for r in all_rows
]
(
    spark.createDataFrame(records, schema)
    .withColumn("run_id", F.lit(run_id))
    .withColumn("run_ts", F.current_timestamp())
    .select("run_id", "run_ts", *cols)
    .write.mode("append")
    .saveAsTable(DQ_RESULTS)
)

this_run = spark.table(DQ_RESULTS).filter(F.col("run_id") == run_id)
display(this_run.filter("success = false").orderBy("severity", "table_name"))

# COMMAND ----------

# Summary and gate
total = len(all_rows)
failed_critical = [r for r in all_rows if not r["success"] and r["severity"] == "critical"]
failed_warning = [r for r in all_rows if not r["success"] and r["severity"] == "warning"]
print(f"{total} expectations | {total - len(failed_critical) - len(failed_warning)} passed | "
      f"{len(failed_warning)} warnings | {len(failed_critical)} critical failures")

for r in failed_warning:
    print(f"WARNING  {r['table_name']}.{r['column_name']} {r['expectation']} unexpected={r['unexpected_count']}")

if failed_critical:
    lines = [f"{r['table_name']}.{r['column_name']} {r['expectation']} (unexpected={r['unexpected_count']})"
             for r in failed_critical]
    raise Exception("Great Expectations validation FAILED:\n" + "\n".join(lines))
print("Great Expectations validation passed")
