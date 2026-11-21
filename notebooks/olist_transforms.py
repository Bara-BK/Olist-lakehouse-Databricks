# Databricks notebook source
# MAGIC %md
# MAGIC # olist_transforms - shared Silver logic
# MAGIC Pure transformation and data quality code (no reads or writes): typed casts, cleaning functions, quality rules,
# MAGIC deduplication and the geolocation aggregation.
# MAGIC
# MAGIC - Loaded by `02_silver_transformation` with `%run ./olist_transforms`
# MAGIC - Imported by the pytest tests in `tests/` (this file is also valid plain Python)
# MAGIC
# MAGIC One source of truth: the code the pipeline runs is the code the tests check.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# Cleaning helpers. try_* functions return NULL on bad input (safe under ANSI mode).

def clean_str(c):
    return F.expr(f"nullif(trim({c}), '')")

def try_ts(c):
    return F.expr(f"try_to_timestamp({c}, 'yyyy-MM-dd HH:mm:ss')")

def try_int(c):
    return F.expr(f"try_cast({c} AS INT)")

def try_dbl(c):
    return F.expr(f"try_cast({c} AS DOUBLE)")

def try_dec(c):
    return F.expr(f"try_cast({c} AS DECIMAL(12,2))")


# Lineage columns carried from Bronze (the raw JSON is only used for the quarantine table)
META = [
    F.col("batch_id").alias("source_batch_id"),
    F.col("ingestion_ts").alias("bronze_ingestion_ts"),
    F.col("_raw_json"),
]

ORDER_STATUSES = ["delivered", "shipped", "canceled", "unavailable",
                  "invoiced", "processing", "created", "approved"]
PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card", "not_defined"]

# COMMAND ----------

# Transformations: Bronze strings -> typed, standardized columns

def t_orders(df):
    return df.select(
        clean_str("order_id").alias("order_id"),
        clean_str("customer_id").alias("customer_id"),
        F.lower(clean_str("order_status")).alias("order_status"),
        try_ts("order_purchase_timestamp").alias("order_purchase_timestamp"),
        try_ts("order_approved_at").alias("order_approved_at"),
        try_ts("order_delivered_carrier_date").alias("order_delivered_carrier_date"),
        try_ts("order_delivered_customer_date").alias("order_delivered_customer_date"),
        try_ts("order_estimated_delivery_date").alias("order_estimated_delivery_date"),
        *META,
    )

def t_order_items(df):
    return df.select(
        clean_str("order_id").alias("order_id"),
        try_int("order_item_id").alias("order_item_id"),
        clean_str("product_id").alias("product_id"),
        clean_str("seller_id").alias("seller_id"),
        try_ts("shipping_limit_date").alias("shipping_limit_date"),
        try_dec("price").alias("price"),
        try_dec("freight_value").alias("freight_value"),
        *META,
    )

def t_order_payments(df):
    return df.select(
        clean_str("order_id").alias("order_id"),
        try_int("payment_sequential").alias("payment_sequential"),
        F.lower(clean_str("payment_type")).alias("payment_type"),
        try_int("payment_installments").alias("payment_installments"),
        try_dec("payment_value").alias("payment_value"),
        *META,
    )

def t_order_reviews(df):
    return df.select(
        clean_str("review_id").alias("review_id"),
        clean_str("order_id").alias("order_id"),
        try_int("review_score").alias("review_score"),
        clean_str("review_comment_title").alias("review_comment_title"),
        clean_str("review_comment_message").alias("review_comment_message"),
        try_ts("review_creation_date").alias("review_creation_date"),
        try_ts("review_answer_timestamp").alias("review_answer_timestamp"),
        *META,
    )

def t_customers(df):
    return df.select(
        clean_str("customer_id").alias("customer_id"),
        clean_str("customer_unique_id").alias("customer_unique_id"),
        clean_str("customer_zip_code_prefix").alias("customer_zip_code_prefix"),
        F.lower(clean_str("customer_city")).alias("customer_city"),
        F.upper(clean_str("customer_state")).alias("customer_state"),
        *META,
    )

def t_sellers(df):
    return df.select(
        clean_str("seller_id").alias("seller_id"),
        clean_str("seller_zip_code_prefix").alias("seller_zip_code_prefix"),
        F.lower(clean_str("seller_city")).alias("seller_city"),
        F.upper(clean_str("seller_state")).alias("seller_state"),
        *META,
    )

def t_products(df, translation_raw):
    # Fix the source misspellings (lenght -> length) and add the English category name
    translation = (
        translation_raw
        .select(
            clean_str("product_category_name").alias("tr_name"),
            clean_str("product_category_name_english").alias("tr_english"),
        )
        .dropDuplicates(["tr_name"])
    )
    base = df.select(
        clean_str("product_id").alias("product_id"),
        F.lower(clean_str("product_category_name")).alias("product_category_name"),
        try_int("product_name_lenght").alias("product_name_length"),
        try_int("product_description_lenght").alias("product_description_length"),
        try_int("product_photos_qty").alias("product_photos_qty"),
        try_dbl("product_weight_g").alias("product_weight_g"),
        try_dbl("product_length_cm").alias("product_length_cm"),
        try_dbl("product_height_cm").alias("product_height_cm"),
        try_dbl("product_width_cm").alias("product_width_cm"),
        *META,
    )
    return (
        base.join(translation, base["product_category_name"] == translation["tr_name"], "left")
        .withColumn(
            "product_category_name_english",
            F.coalesce(F.col("tr_english"), F.col("product_category_name"), F.lit("unknown")),
        )
        .drop("tr_name", "tr_english")
    )

def t_geolocation(df):
    return df.select(
        clean_str("geolocation_zip_code_prefix").alias("geolocation_zip_code_prefix"),
        try_dbl("geolocation_lat").alias("geolocation_lat"),
        try_dbl("geolocation_lng").alias("geolocation_lng"),
        F.lower(clean_str("geolocation_city")).alias("geolocation_city"),
        F.upper(clean_str("geolocation_state")).alias("geolocation_state"),
        *META,
    )


def post_geolocation(valid):
    # The raw table has many GPS points per zip code prefix.
    # Silver keeps one row per prefix: average coordinates + most frequent city/state.
    z = "geolocation_zip_code_prefix"
    centroid = valid.groupBy(z).agg(
        F.avg("geolocation_lat").alias("lat"),
        F.avg("geolocation_lng").alias("lng"),
        F.count("*").alias("gps_points"),
    )
    w = Window.partitionBy(z).orderBy(F.col("n").desc(), F.col("geolocation_city"))
    top_city = (
        valid.groupBy(z, "geolocation_city", "geolocation_state")
        .agg(F.count("*").alias("n"))
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .select(z, "geolocation_city", "geolocation_state")
    )
    return centroid.join(top_city, z).select(
        F.col(z).alias("zip_code_prefix"),
        F.col("geolocation_city").alias("city"),
        F.col("geolocation_state").alias("state"),
        "lat", "lng", "gps_points",
    )

# COMMAND ----------

# Data quality rules. Each rule is a condition that must be TRUE for the row to pass.
# The rule name becomes the rejection reason. NULL counts as a failure.

RULES = {
    "orders": {
        "order_id_missing": F.col("order_id").isNotNull(),
        "customer_id_missing": F.col("customer_id").isNotNull(),
        "purchase_timestamp_invalid": F.col("order_purchase_timestamp").isNotNull(),
        "order_status_invalid": F.col("order_status").isin(ORDER_STATUSES),
    },
    "order_items": {
        "order_id_missing": F.col("order_id").isNotNull(),
        "order_item_id_invalid": F.col("order_item_id").isNotNull(),
        "product_id_missing": F.col("product_id").isNotNull(),
        "seller_id_missing": F.col("seller_id").isNotNull(),
        "price_invalid": F.col("price") >= 0,
        "freight_value_invalid": F.col("freight_value") >= 0,
    },
    "order_payments": {
        "order_id_missing": F.col("order_id").isNotNull(),
        "payment_sequential_invalid": F.col("payment_sequential").isNotNull(),
        "payment_type_invalid": F.col("payment_type").isin(PAYMENT_TYPES),
        "payment_installments_invalid": F.col("payment_installments") >= 0,
        "payment_value_invalid": F.col("payment_value") >= 0,
    },
    "order_reviews": {
        "review_id_missing": F.col("review_id").isNotNull(),
        "order_id_missing": F.col("order_id").isNotNull(),
        "review_score_out_of_range": F.col("review_score").between(1, 5),
    },
    "customers": {
        "customer_id_missing": F.col("customer_id").isNotNull(),
        "customer_unique_id_missing": F.col("customer_unique_id").isNotNull(),
        "customer_state_invalid": F.length("customer_state") == 2,
    },
    "sellers": {
        "seller_id_missing": F.col("seller_id").isNotNull(),
        "seller_state_invalid": F.length("seller_state") == 2,
    },
    "products": {
        "product_id_missing": F.col("product_id").isNotNull(),
        "product_weight_negative": F.col("product_weight_g").isNull() | (F.col("product_weight_g") >= 0),
    },
    "geolocation": {
        "zip_code_missing": F.col("geolocation_zip_code_prefix").isNotNull(),
        # Rough bounding box of Brazil; GPS points outside it are bad data
        "latitude_outside_brazil": F.col("geolocation_lat").between(-35.0, 6.0),
        "longitude_outside_brazil": F.col("geolocation_lng").between(-75.0, -34.0),
    },
}

# table -> (kind, business key after typing, transform, post-processing)
SPECS = {
    "orders":         dict(kind="batch",  keys=["order_id"], transform=t_orders),
    "order_items":    dict(kind="batch",  keys=["order_id", "order_item_id"], transform=t_order_items),
    "order_payments": dict(kind="batch",  keys=["order_id", "payment_sequential"], transform=t_order_payments),
    "order_reviews":  dict(kind="batch",  keys=["review_id", "order_id"], transform=t_order_reviews),
    "customers":      dict(kind="static", keys=["customer_id"], transform=t_customers),
    "products":       dict(kind="static", keys=["product_id"], transform=t_products),
    "sellers":        dict(kind="static", keys=["seller_id"], transform=t_sellers),
    "geolocation":    dict(kind="static", keys=None, transform=t_geolocation, post=post_geolocation),
}
BATCH_TABLES = [t for t, s in SPECS.items() if s["kind"] == "batch"]
STATIC_TABLES = [t for t, s in SPECS.items() if s["kind"] == "static"]

# COMMAND ----------

def split_by_rules(df, rules):
    """Return (valid_rows, rejected_rows). Rejected rows carry the failed rule names in _failed."""
    failures = [F.when(~F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in rules.items()]
    flagged = df.withColumn("_failed", F.concat_ws(",", *failures))
    valid = flagged.filter(F.col("_failed") == "").drop("_failed")
    rejected = flagged.filter(F.col("_failed") != "")
    return valid, rejected


def deduplicate(df, keys):
    """Keep one row per business key: the most recent Bronze load wins."""
    w = Window.partitionBy(*keys).orderBy(F.col("bronze_ingestion_ts").desc(), F.col("source_batch_id").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
