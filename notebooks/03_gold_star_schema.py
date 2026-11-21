# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 - Gold: star schema and business aggregates
# MAGIC Builds analytics-ready tables in `workspace.gold` from the Silver layer.
# MAGIC
# MAGIC **Star schema**
# MAGIC - Dimensions: `dim_date`, `dim_customer`, `dim_seller`, `dim_product`
# MAGIC - Facts: `fact_orders` (grain: one row per order), `fact_order_items` (grain: one row per order item)
# MAGIC
# MAGIC **Aggregates for the dashboard**
# MAGIC `agg_monthly_sales`, `agg_category_performance`, `agg_state_delivery`, `agg_seller_performance`, `agg_payment_mix`
# MAGIC
# MAGIC **Design decisions**
# MAGIC - Gold is rebuilt in full from Silver on every run (simple and idempotent at this data size)
# MAGIC - Natural business keys are used as keys (`order_id`, `customer_id`, ...); `date_key` is an integer `yyyyMMdd`
# MAGIC - A "valid sale" is an order whose status is not `canceled` or `unavailable`; revenue aggregates use valid sales only
# MAGIC - Delivery metrics (days, delay, late flag) are only computed for delivered orders
# MAGIC - `customer_id` in Olist is one id per order; `customer_unique_id` identifies the real customer

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

orders = spark.table(f"{SILVER}.orders")
items = spark.table(f"{SILVER}.order_items")
payments = spark.table(f"{SILVER}.order_payments")
reviews = spark.table(f"{SILVER}.order_reviews")
customers = spark.table(f"{SILVER}.customers")
sellers = spark.table(f"{SILVER}.sellers")
products = spark.table(f"{SILVER}.products")
geo = spark.table(f"{SILVER}.geolocation").select(
    F.col("zip_code_prefix").alias("zip"), "lat", "lng"
)

def write_gold(df, name):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD}.{name}")
    )
    print(f"WROTE {name:26} {spark.table(f'{GOLD}.{name}').count():>9,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensions

# COMMAND ----------

# dim_date: one row per calendar day from the first purchase to the last estimated delivery
bounds = orders.agg(
    F.min(F.to_date("order_purchase_timestamp")).alias("d0"),
    F.max(F.to_date("order_estimated_delivery_date")).alias("d1"),
).first()

dim_date = (
    spark.sql(f"SELECT explode(sequence(DATE'{bounds['d0']}', DATE'{bounds['d1']}', INTERVAL 1 DAY)) AS date")
    .select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.dayofmonth("date").alias("day"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.weekofyear("date").alias("week_of_year"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )
)
write_gold(dim_date, "dim_date")

# COMMAND ----------

dim_customer = (
    customers.join(geo, customers["customer_zip_code_prefix"] == geo["zip"], "left")
    .select(
        "customer_id",
        "customer_unique_id",
        F.col("customer_zip_code_prefix").alias("zip_code_prefix"),
        F.col("customer_city").alias("city"),
        F.col("customer_state").alias("state"),
        "lat",
        "lng",
    )
)
write_gold(dim_customer, "dim_customer")

dim_seller = (
    sellers.join(geo, sellers["seller_zip_code_prefix"] == geo["zip"], "left")
    .select(
        "seller_id",
        F.col("seller_zip_code_prefix").alias("zip_code_prefix"),
        F.col("seller_city").alias("city"),
        F.col("seller_state").alias("state"),
        "lat",
        "lng",
    )
)
write_gold(dim_seller, "dim_seller")

dim_product = products.select(
    "product_id",
    F.col("product_category_name").alias("category_name_pt"),
    F.col("product_category_name_english").alias("category_name"),
    "product_name_length",
    "product_description_length",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
    (F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm")).alias("product_volume_cm3"),
)
write_gold(dim_product, "dim_product")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Facts

# COMMAND ----------

# fact_orders: one row per order, with item totals, payment info, review and delivery metrics
items_agg = items.groupBy("order_id").agg(
    F.count("*").alias("items_count"),
    F.sum("price").alias("items_revenue"),
    F.sum("freight_value").alias("freight_total"),
)

payments_agg = payments.groupBy("order_id").agg(
    F.sum("payment_value").alias("payment_total"),
    F.max("payment_installments").alias("max_installments"),
)

main_payment = (
    payments.withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("order_id").orderBy(F.col("payment_value").desc(), F.col("payment_sequential"))
        ),
    )
    .filter("rn = 1")
    .select("order_id", F.col("payment_type").alias("main_payment_type"))
)

# Some orders have several reviews: keep the most recent one
latest_review = (
    reviews.withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("order_id").orderBy(
                F.col("review_creation_date").desc(),
                F.col("review_answer_timestamp").desc(),
                F.col("review_id"),
            )
        ),
    )
    .filter("rn = 1")
    .select("order_id", "review_score")
)

fact_orders = (
    orders.join(items_agg, "order_id", "left")
    .join(payments_agg, "order_id", "left")
    .join(main_payment, "order_id", "left")
    .join(latest_review, "order_id", "left")
    .withColumn("is_valid_sale", ~F.col("order_status").isin("canceled", "unavailable"))
    .withColumn(
        "is_delivered",
        (F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNotNull(),
    )
    .withColumn("delivery_days", F.when(
        F.col("is_delivered"),
        F.datediff("order_delivered_customer_date", "order_purchase_timestamp"),
    ))
    .withColumn("delivery_delay_days", F.when(
        F.col("is_delivered"),
        F.datediff("order_delivered_customer_date", "order_estimated_delivery_date"),
    ))
    .withColumn("is_late", F.when(F.col("is_delivered"), F.col("delivery_delay_days") > 0))
    .select(
        "order_id",
        "customer_id",
        F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int").alias("purchase_date_key"),
        "order_status",
        "is_valid_sale",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        F.coalesce(F.col("items_count"), F.lit(0)).alias("items_count"),
        F.coalesce(F.col("items_revenue"), F.lit(0)).cast("decimal(14,2)").alias("items_revenue"),
        F.coalesce(F.col("freight_total"), F.lit(0)).cast("decimal(14,2)").alias("freight_total"),
        (F.coalesce(F.col("items_revenue"), F.lit(0)) + F.coalesce(F.col("freight_total"), F.lit(0)))
        .cast("decimal(14,2)").alias("order_total"),
        F.coalesce(F.col("payment_total"), F.lit(0)).cast("decimal(14,2)").alias("payment_total"),
        "max_installments",
        "main_payment_type",
        "review_score",
        "is_delivered",
        "delivery_days",
        "delivery_delay_days",
        "is_late",
    )
)
write_gold(fact_orders, "fact_orders")

# COMMAND ----------

# fact_order_items: one row per item sold (order line)
fact_order_items = (
    items.join(
        orders.select("order_id", "customer_id", "order_status", "order_purchase_timestamp"),
        "order_id",
        "left",
    )
    .select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "customer_id",
        F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int").alias("purchase_date_key"),
        "order_status",
        "shipping_limit_date",
        "price",
        "freight_value",
        (F.col("price") + F.col("freight_value")).alias("item_total"),
    )
)
write_gold(fact_order_items, "fact_order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregates (read from the Gold star schema, not from Silver)

# COMMAND ----------

fo = spark.table(f"{GOLD}.fact_orders")
foi = spark.table(f"{GOLD}.fact_order_items")
dd = spark.table(f"{GOLD}.dim_date")
dc = spark.table(f"{GOLD}.dim_customer")
dp = spark.table(f"{GOLD}.dim_product")
ds = spark.table(f"{GOLD}.dim_seller")

# Monthly sales
agg_monthly_sales = (
    fo.filter("is_valid_sale")
    .join(dc.select("customer_id", "customer_unique_id"), "customer_id", "left")
    .join(
        dd.select(F.col("date_key").alias("purchase_date_key"), "year", "month", "year_month"),
        "purchase_date_key",
        "left",
    )
    .groupBy("year", "month", "year_month")
    .agg(
        F.count("*").alias("orders"),
        F.sum("items_count").alias("items_sold"),
        F.sum("items_revenue").alias("revenue"),
        F.sum("freight_total").alias("freight"),
        F.countDistinct("customer_unique_id").alias("unique_customers"),
        F.round(F.avg("review_score"), 2).alias("avg_review_score"),
    )
    .withColumn("avg_order_value", F.round(F.col("revenue") / F.col("orders"), 2))
    .orderBy("year_month")
)
write_gold(agg_monthly_sales, "agg_monthly_sales")

# Category performance
order_info = fo.select("order_id", "is_valid_sale", "review_score")
agg_category_performance = (
    foi.join(order_info, "order_id", "left")
    .filter("is_valid_sale")
    .join(dp.select("product_id", "category_name"), "product_id", "left")
    .groupBy("category_name")
    .agg(
        F.count("*").alias("items_sold"),
        F.countDistinct("order_id").alias("orders"),
        F.sum("price").alias("revenue"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.round(F.avg("review_score"), 2).alias("avg_review_score"),
    )
    .orderBy(F.col("revenue").desc())
)
write_gold(agg_category_performance, "agg_category_performance")

# COMMAND ----------

# Delivery performance by customer state (delivered orders only)
agg_state_delivery = (
    fo.filter("is_delivered")
    .join(dc.select("customer_id", F.col("state").alias("customer_state")), "customer_id", "left")
    .groupBy("customer_state")
    .agg(
        F.count("*").alias("delivered_orders"),
        F.round(F.avg("delivery_days"), 1).alias("avg_delivery_days"),
        F.sum(F.col("is_late").cast("int")).alias("late_orders"),
        F.round(F.avg(F.col("is_late").cast("int")) * 100, 2).alias("late_rate_pct"),
        F.round(F.avg("review_score"), 2).alias("avg_review_score"),
    )
    .orderBy(F.col("delivered_orders").desc())
)
write_gold(agg_state_delivery, "agg_state_delivery")

# Seller performance
order_metrics = fo.select("order_id", "is_valid_sale", "review_score", "is_delivered", "delivery_days", "is_late")
agg_seller_performance = (
    foi.join(order_metrics, "order_id", "left")
    .filter("is_valid_sale")
    .groupBy("seller_id")
    .agg(
        F.count("*").alias("items_sold"),
        F.countDistinct("order_id").alias("orders"),
        F.sum("price").alias("revenue"),
        F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        F.round(F.avg("delivery_days"), 1).alias("avg_delivery_days"),
        F.round(F.avg(F.col("is_late").cast("int")) * 100, 2).alias("late_rate_pct"),
    )
    .join(ds.select("seller_id", F.col("city").alias("seller_city"), F.col("state").alias("seller_state")), "seller_id", "left")
    .orderBy(F.col("revenue").desc())
)
write_gold(agg_seller_performance, "agg_seller_performance")

# Payment mix by month
agg_payment_mix = (
    payments.join(fo.select("order_id", "purchase_date_key", "is_valid_sale"), "order_id", "left")
    .filter("is_valid_sale")
    .join(dd.select(F.col("date_key").alias("purchase_date_key"), "year_month"), "purchase_date_key", "left")
    .groupBy("year_month", "payment_type")
    .agg(
        F.count("*").alias("payments"),
        F.countDistinct("order_id").alias("orders"),
        F.sum("payment_value").alias("total_value"),
        F.round(F.avg("payment_installments"), 2).alias("avg_installments"),
    )
    .orderBy("year_month", "payment_type")
)
write_gold(agg_payment_mix, "agg_payment_mix")

print("Gold build complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks

# COMMAND ----------

# Row counts. Expected: dim_customer 99,441 | dim_seller 3,095 | dim_product 32,951 |
# fact_orders 99,441 | fact_order_items 112,650
for t in ["dim_date", "dim_customer", "dim_seller", "dim_product", "fact_orders", "fact_order_items",
          "agg_monthly_sales", "agg_category_performance", "agg_state_delivery",
          "agg_seller_performance", "agg_payment_mix"]:
    print(f"{t:26} {spark.table(f'{GOLD}.{t}').count():>9,}")

# COMMAND ----------

# Money reconciliation: Gold must match Silver exactly (all differences must be 0.00)
silver_price = items.agg(F.sum("price")).first()[0]
silver_freight = items.agg(F.sum("freight_value")).first()[0]
silver_paid = payments.agg(F.sum("payment_value")).first()[0]

gold_item_price = foi.agg(F.sum("price")).first()[0]
gold_order_rev = fo.agg(F.sum("items_revenue")).first()[0]
gold_order_freight = fo.agg(F.sum("freight_total")).first()[0]
gold_paid = fo.agg(F.sum("payment_total")).first()[0]

print(f"item price        silver={silver_price:,.2f}  fact_order_items={gold_item_price:,.2f}  fact_orders={gold_order_rev:,.2f}")
print(f"freight           silver={silver_freight:,.2f}  fact_orders={gold_order_freight:,.2f}")
print(f"payments          silver={silver_paid:,.2f}  fact_orders={gold_paid:,.2f}")
print("difference price  :", silver_price - gold_item_price, silver_price - gold_order_rev)
print("difference freight:", silver_freight - gold_order_freight)
print("difference paid   :", silver_paid - gold_paid)

# COMMAND ----------

# Key uniqueness: every count must be 0
KEYS = {
    "dim_date": ["date_key"], "dim_customer": ["customer_id"], "dim_seller": ["seller_id"],
    "dim_product": ["product_id"], "fact_orders": ["order_id"],
    "fact_order_items": ["order_id", "order_item_id"],
}
for t, keys in KEYS.items():
    d = spark.table(f"{GOLD}.{t}").groupBy(*keys).count().filter("count > 1").count()
    print(f"{t:18} duplicate keys: {d}")

# Foreign keys: orphans must be 0
FKS = [
    ("fact_orders", "customer_id", "dim_customer", "customer_id"),
    ("fact_orders", "purchase_date_key", "dim_date", "date_key"),
    ("fact_order_items", "order_id", "fact_orders", "order_id"),
    ("fact_order_items", "product_id", "dim_product", "product_id"),
    ("fact_order_items", "seller_id", "dim_seller", "seller_id"),
    ("fact_order_items", "customer_id", "dim_customer", "customer_id"),
    ("fact_order_items", "purchase_date_key", "dim_date", "date_key"),
]
for child, ck, parent, pk in FKS:
    c = spark.table(f"{GOLD}.{child}")
    p = spark.table(f"{GOLD}.{parent}")
    orphans = c.join(p, c[ck] == p[pk], "left_anti").count()
    print(f"{child}.{ck} -> {parent}.{pk}: {orphans} orphans")

# COMMAND ----------

# Business sanity: do the numbers look right?
display(spark.table(f"{GOLD}.agg_monthly_sales").orderBy("year_month"))

# COMMAND ----------

display(spark.table(f"{GOLD}.agg_category_performance").limit(10))

# COMMAND ----------

display(spark.table(f"{GOLD}.agg_state_delivery").limit(10))

# COMMAND ----------

display(spark.table(f"{GOLD}.agg_seller_performance").limit(10))

# COMMAND ----------

display(spark.table(f"{GOLD}.fact_orders").limit(5))

# COMMAND ----------

# OPTIONAL: full reset to re-run Day 4 from scratch. Uncomment, run once, then re-comment.
# for t in ["dim_date", "dim_customer", "dim_seller", "dim_product", "fact_orders", "fact_order_items",
#           "agg_monthly_sales", "agg_category_performance", "agg_state_delivery",
#           "agg_seller_performance", "agg_payment_mix"]:
#     spark.sql(f"DROP TABLE IF EXISTS {GOLD}.{t}")