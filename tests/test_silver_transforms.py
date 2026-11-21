"""Unit tests for the Silver layer logic (casting, cleaning, quality rules, deduplication)."""
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

import olist_transforms as t

# Bronze column contracts (what the Bronze layer delivers: all strings)
ORDERS = ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
          "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"]
ITEMS = ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"]
PAYMENTS = ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]
REVIEWS = ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message",
           "review_creation_date", "review_answer_timestamp"]
CUSTOMERS = ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]
PRODUCTS = ["product_id", "product_category_name", "product_name_lenght", "product_description_lenght",
            "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]
GEO = ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_city", "geolocation_state"]


def bronze(spark, columns, rows, batch_id="batch_1", ts="2026-01-01 00:00:00"):
    """Build a Bronze-like DataFrame: string columns + the metadata columns Bronze adds."""
    df = spark.createDataFrame(rows, ", ".join(f"{c} string" for c in columns))
    return (
        df.withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingestion_ts", F.to_timestamp(F.lit(ts)))
        .withColumn("source_file", F.lit("test.csv"))
        .withColumn("_raw_json", F.to_json(F.struct(*columns)))
    )


def run(df, table, *extra):
    """Apply the transform and the quality rules of a table. Returns (valid_rows, rejected_rows)."""
    typed = t.SPECS[table]["transform"](df, *extra)
    return t.split_by_rules(typed, t.RULES[table])


def reasons(rejected):
    return [r["_failed"] for r in rejected.select("_failed").collect()]


# ---------------------------------------------------------------- orders

def test_valid_order_is_typed_and_standardized(spark):
    df = bronze(spark, ORDERS, [("o1", "c1", " Delivered ", "2017-10-02 10:56:33", None, None, None, "2017-10-18 00:00:00")])
    valid, rejected = run(df, "orders")
    row = valid.collect()[0]
    assert rejected.count() == 0
    assert row["order_status"] == "delivered"
    assert row["order_purchase_timestamp"].year == 2017
    assert row["order_approved_at"] is None


def test_invalid_order_status_is_rejected(spark):
    df = bronze(spark, ORDERS, [("o1", "c1", "banana", "2017-10-02 10:56:33", None, None, None, None)])
    valid, rejected = run(df, "orders")
    assert valid.count() == 0
    assert reasons(rejected) == ["order_status_invalid"]


def test_unparseable_purchase_timestamp_is_rejected_not_a_crash(spark):
    df = bronze(spark, ORDERS, [("o1", "c1", "delivered", "not a date", None, None, None, None)])
    valid, rejected = run(df, "orders")
    assert valid.count() == 0
    assert reasons(rejected) == ["purchase_timestamp_invalid"]


def test_missing_keys_list_every_failed_rule(spark):
    df = bronze(spark, ORDERS, [(None, "", "delivered", "2017-10-02 10:56:33", None, None, None, None)])
    _, rejected = run(df, "orders")
    assert set(reasons(rejected)[0].split(",")) == {"order_id_missing", "customer_id_missing"}


def test_rejected_row_keeps_the_original_raw_values(spark):
    df = bronze(spark, ORDERS, [("o1", "c1", "banana", "2017-10-02 10:56:33", None, None, None, None)])
    _, rejected = run(df, "orders")
    raw = rejected.select("_raw_json").first()[0]
    assert '"order_status":"banana"' in raw


# ---------------------------------------------------------------- order items and payments

def test_item_price_is_decimal_and_negative_price_rejected(spark):
    df = bronze(spark, ITEMS, [
        ("o1", "1", "p1", "s1", "2017-10-02 10:56:33", "58.90", "13.29"),
        ("o2", "1", "p1", "s1", "2017-10-02 10:56:33", "-5.00", "1.00"),
        ("o3", "1", "p1", "s1", "2017-10-02 10:56:33", "abc", "1.00"),
    ])
    valid, rejected = run(df, "order_items")
    assert valid.collect()[0]["price"] == Decimal("58.90")
    assert sorted(reasons(rejected)) == ["price_invalid", "price_invalid"]


def test_payment_type_and_value_rules(spark):
    df = bronze(spark, PAYMENTS, [
        ("o1", "1", "Credit_Card", "3", "100.00"),
        ("o2", "1", "bitcoin", "1", "10.00"),
        ("o3", "1", "boleto", "1", "-1.00"),
    ])
    valid, rejected = run(df, "order_payments")
    assert valid.collect()[0]["payment_type"] == "credit_card"
    assert sorted(reasons(rejected)) == ["payment_type_invalid", "payment_value_invalid"]


# ---------------------------------------------------------------- reviews

def test_review_score_must_be_between_1_and_5(spark):
    df = bronze(spark, REVIEWS, [
        ("r1", "o1", "5", None, "great", "2018-01-01 00:00:00", "2018-01-02 00:00:00"),
        ("r2", "o2", "6", None, None, "2018-01-01 00:00:00", "2018-01-02 00:00:00"),
        ("r3", "o3", "0", None, None, "2018-01-01 00:00:00", "2018-01-02 00:00:00"),
    ])
    valid, rejected = run(df, "order_reviews")
    assert valid.count() == 1
    assert reasons(rejected) == ["review_score_out_of_range"] * 2


def test_blank_review_comment_becomes_null(spark):
    df = bronze(spark, REVIEWS, [("r1", "o1", "4", "  ", "   ", "2018-01-01 00:00:00", "2018-01-02 00:00:00")])
    valid, _ = run(df, "order_reviews")
    row = valid.collect()[0]
    assert row["review_comment_title"] is None and row["review_comment_message"] is None


# ---------------------------------------------------------------- customers

def test_customer_text_is_standardized_and_bad_state_rejected(spark):
    df = bronze(spark, CUSTOMERS, [
        ("c1", "u1", "14409", "  Sao Paulo ", "sp"),
        ("c2", "u2", "14409", "Franca", "SPP"),
    ])
    valid, rejected = run(df, "customers")
    row = valid.collect()[0]
    assert (row["customer_city"], row["customer_state"]) == ("sao paulo", "SP")
    assert reasons(rejected) == ["customer_state_invalid"]


# ---------------------------------------------------------------- products

def test_products_get_english_category_with_fallbacks(spark):
    translation = spark.createDataFrame(
        [("beleza_saude", "health_beauty")],
        "product_category_name string, product_category_name_english string",
    )
    df = bronze(spark, PRODUCTS, [
        ("p1", "beleza_saude", "40", "287", "1", "225", "16", "10", "14"),
        ("p2", "pc_gamer", "40", "287", "1", "225", "16", "10", "14"),     # not in translation table
        ("p3", None, None, None, None, None, None, None, None),            # no category at all
    ])
    valid, rejected = run(df, "products", translation)
    english = {r["product_id"]: r["product_category_name_english"] for r in valid.collect()}
    assert english == {"p1": "health_beauty", "p2": "pc_gamer", "p3": "unknown"}
    assert rejected.count() == 0


def test_product_column_typos_are_fixed(spark):
    translation = spark.createDataFrame([], "product_category_name string, product_category_name_english string")
    df = bronze(spark, PRODUCTS, [("p1", "x", "40", "287", "1", "225", "16", "10", "14")])
    valid, _ = run(df, "products", translation)
    assert "product_name_length" in valid.columns and "product_description_length" in valid.columns
    assert "product_name_lenght" not in valid.columns


# ---------------------------------------------------------------- geolocation

def test_gps_points_outside_brazil_are_rejected(spark):
    df = bronze(spark, GEO, [
        ("01046", "-23.54", "-46.64", "sao paulo", "SP"),
        ("99999", "45.00", "-46.64", "paris", "XX"),       # latitude outside Brazil
        ("99998", "-23.54", "10.00", "berlin", "XX"),      # longitude outside Brazil
    ])
    valid, rejected = run(df, "geolocation")
    assert valid.count() == 1
    assert sorted(reasons(rejected)) == ["latitude_outside_brazil", "longitude_outside_brazil"]


def test_geolocation_is_aggregated_to_one_row_per_zip(spark):
    df = bronze(spark, GEO, [
        ("01046", "-23.00", "-46.00", "sao paulo", "SP"),
        ("01046", "-24.00", "-47.00", "sao paulo", "SP"),
        ("01046", "-25.00", "-48.00", "sampa", "SP"),
        ("02000", "-10.00", "-40.00", "salvador", "BA"),
    ])
    valid, _ = run(df, "geolocation")
    out = {r["zip_code_prefix"]: r for r in t.post_geolocation(valid).collect()}
    assert len(out) == 2
    assert out["01046"]["city"] == "sao paulo"            # most frequent city wins
    assert out["01046"]["gps_points"] == 3
    assert out["01046"]["lat"] == pytest.approx(-24.0)    # average of the points
    assert out["01046"]["lng"] == pytest.approx(-47.0)


# ---------------------------------------------------------------- deduplication

def test_deduplicate_keeps_the_latest_bronze_load(spark):
    old = bronze(spark, ORDERS, [("o1", "c1", "processing", "2017-10-02 10:56:33", None, None, None, None)],
                 batch_id="batch_1", ts="2026-01-01 00:00:00")
    new = bronze(spark, ORDERS, [("o1", "c1", "delivered", "2017-10-02 10:56:33", None, None, None, None)],
                 batch_id="batch_2", ts="2026-01-02 00:00:00")
    other = bronze(spark, ORDERS, [("o2", "c2", "shipped", "2017-10-03 10:00:00", None, None, None, None)],
                   batch_id="batch_1", ts="2026-01-01 00:00:00")
    typed = t.t_orders(old.unionByName(new).unionByName(other))
    deduped = t.deduplicate(typed, ["order_id"])
    status = {r["order_id"]: r["order_status"] for r in deduped.collect()}
    assert status == {"o1": "delivered", "o2": "shipped"}


def test_deduplicate_handles_composite_keys(spark):
    df = bronze(spark, ITEMS, [
        ("o1", "1", "p1", "s1", "2017-10-02 10:56:33", "10.00", "1.00"),
        ("o1", "1", "p1", "s1", "2017-10-02 10:56:33", "10.00", "1.00"),   # exact duplicate
        ("o1", "2", "p2", "s1", "2017-10-02 10:56:33", "20.00", "1.00"),   # different item
    ])
    deduped = t.deduplicate(t.t_order_items(df), ["order_id", "order_item_id"])
    assert deduped.count() == 2


# ---------------------------------------------------------------- the rule engine itself

def test_null_condition_counts_as_failure(spark):
    df = spark.createDataFrame([(1,), (None,)], "x int")
    valid, rejected = t.split_by_rules(df, {"x_not_positive": F.col("x") > 0})
    assert valid.count() == 1 and rejected.count() == 1


def test_every_table_has_rules_and_a_key_definition():
    assert set(t.RULES) == set(t.SPECS)
    for table, spec in t.SPECS.items():
        assert spec["kind"] in ("batch", "static"), table
        assert spec["keys"] or table == "geolocation", table
