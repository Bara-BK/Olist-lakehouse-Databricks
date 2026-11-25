# Databricks notebook source
# MAGIC %md
# MAGIC # dq_expectations - shared Great Expectations suites for Silver
# MAGIC Pure code (no table reads or writes): the expectation suites for every Silver table and a function that runs them.
# MAGIC
# MAGIC - Loaded by `02b_validate_silver_gx` with `%run ./dq_expectations`
# MAGIC - Imported by the pytest tests in `tests/` (this file is also valid plain Python)
# MAGIC
# MAGIC **Severity:** every expectation is `critical` (stops the Job) unless `meta={"severity": "warning"}` (logged only).

# COMMAND ----------

import great_expectations as gx
import great_expectations.expectations as gxe

CRITICAL = "critical"
WARNING = "warning"

ORDER_STATUSES = ["delivered", "shipped", "canceled", "unavailable",
                  "invoiced", "processing", "created", "approved"]
PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card", "not_defined"]

# COMMAND ----------

def _w(exp):
    """Mark an expectation as warning-only (logged, does not stop the Job)."""
    exp.meta = {**(exp.meta or {}), "severity": WARNING}
    return exp


def not_null(col, mostly=1.0):
    return gxe.ExpectColumnValuesToNotBeNull(column=col, mostly=mostly)


def unique(col):
    return gxe.ExpectColumnValuesToBeUnique(column=col)


def build_expectations():
    """Return {silver_table: [expectation, ...]}."""
    return {
        "orders": [
            gxe.ExpectTableRowCountToBeBetween(min_value=90_000),
            not_null("order_id"), unique("order_id"),
            not_null("customer_id"),
            not_null("order_purchase_timestamp"),
            gxe.ExpectColumnValuesToBeInSet(column="order_status", value_set=ORDER_STATUSES),
        ],
        "order_items": [
            gxe.ExpectTableRowCountToBeBetween(min_value=100_000),
            not_null("order_id"), not_null("order_item_id"),
            not_null("product_id"), not_null("seller_id"),
            gxe.ExpectCompoundColumnsToBeUnique(column_list=["order_id", "order_item_id"]),
            gxe.ExpectColumnValuesToBeBetween(column="price", min_value=0),
            gxe.ExpectColumnValuesToBeBetween(column="freight_value", min_value=0),
            # a price above 10,000 R$ is suspicious but not impossible
            _w(gxe.ExpectColumnValuesToBeBetween(column="price", max_value=10_000)),
        ],
        "order_payments": [
            not_null("order_id"),
            gxe.ExpectCompoundColumnsToBeUnique(column_list=["order_id", "payment_sequential"]),
            gxe.ExpectColumnValuesToBeInSet(column="payment_type", value_set=PAYMENT_TYPES),
            gxe.ExpectColumnValuesToBeBetween(column="payment_value", min_value=0),
            gxe.ExpectColumnValuesToBeBetween(column="payment_installments", min_value=0),
        ],
        "order_reviews": [
            not_null("review_id"), not_null("order_id"),
            gxe.ExpectColumnValuesToBeBetween(column="review_score", min_value=1, max_value=5),
            gxe.ExpectColumnValuesToBeInSet(column="review_score", value_set=[1, 2, 3, 4, 5]),
        ],
        "customers": [
            gxe.ExpectTableRowCountToBeBetween(min_value=90_000),
            not_null("customer_id"), unique("customer_id"),
            not_null("customer_unique_id"),
            gxe.ExpectColumnValueLengthsToEqual(column="customer_state", value=2),
        ],
        "sellers": [
            not_null("seller_id"), unique("seller_id"),
            gxe.ExpectColumnValueLengthsToEqual(column="seller_state", value=2),
        ],
        "products": [
            not_null("product_id"), unique("product_id"),
            not_null("product_category_name_english"),
            _w(gxe.ExpectColumnValuesToBeBetween(column="product_weight_g", min_value=0, mostly=0.999)),
        ],
        "geolocation": [
            not_null("zip_code_prefix"), unique("zip_code_prefix"),
            gxe.ExpectColumnValuesToBeBetween(column="lat", min_value=-35.0, max_value=6.0),
            gxe.ExpectColumnValuesToBeBetween(column="lng", min_value=-75.0, max_value=-34.0),
        ],
    }

# COMMAND ----------

def _severity(exp):
    return (exp.meta or {}).get("severity", CRITICAL)


def _batch_from(df, table, use_pandas):
    """Return a GX batch for a Spark DataFrame (or a pandas DataFrame when use_pandas=True)."""
    context = gx.get_context(mode="ephemeral")
    if use_pandas:
        source = context.data_sources.add_pandas(name=f"pd_{table}")
        asset = source.add_dataframe_asset(name=table)
    else:
        source = context.data_sources.add_spark(name=f"spark_{table}")
        asset = source.add_dataframe_asset(name=table)
    batch_def = asset.add_batch_definition_whole_dataframe("whole")
    return context, batch_def.get_batch(batch_parameters={"dataframe": df})


def validate_table(df, table, expectations, use_pandas=False):
    """Run every expectation on one DataFrame. Returns a list of plain result dicts (one per expectation)."""
    _, batch = _batch_from(df, table, use_pandas)
    rows = []
    for exp in expectations:
        res = batch.validate(exp)
        r = res.result or {}
        kwargs = exp.configuration.kwargs
        rows.append({
            "table_name": table,
            "expectation": exp.expectation_type,
            "column_name": kwargs.get("column") or ",".join(kwargs.get("column_list", [])) or None,
            "severity": _severity(exp),
            "success": bool(res.success),
            "observed_value": str(r.get("observed_value")) if r.get("observed_value") is not None else None,
            "unexpected_count": r.get("unexpected_count"),
            "unexpected_percent": r.get("unexpected_percent"),
            "element_count": r.get("element_count"),
        })
    return rows
