"""Tests for the Great Expectations suites: good data passes, each kind of bad data is caught.

Runs on pandas (fast, no Spark needed for the checks themselves).
"""
import pathlib
import sys

import pandas as pd
import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "notebooks"))
import dq_expectations as dq  # noqa: E402


def run(table, df, only=None):
    exps = dq.build_expectations()[table]
    # the row-count floors are for the real data, skip them on tiny synthetic frames
    exps = [e for e in exps if e.expectation_type != "expect_table_row_count_to_be_between"]
    if only:
        exps = [e for e in exps if getattr(e, "column", None) == only]
    return dq.validate_table(df, table, exps, use_pandas=True)


def failed(rows):
    return {(r["column_name"], r["expectation"]) for r in rows if not r["success"]}


def good_orders():
    return pd.DataFrame({
        "order_id": ["o1", "o2"], "customer_id": ["c1", "c2"],
        "order_status": ["delivered", "shipped"],
        "order_purchase_timestamp": pd.to_datetime(["2018-01-01", "2018-01-02"]),
    })


def test_good_orders_pass():
    assert failed(run("orders", good_orders())) == set()


def test_duplicate_order_id_is_caught():
    df = good_orders()
    df.loc[1, "order_id"] = "o1"
    assert ("order_id", "expect_column_values_to_be_unique") in failed(run("orders", df))


def test_null_customer_is_caught():
    df = good_orders()
    df.loc[0, "customer_id"] = None
    assert ("customer_id", "expect_column_values_to_not_be_null") in failed(run("orders", df))


def test_unknown_status_is_caught():
    df = good_orders()
    df.loc[0, "order_status"] = "teleported"
    assert ("order_status", "expect_column_values_to_be_in_set") in failed(run("orders", df))


def test_negative_price_is_caught():
    df = pd.DataFrame({
        "order_id": ["o1"], "order_item_id": [1], "product_id": ["p"], "seller_id": ["s"],
        "price": [-5.0], "freight_value": [1.0],
    })
    assert ("price", "expect_column_values_to_be_between") in failed(run("order_items", df))


def test_review_score_out_of_range_is_caught():
    df = pd.DataFrame({"review_id": ["r1", "r2"], "order_id": ["o1", "o2"], "review_score": [5, 9]})
    assert ("review_score", "expect_column_values_to_be_between") in failed(run("order_reviews", df))


def test_gps_outside_brazil_is_caught():
    df = pd.DataFrame({"zip_code_prefix": ["01001", "01002"], "lat": [-23.5, 48.8], "lng": [-46.6, 2.3]})
    assert ("lat", "expect_column_values_to_be_between") in failed(run("geolocation", df))


def test_warning_expectations_are_marked():
    sev = {e.expectation_type: dq._severity(e) for e in dq.build_expectations()["order_items"]}
    assert dq.WARNING in sev.values() and dq.CRITICAL in sev.values()
