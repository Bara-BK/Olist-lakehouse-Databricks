import pathlib
import shutil
import pandas as pd

SRC = pathlib.Path("raw_olist")
OUT = pathlib.Path("upload")

# Static reference tables (copied unchanged)
STATIC = [
    "olist_customers_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "product_category_name_translation.csv",
]
# Tables that arrive over time, linked by order_id
TRANSACTIONAL = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
]

OUT.mkdir(exist_ok=True)

# Static
(OUT / "static").mkdir(exist_ok=True)
for f in STATIC:
    shutil.copy(SRC / f, OUT / "static" / f)

# Assign each order to a batch by purchase date
orders = pd.read_csv(SRC / "olist_orders_dataset.csv", parse_dates=["order_purchase_timestamp"])
orders = orders.sort_values("order_purchase_timestamp")
orders["batch"] = pd.qcut(
    orders["order_purchase_timestamp"].rank(method="first"),
    3,
    labels=["batch_1", "batch_2", "batch_3"],
)
mapping = orders[["order_id", "batch"]]

for f in TRANSACTIONAL:
    df = pd.read_csv(SRC / f)
    df = df.merge(mapping, on="order_id", how="left")
    for batch, part in df.groupby("batch", observed=True):
        folder = OUT / str(batch)
        folder.mkdir(exist_ok=True)
        part.drop(columns="batch").to_csv(folder / f, index=False)
    print(f, "done")

print("Finished. Upload the 'upload' folder contents to Databricks.")