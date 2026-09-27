"""
============================================================================
E-COMMERCE CUSTOMER PROFITABILITY & RETENTION INTELLIGENCE
Step 1 of the pipeline: Python ETL

What this script does (matches section 9 of the project blueprint):
  1. Loads all 8 of your cleaned CSVs and profiles them.
  2. Resolves duplicates / missing values while preserving business meaning.
  3. Engineers customer-level RFM features (recency, frequency, monetary,
     RFM scores, customer_segment).
  4. Engineers order-level features (delivery_delay_days, freight_ratio,
     customer_order_number).
  5. Builds a monthly cohort retention table.
  6. Loads everything into the PostgreSQL "olist" schema created by
     ecom_02_schema.sql, AND writes the same tables to local CSVs so you can
     inspect them / use them even before Postgres is set up.

HOW TO RUN:
  1. pip install -r ecom_00_requirements.txt
  2. Run ecom_02_schema.sql against your Postgres database first
     (creates empty tables this script will fill).
  3. Edit the CONFIG block below: CSV_DIR and DB_* values.
  4. python ecom_01_etl_pipeline.py

If Postgres isn't set up yet, set LOAD_TO_DB = False below and this script
will still run the full cleaning + feature engineering and save everything
as CSVs in the OUTPUT_DIR, so you have something to work with immediately.
============================================================================
"""

import os
import sys
from datetime import timedelta

import numpy as np
import pandas as pd

# ============================================================================
# CONFIG - edit this block for your machine
# ============================================================================

# Folder containing your 8 cleaned CSVs (customers_clean.csv, etc.)
CSV_DIR = r"./data_set"

# Folder where engineered CSVs will be written (created if missing)
OUTPUT_DIR = r"./output"

# Set to False to skip the database entirely and just produce CSVs
LOAD_TO_DB = True

# Postgres connection - edit these to match your setup
DB_USER = os.environ.get("OLIST_DB_USER", "postgres")
DB_PASSWORD = os.environ.get("OLIST_DB_PASSWORD", "")
DB_HOST = os.environ.get("OLIST_DB_HOST", "localhost")
DB_PORT = os.environ.get("OLIST_DB_PORT", "5432")
DB_NAME = os.environ.get("OLIST_DB_NAME", "olist_ecommerce")

# File names as they appear in your screenshot (without .csv - adjust if
# your files use a different extension, e.g. .xlsx)
FILES = {
    "customers": "customers_clean.csv",
    "geolocation": "geolocation_clean.csv",
    "order_items": "order_items_clean.csv",
    "order_payments": "order_payments_clean.csv",
    "order_reviews": "order_reviews_clean.csv",
    "orders": "orders_clean.csv",
    "products": "products_clean.csv",
    "sellers": "sellers_clean.csv",
}

# RFM segment rules use 1-5 scores. Adjust thresholds here if you want a
# different segmentation logic.
RFM_BINS = 5

pd.set_option("display.width", 120)
pd.set_option("display.max_columns", 20)


# ============================================================================
# STEP 0 - helpers
# ============================================================================

def load_csv(name, filename):
    """Load one CSV, trying .csv then .xlsx, and report basic profile info."""
    path_csv = os.path.join(CSV_DIR, filename)
    path_xlsx = os.path.join(CSV_DIR, filename.replace(".csv", ".xlsx"))

    if os.path.exists(path_csv):
        df = pd.read_csv(path_csv)
    elif os.path.exists(path_xlsx):
        df = pd.read_excel(path_xlsx)
    else:
        raise FileNotFoundError(
            f"Could not find {path_csv} or {path_xlsx}. "
            f"Check CSV_DIR and the FILES dict at the top of this script."
        )

    print(f"\n--- {name} ---")
    print(f"shape: {df.shape}")
    print(f"columns: {list(df.columns)}")
    n_dupe = df.duplicated().sum()
    n_null = df.isna().sum().sum()
    print(f"duplicate rows: {n_dupe} | total null cells: {n_null}")
    return df


def to_datetime_safe(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df


def find_col(df, candidates):
    """Return the first column name in `candidates` that exists in df."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


# ============================================================================
# STEP 1 - LOAD + PROFILE
# ============================================================================

def load_all():
    data = {}
    for key, fname in FILES.items():
        data[key] = load_csv(key, fname)
    return data


# ============================================================================
# STEP 2 - CLEAN
# ============================================================================

def clean_data(data):
    customers = data["customers"].drop_duplicates()
    geolocation = data["geolocation"].drop_duplicates()
    order_items = data["order_items"].drop_duplicates()
    order_payments = data["order_payments"].drop_duplicates()
    order_reviews = data["order_reviews"]
    orders = data["orders"].drop_duplicates()
    products = data["products"].drop_duplicates()
    sellers = data["sellers"].drop_duplicates()

    # Parse dates
    orders = to_datetime_safe(
        orders,
        [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    )
    order_reviews = to_datetime_safe(
        order_reviews, ["review_creation_date", "review_answer_timestamp"]
    )
    order_items = to_datetime_safe(order_items, ["shipping_limit_date"])

    # Orders: purchase date is mandatory for any analysis - drop rows without it
    before = len(orders)
    orders = orders.dropna(subset=["order_purchase_timestamp"])
    print(f"\norders: dropped {before - len(orders)} rows with no purchase timestamp")

    # Order items: price/freight must be non-negative and present
    for col in ["price", "freight_value"]:
        if col in order_items.columns:
            order_items[col] = order_items[col].clip(lower=0)

    # Reviews: keep the most recent review per (order_id, review_id) if duplicated
    review_id_col = find_col(order_reviews, ["review_id"])
    if review_id_col:
        order_reviews = order_reviews.sort_values(
            "review_answer_timestamp"
        ).drop_duplicates(subset=[review_id_col], keep="last")

    # Payments: fill missing installments with 1 (single payment) rather than drop
    if "payment_installments" in order_payments.columns:
        order_payments["payment_installments"] = order_payments[
            "payment_installments"
        ].fillna(1)

    print("\nCleaning complete. Row counts:")
    for name, df in [
        ("customers", customers),
        ("geolocation", geolocation),
        ("order_items", order_items),
        ("order_payments", order_payments),
        ("order_reviews", order_reviews),
        ("orders", orders),
        ("products", products),
        ("sellers", sellers),
    ]:
        print(f"  {name}: {len(df):,} rows")

    return {
        "customers": customers,
        "geolocation": geolocation,
        "order_items": order_items,
        "order_payments": order_payments,
        "order_reviews": order_reviews,
        "orders": orders,
        "products": products,
        "sellers": sellers,
    }


# ============================================================================
# STEP 3 - ORDER-LEVEL FEATURES
# (delivery_delay_days, freight_ratio, customer_order_number)
# ============================================================================

def engineer_order_features(clean):
    orders = clean["orders"].copy()
    order_items = clean["order_items"].copy()
    customers = clean["customers"].copy()

    # delivery_delay_days: positive = delivered later than estimated
    orders["delivery_delay_days"] = (
        orders["order_delivered_customer_date"] - orders["order_estimated_delivery_date"]
    ).dt.days

    # Join customer_unique_id onto orders so we can compute order sequence
    # per REAL customer, not per order-level customer_id
    orders = orders.merge(
        customers[["customer_id", "customer_unique_id"]], on="customer_id", how="left"
    )
    orders = orders.sort_values(["customer_unique_id", "order_purchase_timestamp"])
    orders["customer_order_number"] = (
        orders.groupby("customer_unique_id").cumcount() + 1
    )

    # freight_ratio at the order-item level
    order_items["freight_ratio"] = order_items["freight_value"] / (
        order_items["price"] + order_items["freight_value"]
    ).replace(0, np.nan)

    return orders, order_items


# ============================================================================
# STEP 4 - RFM FEATURE ENGINEERING (customer_unique_id level)
# ============================================================================

def segment_from_scores(r, f, m):
    """Simple, explainable rule-based RFM segmentation (1=worst, 5=best)."""
    if r >= 4 and f >= 4 and m >= 4:
        return "Champions"
    if r >= 3 and f >= 3:
        return "Loyal Customers"
    if r >= 4 and f <= 2:
        return "Potential Loyalists"
    if r <= 2 and f >= 4 and m >= 4:
        return "Cannot Lose Them"
    if r <= 2 and f >= 3:
        return "At Risk"
    if r <= 2 and f <= 2 and m <= 2:
        return "Lost Customers"
    return "Others"


def build_customer_rfm(orders, order_payments):
    # Monetary value per order = sum of its payments (handles installments)
    order_value = order_payments.groupby("order_id")["payment_value"].sum().reset_index()
    order_value.columns = ["order_id", "order_value"]

    orders_val = orders.merge(order_value, on="order_id", how="left")
    orders_val["order_value"] = orders_val["order_value"].fillna(0)

    snapshot_date = orders_val["order_purchase_timestamp"].max() + timedelta(days=1)

    cust = (
        orders_val.groupby("customer_unique_id")
        .agg(
            first_order_date=("order_purchase_timestamp", "min"),
            last_order_date=("order_purchase_timestamp", "max"),
            frequency=("order_id", "nunique"),
            monetary_value=("order_value", "sum"),
        )
        .reset_index()
    )
    cust["recency_days"] = (snapshot_date - cust["last_order_date"]).dt.days

    # Quintile scores. duplicates="drop" handles ties on low-cardinality data;
    # rank(method="first") avoids errors from repeated bin edges.
    cust["r_score"] = pd.qcut(
        cust["recency_days"].rank(method="first", ascending=False), RFM_BINS, labels=False
    ) + 1
    cust["f_score"] = pd.qcut(
        cust["frequency"].rank(method="first"), RFM_BINS, labels=False
    ) + 1
    cust["m_score"] = pd.qcut(
        cust["monetary_value"].rank(method="first"), RFM_BINS, labels=False
    ) + 1

    cust["rfm_score"] = (
        cust["r_score"].astype(int).astype(str)
        + cust["f_score"].astype(int).astype(str)
        + cust["m_score"].astype(int).astype(str)
    )
    cust["customer_segment"] = cust.apply(
        lambda row: segment_from_scores(row["r_score"], row["f_score"], row["m_score"]),
        axis=1,
    )

    cust["first_order_date"] = cust["first_order_date"].dt.date
    cust["last_order_date"] = cust["last_order_date"].dt.date

    return cust


# ============================================================================
# STEP 5 - COHORT RETENTION TABLE
# ============================================================================

def build_cohort_retention(orders):
    df = orders[["customer_unique_id", "order_purchase_timestamp"]].copy()
    df["order_month"] = df["order_purchase_timestamp"].dt.to_period("M")
    cohort = df.groupby("customer_unique_id")["order_month"].min().rename("cohort_month")
    df = df.join(cohort, on="customer_unique_id")

    df["period_number"] = (
        (df["order_month"].dt.year - df["cohort_month"].dt.year) * 12
        + (df["order_month"].dt.month - df["cohort_month"].dt.month)
    )

    grid = (
        df.groupby(["cohort_month", "order_month", "period_number"])["customer_unique_id"]
        .nunique()
        .reset_index()
        .rename(columns={"customer_unique_id": "n_customers"})
    )
    grid["cohort_month"] = grid["cohort_month"].dt.to_timestamp().dt.date
    grid["order_month"] = grid["order_month"].dt.to_timestamp().dt.date
    return grid


# ============================================================================
# STEP 6 - DIM DATE
# ============================================================================

def build_dim_date(orders):
    start = orders["order_purchase_timestamp"].min().normalize()
    end = orders["order_purchase_timestamp"].max().normalize() + timedelta(days=31)
    dates = pd.date_range(start=start, end=end, freq="D")
    dim = pd.DataFrame({"full_date": dates})
    dim["date_sk"] = dim["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim["year"] = dim["full_date"].dt.year
    dim["quarter"] = dim["full_date"].dt.quarter
    dim["month"] = dim["full_date"].dt.month
    dim["month_name"] = dim["full_date"].dt.month_name()
    dim["day"] = dim["full_date"].dt.day
    dim["day_of_week"] = dim["full_date"].dt.dayofweek
    dim["day_name"] = dim["full_date"].dt.day_name()
    dim["is_weekend"] = dim["day_of_week"].isin([5, 6])
    return dim[
        [
            "date_sk",
            "full_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day",
            "day_of_week",
            "day_name",
            "is_weekend",
        ]
    ]


# ============================================================================
# STEP 7 - LOAD TO POSTGRES
# ============================================================================

def load_to_postgres(tables: dict):
    from sqlalchemy import create_engine
    from urllib.parse import quote_plus

    # quote_plus escapes special characters (like @, #, %) so a password
    # containing them doesn't break the connection string's syntax.
    safe_user = quote_plus(DB_USER)
    safe_password = quote_plus(DB_PASSWORD)
    conn_str = (
        f"postgresql+psycopg2://{safe_user}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    try:
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            pass
    except Exception as e:
        print(f"\n[WARN] Could not connect to Postgres ({e}).")
        print("       Skipping DB load. CSVs were still written to OUTPUT_DIR.")
        print("       Fix DB_* settings at the top of this script and re-run,")
        print("       or run ecom_02_schema.sql first if the tables don't exist yet.")
        return

    for table_name, df in tables.items():
        df.to_sql(
            table_name,
            engine,
            schema="olist",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )
        print(f"  loaded {len(df):,} rows -> olist.{table_name}")

    print("\nAll tables loaded into Postgres schema 'olist'.")


# ============================================================================
# MAIN
# ============================================================================

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("=" * 70)
    print("STEP 1: LOAD + PROFILE")
    print("=" * 70)
    raw = load_all()

    print("\n" + "=" * 70)
    print("STEP 2: CLEAN")
    print("=" * 70)
    clean = clean_data(raw)

    print("\n" + "=" * 70)
    print("STEP 3: ORDER-LEVEL FEATURES")
    print("=" * 70)
    orders, order_items = engineer_order_features(clean)
    print("delivery_delay_days computed for", orders["delivery_delay_days"].notna().sum(), "orders")
    print("customer_order_number computed for", len(orders), "orders")

    print("\n" + "=" * 70)
    print("STEP 4: CUSTOMER RFM")
    print("=" * 70)
    customer_rfm = build_customer_rfm(orders, clean["order_payments"])
    print(customer_rfm["customer_segment"].value_counts())

    print("\n" + "=" * 70)
    print("STEP 5: COHORT RETENTION")
    print("=" * 70)
    cohort = build_cohort_retention(orders)
    print(f"{len(cohort):,} cohort/order-month rows built")

    print("\n" + "=" * 70)
    print("STEP 6: DIM DATE")
    print("=" * 70)
    dim_date = build_dim_date(orders)
    print(f"{len(dim_date):,} calendar days built")

    # ---- prep dim_customer for output/load ----
    dim_customer = clean["customers"].rename(
        columns={"customer_zip_code_prefix": "customer_zip_prefix"}
    )[
        [c for c in [
            "customer_id", "customer_unique_id", "customer_zip_prefix",
            "customer_city", "customer_state",
        ] if c in clean["customers"].rename(
            columns={"customer_zip_code_prefix": "customer_zip_prefix"}
        ).columns]
    ]

    dim_seller = clean["sellers"].rename(
        columns={"seller_zip_code_prefix": "seller_zip_prefix"}
    )
    _dim_seller_cols = ["seller_id", "seller_zip_prefix", "seller_city", "seller_state"]
    dim_seller = dim_seller[[c for c in _dim_seller_cols if c in dim_seller.columns]]

    dim_geo = clean["geolocation"].rename(
        columns={
            "geolocation_zip_code_prefix": "zip_prefix",
            "geolocation_city": "city",
            "geolocation_state": "state",
            "geolocation_lat": "lat",
            "geolocation_lng": "lng",
        }
    ).drop_duplicates(subset=["zip_prefix"]) if "geolocation_zip_code_prefix" in clean["geolocation"].columns else clean["geolocation"]
    _dim_geo_cols = ["zip_prefix", "city", "state", "lat", "lng"]
    dim_geo = dim_geo[[c for c in _dim_geo_cols if c in dim_geo.columns]]

    # Only keep columns that exist in the dim_product table schema - the raw
    # products file may have extra columns (product_name_lenght,
    # product_description_lenght, product_category_name_english, etc.) that
    # aren't part of this project's schema, so they're dropped here rather
    # than causing an UndefinedColumn error on load.
    _dim_product_cols = [
        "product_id", "product_category_name", "product_weight_g",
        "product_length_cm", "product_height_cm", "product_width_cm",
        "product_photos_qty",
    ]
    dim_product = clean["products"][
        [c for c in _dim_product_cols if c in clean["products"].columns]
    ]

    fact_orders_out = orders[
        [c for c in [
            "order_id", "customer_id", "order_status", "order_purchase_timestamp",
            "order_approved_at", "order_delivered_carrier_date",
            "order_delivered_customer_date", "order_estimated_delivery_date",
            "delivery_delay_days", "customer_order_number",
        ] if c in orders.columns]
    ].copy()
    fact_orders_out["order_purchase_date_sk"] = (
        fact_orders_out["order_purchase_timestamp"].dt.strftime("%Y%m%d").astype(int)
    )

    _fact_order_items_cols = [
        "order_id", "order_item_id", "product_id", "seller_id",
        "shipping_limit_date", "price", "freight_value", "freight_ratio",
    ]
    fact_order_items_out = order_items[
        [c for c in _fact_order_items_cols if c in order_items.columns]
    ]

    _fact_payments_cols = [
        "order_id", "payment_sequential", "payment_type",
        "payment_installments", "payment_value",
    ]
    fact_payments_out = clean["order_payments"][
        [c for c in _fact_payments_cols if c in clean["order_payments"].columns]
    ]

    _fact_reviews_cols = [
        "review_id", "order_id", "review_score",
        "review_creation_date", "review_answer_timestamp",
    ]
    fact_reviews_out = clean["order_reviews"][
        [c for c in _fact_reviews_cols if c in clean["order_reviews"].columns]
    ]

    # ---- write everything to CSV (always happens, DB or not) ----
    print("\n" + "=" * 70)
    print("WRITING ENGINEERED CSVs")
    print("=" * 70)
    outputs = {
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_seller": dim_seller,
        "dim_geography": dim_geo,
        "dim_date": dim_date,
        "fact_orders": fact_orders_out,
        "fact_order_items": fact_order_items_out,
        "fact_payments": fact_payments_out,
        "fact_reviews": fact_reviews_out,
        "customer_rfm": customer_rfm,
        "cohort_retention": cohort,
    }
    for name, df in outputs.items():
        path = os.path.join(OUTPUT_DIR, f"{name}.csv")
        df.to_csv(path, index=False)
        print(f"  wrote {path}  ({len(df):,} rows)")

    if LOAD_TO_DB:
        print("\n" + "=" * 70)
        print("STEP 7: LOAD TO POSTGRES")
        print("=" * 70)
        # Load in FK-safe order: dimensions before facts
        load_order = [
            "dim_customer", "dim_product", "dim_seller", "dim_geography", "dim_date",
            "fact_orders", "fact_order_items", "fact_payments", "fact_reviews",
            "customer_rfm", "cohort_retention",
        ]
        load_to_postgres({k: outputs[k] for k in load_order})
    else:
        print("\nLOAD_TO_DB is False - skipped database load. CSVs are ready in", OUTPUT_DIR)

    print("\nDONE. Next: run ecom_03_analysis_queries.sql against the 'olist' schema.")


if __name__ == "__main__":
    main()
