-- ============================================================================
-- E-COMMERCE CUSTOMER PROFITABILITY & RETENTION INTELLIGENCE
-- Step 2 of the pipeline: database schema (star schema)
-- Engine: PostgreSQL
--
-- HOW TO RUN:
--   psql -U <your_user> -d <your_database> -f ecom_02_schema.sql
-- or open this file in pgAdmin / DBeaver and execute it.
--
-- This creates a dedicated schema called "olist" so it never collides with
-- anything else in your database. All tables in this project live there.
-- ============================================================================

DROP SCHEMA IF EXISTS olist CASCADE;
CREATE SCHEMA olist;
SET search_path TO olist;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- One row per Olist customer_id (an order-level id). customer_unique_id is
-- the TRUE customer identity and is what all retention analysis must key on.
CREATE TABLE dim_customer (
    customer_sk         SERIAL PRIMARY KEY,
    customer_id         VARCHAR(64) UNIQUE NOT NULL,
    customer_unique_id  VARCHAR(64) NOT NULL,
    customer_zip_prefix VARCHAR(10),
    customer_city       VARCHAR(100),
    customer_state      VARCHAR(2)
);
CREATE INDEX idx_dim_customer_unique_id ON dim_customer (customer_unique_id);

CREATE TABLE dim_product (
    product_sk             SERIAL PRIMARY KEY,
    product_id             VARCHAR(64) UNIQUE NOT NULL,
    product_category_name  VARCHAR(100),
    product_weight_g       NUMERIC,
    product_length_cm      NUMERIC,
    product_height_cm      NUMERIC,
    product_width_cm       NUMERIC,
    product_photos_qty     INT
);

CREATE TABLE dim_seller (
    seller_sk         SERIAL PRIMARY KEY,
    seller_id         VARCHAR(64) UNIQUE NOT NULL,
    seller_zip_prefix VARCHAR(10),
    seller_city        VARCHAR(100),
    seller_state       VARCHAR(2)
);

CREATE TABLE dim_geography (
    geo_sk     SERIAL PRIMARY KEY,
    zip_prefix VARCHAR(10) NOT NULL,
    city       VARCHAR(100),
    state      VARCHAR(2),
    lat        NUMERIC,
    lng        NUMERIC
);

CREATE TABLE dim_date (
    date_sk     INT PRIMARY KEY,        -- YYYYMMDD, e.g. 20170815
    full_date   DATE NOT NULL UNIQUE,
    year        INT,
    quarter     INT,
    month       INT,
    month_name  VARCHAR(10),
    day         INT,
    day_of_week INT,                    -- 0=Monday ... 6=Sunday
    day_name    VARCHAR(10),
    is_weekend  BOOLEAN
);

-- ============================================================================
-- FACT TABLES
-- ============================================================================

CREATE TABLE fact_orders (
    order_sk                       SERIAL PRIMARY KEY,
    order_id                       VARCHAR(64) UNIQUE NOT NULL,
    customer_id                    VARCHAR(64) REFERENCES dim_customer (customer_id),
    order_status                   VARCHAR(30),
    order_purchase_date_sk         INT REFERENCES dim_date (date_sk),
    order_purchase_timestamp       TIMESTAMP,
    order_approved_at              TIMESTAMP,
    order_delivered_carrier_date   TIMESTAMP,
    order_delivered_customer_date  TIMESTAMP,
    order_estimated_delivery_date  TIMESTAMP,
    delivery_delay_days            NUMERIC,   -- delivered_customer_date - estimated_delivery_date (>0 = late)
    customer_order_number          INT        -- 1st, 2nd, 3rd... order for that customer_unique_id
);
CREATE INDEX idx_fact_orders_customer ON fact_orders (customer_id);
CREATE INDEX idx_fact_orders_purchase_date ON fact_orders (order_purchase_date_sk);

CREATE TABLE fact_order_items (
    order_item_sk       SERIAL PRIMARY KEY,
    order_id             VARCHAR(64) REFERENCES fact_orders (order_id),
    order_item_id        INT,
    product_id            VARCHAR(64) REFERENCES dim_product (product_id),
    seller_id             VARCHAR(64) REFERENCES dim_seller (seller_id),
    shipping_limit_date   TIMESTAMP,
    price                 NUMERIC,
    freight_value         NUMERIC,
    freight_ratio         NUMERIC   -- freight_value / (price + freight_value)
);
CREATE INDEX idx_fact_order_items_order ON fact_order_items (order_id);
CREATE INDEX idx_fact_order_items_product ON fact_order_items (product_id);
CREATE INDEX idx_fact_order_items_seller ON fact_order_items (seller_id);

CREATE TABLE fact_payments (
    payment_sk           SERIAL PRIMARY KEY,
    order_id              VARCHAR(64) REFERENCES fact_orders (order_id),
    payment_sequential    INT,
    payment_type          VARCHAR(30),
    payment_installments  INT,
    payment_value         NUMERIC
);
CREATE INDEX idx_fact_payments_order ON fact_payments (order_id);

CREATE TABLE fact_reviews (
    review_sk                SERIAL PRIMARY KEY,
    review_id                 VARCHAR(64),
    order_id                  VARCHAR(64) REFERENCES fact_orders (order_id),
    review_score              INT,
    review_creation_date      TIMESTAMP,
    review_answer_timestamp   TIMESTAMP
);
CREATE INDEX idx_fact_reviews_order ON fact_reviews (order_id);

-- ============================================================================
-- ANALYTICS TABLES built by the Python ETL (02_etl_pipeline.py writes these)
-- ============================================================================

-- One row per unique human being (customer_unique_id), with RFM + segment.
CREATE TABLE customer_rfm (
    customer_unique_id  VARCHAR(64) PRIMARY KEY,
    first_order_date     DATE,
    last_order_date      DATE,
    recency_days          INT,
    frequency              INT,
    monetary_value          NUMERIC,
    r_score                 INT,
    f_score                 INT,
    m_score                 INT,
    rfm_score                VARCHAR(3),   -- e.g. '514'
    customer_segment         VARCHAR(30)
);

-- Monthly cohort retention grid (first-purchase month x months-since-cohort).
CREATE TABLE cohort_retention (
    cohort_month    DATE,
    order_month     DATE,
    period_number   INT,     -- months since first purchase (0,1,2,...)
    n_customers     INT
);

SET search_path TO public;
