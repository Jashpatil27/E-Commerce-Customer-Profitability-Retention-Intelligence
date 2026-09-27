-- ============================================================================
-- E-COMMERCE CUSTOMER PROFITABILITY & RETENTION INTELLIGENCE
-- Step 3 of the pipeline: analysis queries (run after schema + ETL load)
-- Engine: PostgreSQL
--
-- HOW TO RUN:
--   psql -U <user> -d <db> -f ecom_03_analysis_queries.sql
-- or run each query individually in pgAdmin / DBeaver to inspect results.
--
-- Since you're learning SQL: each query has a short comment on WHAT it
-- answers and WHICH SQL technique it teaches (window functions, CTEs,
-- self-joins, etc). Run them one at a time and look at the output.
-- ============================================================================

SET search_path TO olist;

-- ============================================================================
-- 1) Monthly GMV / revenue trend
-- Technique: DATE_TRUNC + GROUP BY
-- ============================================================================
SELECT
    DATE_TRUNC('month', order_purchase_timestamp)::date AS order_month,
    ROUND(SUM(oi.price + oi.freight_value)::numeric, 2) AS gmv,
    COUNT(DISTINCT o.order_id) AS orders
FROM fact_orders o
JOIN fact_order_items oi ON oi.order_id = o.order_id
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY 1
ORDER BY 1;

-- ============================================================================
-- 2) Customer count and repeat-customer rate
-- Technique: CTE + CASE WHEN aggregation
-- ============================================================================
WITH customer_orders AS (
    SELECT customer_unique_id, frequency
    FROM customer_rfm
)
SELECT
    COUNT(*) AS total_customers,
    COUNT(*) FILTER (WHERE frequency > 1) AS repeat_customers,
    ROUND(100.0 * COUNT(*) FILTER (WHERE frequency > 1) / NULLIF(COUNT(*), 0), 2) AS repeat_rate_pct
FROM customer_orders;

-- ============================================================================
-- 3) Average order value (AOV)
-- Technique: aggregate with SUM/COUNT DISTINCT
-- ============================================================================
SELECT
    ROUND(SUM(p.payment_value)::numeric / NULLIF(COUNT(DISTINCT o.order_id), 0), 2) AS avg_order_value
FROM fact_orders o
JOIN fact_payments p ON p.order_id = o.order_id
WHERE o.order_status NOT IN ('canceled', 'unavailable');

-- ============================================================================
-- 4) Orders per customer (distribution)
-- Technique: GROUP BY + GROUP BY on the aggregate (histogram)
-- ============================================================================
SELECT
    frequency AS orders_per_customer,
    COUNT(*) AS n_customers
FROM customer_rfm
GROUP BY frequency
ORDER BY frequency;

-- ============================================================================
-- 5) Days between first and second purchase
-- Technique: window function LEAD() over customer, ordered by purchase date
-- ============================================================================
WITH ordered AS (
    SELECT
        customer_id,
        order_purchase_timestamp,
        customer_order_number,
        LEAD(order_purchase_timestamp) OVER (
            PARTITION BY customer_id ORDER BY order_purchase_timestamp
        ) AS next_order_ts
    FROM fact_orders
)
SELECT
    ROUND(AVG(EXTRACT(DAY FROM (next_order_ts - order_purchase_timestamp)))::numeric, 1)
        AS avg_days_first_to_second_purchase
FROM ordered
WHERE customer_order_number = 1 AND next_order_ts IS NOT NULL;

-- ============================================================================
-- 6) Customer lifetime revenue (top-down view of monetary_value)
-- Technique: simple SELECT off the pre-built customer_rfm table
-- ============================================================================
SELECT customer_unique_id, monetary_value, frequency
FROM customer_rfm
ORDER BY monetary_value DESC
LIMIT 100;

-- ============================================================================
-- 7) Top customers by monetary value (with segment)
-- Technique: RANK() window function
-- ============================================================================
SELECT
    customer_unique_id,
    monetary_value,
    customer_segment,
    RANK() OVER (ORDER BY monetary_value DESC) AS value_rank
FROM customer_rfm
ORDER BY value_rank
LIMIT 20;

-- ============================================================================
-- 8) RFM score calculation (recomputed in SQL to validate the Python version)
-- Technique: NTILE() window function - this is how you'd do RFM scoring
-- in pure SQL if you didn't have Python.
-- ============================================================================
SELECT
    customer_unique_id,
    recency_days,
    frequency,
    monetary_value,
    NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score_sql,
    NTILE(5) OVER (ORDER BY frequency ASC) AS f_score_sql,
    NTILE(5) OVER (ORDER BY monetary_value ASC) AS m_score_sql
FROM customer_rfm
ORDER BY customer_unique_id
LIMIT 50;
-- Compare r_score_sql/f_score_sql/m_score_sql above against r_score/f_score/
-- m_score already stored in customer_rfm (computed in pandas). They should
-- be close; small differences are normal because qcut and NTILE break ties
-- slightly differently.

-- ============================================================================
-- 9) RFM segment revenue
-- Technique: GROUP BY on a categorical column + percentage of total
-- ============================================================================
SELECT
    customer_segment,
    COUNT(*) AS n_customers,
    ROUND(SUM(monetary_value)::numeric, 2) AS segment_revenue,
    ROUND(100.0 * SUM(monetary_value) / NULLIF(SUM(SUM(monetary_value)) OVER (), 0), 2) AS pct_of_total_revenue
FROM customer_rfm
GROUP BY customer_segment
ORDER BY segment_revenue DESC;

-- ============================================================================
-- 10) At-risk customer ranking
-- Technique: filtering + ORDER BY on multiple columns
-- ============================================================================
SELECT
    customer_unique_id,
    recency_days,
    frequency,
    monetary_value,
    rfm_score
FROM customer_rfm
WHERE customer_segment IN ('At Risk', 'Cannot Lose Them')
ORDER BY monetary_value DESC, recency_days DESC
LIMIT 100;

-- ============================================================================
-- 11) Category repeat-purchase rate
-- Technique: subquery to flag repeat customers, joined back to category
-- ============================================================================
WITH order_category AS (
    SELECT DISTINCT
        o.order_id,
        o.customer_id,
        p.product_category_name
    FROM fact_orders o
    JOIN fact_order_items oi ON oi.order_id = o.order_id
    JOIN dim_product p ON p.product_id = oi.product_id
),
customer_category_orders AS (
    SELECT
        product_category_name,
        customer_id,
        COUNT(DISTINCT order_id) AS orders_in_category
    FROM order_category
    GROUP BY product_category_name, customer_id
)
SELECT
    product_category_name,
    COUNT(*) AS customers,
    COUNT(*) FILTER (WHERE orders_in_category > 1) AS repeat_customers,
    ROUND(100.0 * COUNT(*) FILTER (WHERE orders_in_category > 1) / NULLIF(COUNT(*), 0), 2) AS repeat_rate_pct
FROM customer_category_orders
GROUP BY product_category_name
HAVING COUNT(*) >= 30   -- ignore tiny categories with noisy rates
ORDER BY repeat_rate_pct DESC;

-- ============================================================================
-- 12) Seller revenue ranking
-- Technique: JOIN + GROUP BY + RANK()
-- ============================================================================
SELECT
    s.seller_id,
    s.seller_state,
    ROUND(SUM(oi.price)::numeric, 2) AS seller_revenue,
    RANK() OVER (ORDER BY SUM(oi.price) DESC) AS revenue_rank
FROM fact_order_items oi
JOIN dim_seller s ON s.seller_id = oi.seller_id
GROUP BY s.seller_id, s.seller_state
ORDER BY seller_revenue DESC
LIMIT 50;

-- ============================================================================
-- 13) Seller late-delivery rate
-- Technique: JOIN order_items to orders to get delay, aggregate by seller
-- ============================================================================
SELECT
    s.seller_id,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.order_id) FILTER (WHERE o.delivery_delay_days > 0) AS late_orders,
    ROUND(
        100.0 * COUNT(DISTINCT o.order_id) FILTER (WHERE o.delivery_delay_days > 0)
        / NULLIF(COUNT(DISTINCT o.order_id), 0), 2
    ) AS late_rate_pct
FROM fact_order_items oi
JOIN dim_seller s ON s.seller_id = oi.seller_id
JOIN fact_orders o ON o.order_id = oi.order_id
WHERE o.order_delivered_customer_date IS NOT NULL
GROUP BY s.seller_id
HAVING COUNT(DISTINCT o.order_id) >= 10
ORDER BY late_rate_pct DESC
LIMIT 50;

-- ============================================================================
-- 14) Freight as percentage of order value
-- Technique: derived ratio column, already engineered in Python (freight_ratio)
-- but recomputed here to show the raw SQL expression
-- ============================================================================
SELECT
    order_id,
    price,
    freight_value,
    ROUND(100.0 * freight_value / NULLIF(price + freight_value, 0), 2) AS freight_pct_of_order
FROM fact_order_items
ORDER BY freight_pct_of_order DESC
LIMIT 100;

-- ============================================================================
-- 15) Delivery delay by region (customer state)
-- Technique: JOIN across fact_orders -> dim_customer, GROUP BY state
-- ============================================================================
SELECT
    c.customer_state,
    ROUND(AVG(o.delivery_delay_days)::numeric, 2) AS avg_delay_days,
    COUNT(*) FILTER (WHERE o.delivery_delay_days > 0) AS late_orders,
    COUNT(*) AS total_orders
FROM fact_orders o
JOIN dim_customer c ON c.customer_id = o.customer_id
WHERE o.order_delivered_customer_date IS NOT NULL
GROUP BY c.customer_state
ORDER BY avg_delay_days DESC;

-- ============================================================================
-- 16) Review score by delivery-delay bucket
-- Technique: CASE WHEN to bucket a continuous variable, then GROUP BY it
-- ============================================================================
SELECT
    CASE
        WHEN o.delivery_delay_days IS NULL THEN 'not delivered'
        WHEN o.delivery_delay_days <= 0 THEN 'on time / early'
        WHEN o.delivery_delay_days BETWEEN 1 AND 3 THEN 'late 1-3 days'
        WHEN o.delivery_delay_days BETWEEN 4 AND 7 THEN 'late 4-7 days'
        ELSE 'late 8+ days'
    END AS delay_bucket,
    ROUND(AVG(r.review_score)::numeric, 2) AS avg_review_score,
    COUNT(*) AS n_orders
FROM fact_orders o
JOIN fact_reviews r ON r.order_id = o.order_id
GROUP BY delay_bucket
ORDER BY avg_review_score;

-- ============================================================================
-- 17) Revenue associated with delayed orders
-- Technique: CASE WHEN inside SUM (conditional aggregation)
-- ============================================================================
SELECT
    SUM(CASE WHEN o.delivery_delay_days > 0 THEN p.payment_value ELSE 0 END) AS revenue_from_late_orders,
    SUM(p.payment_value) AS total_revenue,
    ROUND(
        100.0 * SUM(CASE WHEN o.delivery_delay_days > 0 THEN p.payment_value ELSE 0 END)
        / NULLIF(SUM(p.payment_value), 0), 2
    ) AS pct_revenue_from_late_orders
FROM fact_orders o
JOIN fact_payments p ON p.order_id = o.order_id;

-- ============================================================================
-- 18) Cohort retention by first-purchase month
-- Technique: pivot-style read of the pre-built cohort_retention table
-- ============================================================================
SELECT
    cohort_month,
    period_number,
    n_customers,
    FIRST_VALUE(n_customers) OVER (
        PARTITION BY cohort_month ORDER BY period_number
    ) AS cohort_size,
    ROUND(
        100.0 * n_customers / NULLIF(
            FIRST_VALUE(n_customers) OVER (PARTITION BY cohort_month ORDER BY period_number), 0
        ), 2
    ) AS retention_pct
FROM cohort_retention
ORDER BY cohort_month, period_number;

-- ============================================================================
-- 19) Month-over-month revenue change
-- Technique: LAG() window function to compare a row to the previous month
-- ============================================================================
WITH monthly AS (
    SELECT
        DATE_TRUNC('month', o.order_purchase_timestamp)::date AS order_month,
        SUM(p.payment_value) AS revenue
    FROM fact_orders o
    JOIN fact_payments p ON p.order_id = o.order_id
    GROUP BY 1
)
SELECT
    order_month,
    revenue,
    LAG(revenue) OVER (ORDER BY order_month) AS prev_month_revenue,
    ROUND(
        100.0 * (revenue - LAG(revenue) OVER (ORDER BY order_month))
        / NULLIF(LAG(revenue) OVER (ORDER BY order_month), 0), 2
    ) AS mom_pct_change
FROM monthly
ORDER BY order_month;

-- ============================================================================
-- 20) Customer and seller drill-down exception tables
-- Technique: two separate "exception" queries analysts ship alongside a
-- dashboard so stakeholders can see WHO/WHAT is driving a KPI.
-- ============================================================================

-- 20a) Customers who are high value AND at risk of churning (priority save list)
SELECT
    customer_unique_id,
    monetary_value,
    recency_days,
    frequency,
    customer_segment
FROM customer_rfm
WHERE customer_segment IN ('At Risk', 'Cannot Lose Them')
  AND monetary_value > (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY monetary_value) FROM customer_rfm)
ORDER BY monetary_value DESC;

-- 20b) Sellers with both high delay rate AND low review scores (priority fix list)
WITH seller_delay AS (
    SELECT
        oi.seller_id,
        COUNT(DISTINCT o.order_id) AS total_orders,
        ROUND(
            100.0 * COUNT(DISTINCT o.order_id) FILTER (WHERE o.delivery_delay_days > 0)
            / NULLIF(COUNT(DISTINCT o.order_id), 0), 2
        ) AS late_rate_pct
    FROM fact_order_items oi
    JOIN fact_orders o ON o.order_id = oi.order_id
    WHERE o.order_delivered_customer_date IS NOT NULL
    GROUP BY oi.seller_id
    HAVING COUNT(DISTINCT o.order_id) >= 10
),
seller_reviews AS (
    SELECT
        oi.seller_id,
        ROUND(AVG(r.review_score)::numeric, 2) AS avg_review_score
    FROM fact_order_items oi
    JOIN fact_reviews r ON r.order_id = oi.order_id
    GROUP BY oi.seller_id
)
SELECT
    d.seller_id,
    d.total_orders,
    d.late_rate_pct,
    rv.avg_review_score
FROM seller_delay d
JOIN seller_reviews rv ON rv.seller_id = d.seller_id
WHERE d.late_rate_pct > 20 AND rv.avg_review_score < 3.5
ORDER BY d.late_rate_pct DESC, rv.avg_review_score ASC;

SET search_path TO public;
