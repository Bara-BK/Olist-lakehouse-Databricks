-- Dataset 1: orders  (one row per valid order, Jan 2017 - Aug 2018)
SELECT o.order_id,
       d.year_month,
       d.date AS order_date,
       c.state AS customer_state,
       c.customer_unique_id,
       o.order_status,
       o.items_revenue,
       o.freight_total,
       o.order_total,
       o.review_score,
       o.delivery_days,
       CASE WHEN o.is_late IS NULL THEN NULL WHEN o.is_late THEN 1 ELSE 0 END AS late_flag,
       o.main_payment_type
FROM workspace.gold.fact_orders o
JOIN workspace.gold.dim_date d ON o.purchase_date_key = d.date_key
JOIN workspace.gold.dim_customer c ON o.customer_id = c.customer_id
WHERE o.is_valid_sale
  AND d.year_month BETWEEN '2017-01' AND '2018-08';

-- Dataset 2: items  (one row per item sold)
SELECT i.order_id,
       i.seller_id,
       d.year_month,
       c.state AS customer_state,
       p.category_name,
       i.price,
       i.freight_value
FROM workspace.gold.fact_order_items i
JOIN workspace.gold.fact_orders o ON i.order_id = o.order_id
JOIN workspace.gold.dim_date d ON i.purchase_date_key = d.date_key
JOIN workspace.gold.dim_customer c ON i.customer_id = c.customer_id
JOIN workspace.gold.dim_product p ON i.product_id = p.product_id
WHERE o.is_valid_sale
  AND d.year_month BETWEEN '2017-01' AND '2018-08';

-- Dataset 3: sellers  (top 20 sellers by revenue, at least 50 items sold)
SELECT seller_id, seller_city, seller_state, items_sold, orders, revenue,
       avg_review_score, avg_delivery_days, late_rate_pct
FROM workspace.gold.agg_seller_performance
WHERE items_sold >= 50
ORDER BY revenue DESC
LIMIT 20;
