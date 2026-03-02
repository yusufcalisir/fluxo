-- daily_summary: aggregated revenue per day (depends on raw_orders)
SELECT
    order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT user_id) AS unique_customers,
    SUM(quantity) AS items_sold,
    ROUND(SUM(quantity * price), 2) AS total_revenue,
    ROUND(AVG(quantity * price), 2) AS avg_order_value
FROM raw_orders
GROUP BY order_date
ORDER BY order_date
