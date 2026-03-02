-- raw_orders: simulated e-commerce order data
SELECT 1001 AS order_id, 1 AS user_id, 'laptop' AS product, 2 AS quantity, 999.99 AS price, '2026-03-01' AS order_date
UNION ALL
SELECT 1002, 2, 'keyboard', 1, 49.99, '2026-03-01'
UNION ALL
SELECT 1003, 1, 'monitor', 1, 399.00, '2026-03-01'
UNION ALL
SELECT 1004, 3, 'mouse', 3, 19.99, '2026-03-02'
UNION ALL
SELECT 1005, 2, 'laptop', 1, 999.99, '2026-03-02'
UNION ALL
SELECT 1006, 1, 'headphones', 2, 79.99, '2026-03-02'
UNION ALL
SELECT 1007, 3, 'keyboard', 2, 49.99, '2026-03-02'
UNION ALL
SELECT 1008, 2, 'monitor', 1, 399.00, '2026-03-01'
UNION ALL
SELECT 1009, 1, 'mouse', 5, 19.99, '2026-03-01'
UNION ALL
SELECT 1010, 3, 'laptop', 1, 999.99, '2026-03-02'
