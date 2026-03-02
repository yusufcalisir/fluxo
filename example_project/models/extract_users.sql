-- raw_users: seed data
SELECT 1 AS id, 'Alice' AS name, 'active' AS status
UNION ALL
SELECT 2, 'Bob', 'active'
UNION ALL
SELECT 3, 'Charlie', 'inactive'
