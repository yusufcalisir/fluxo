-- clean_users: depends on raw_users
SELECT id, UPPER(name) AS clean_name, status FROM raw_users
