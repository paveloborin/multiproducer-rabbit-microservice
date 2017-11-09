SELECT
  u.id                   AS id,
  UNIX_TIMESTAMP(u.date) AS timestamp
FROM users u
WHERE u.id > 0