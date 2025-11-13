-- computes cumulative spend per customer, but ignores store/region context
SELECT
  customer_id,
  purchase_ts,
  amount,
  SUM(amount) OVER (
    PARTITION BY customer_id
    ORDER BY purchase_ts
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_spend
FROM purchases;

SELECT
  customer_id,
  COALESCE(store_id::text, 'UNKNOWN') AS store_id,
  COALESCE(region::text, 'UNKNOWN')   AS region,
  purchase_ts,
  amount,
  SUM(amount) OVER (
    PARTITION BY customer_id, COALESCE(store_id, 'UNKNOWN'), COALESCE(region, 'UNKNOWN')
    ORDER BY purchase_ts
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_spend_by_context
FROM purchases;

WITH agg AS (
  SELECT customer_id, store_id, region, purchase_date, SUM(amount) AS day_amount
  FROM purchases
  GROUP BY 1,2,3,4
)
SELECT
  customer_id, store_id, region, purchase_date, day_amount,
  SUM(day_amount) OVER (
    PARTITION BY customer_id, store_id, region
    ORDER BY purchase_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_by_day
FROM agg;


SELECT
  customer_id, purchase_ts, amount,
  SUM(amount) OVER (
    PARTITION BY customer_id
    ORDER BY purchase_ts
    ROWS BETWEEN 100 PRECEDING AND CURRENT ROW  -- arbitrary row count — fragile!
  ) AS rolling_approx
FROM purchases;

SELECT
  customer_id, purchase_ts, amount,
  SUM(amount) OVER (
    PARTITION BY customer_id
    ORDER BY purchase_ts
    RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW
  ) AS rolling_24h
FROM purchases;

-- Find partitions bigger than N:
SELECT customer_id, COUNT(*) AS cnt
FROM purchases
GROUP BY customer_id
HAVING COUNT(*) > 1000000;

-- Show rows with identical timestamps (duplicates):
SELECT purchase_ts, COUNT(*) AS cnt
FROM purchases
GROUP BY purchase_ts
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 50;

-- Test ROWS vs RANGE on a tiny dataset (create & compare)
CREATE TEMP TABLE sample (
  id serial PRIMARY KEY,
  customer_id int,
  purchase_ts timestamptz,
  amount numeric
);

INSERT INTO sample (customer_id, purchase_ts, amount) VALUES
(1, '2023-01-01 00:00:01', 10),
(1, '2023-01-01 00:00:01', 20),
(1, '2023-01-01 00:00:02', 30);
-- ROWS cumulative
SELECT *, SUM(amount) OVER (PARTITION BY customer_id ORDER BY purchase_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_rows
FROM sample
ORDER BY purchase_ts, id;
-- RANGE cumulative (24h)
SELECT *, SUM(amount) OVER (PARTITION BY customer_id ORDER BY purchase_ts RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW) AS cum_range
FROM sample
ORDER BY purchase_ts, id;


CREATE TEMP TABLE sample (
  id serial PRIMARY KEY,
  customer_id int,
  purchase_ts timestamptz,
  amount numeric
);

INSERT INTO sample (customer_id, purchase_ts, amount) VALUES
(1, '2023-01-01 00:00:01', 10),
(1, '2023-01-01 00:00:01', 20),
(1, '2023-01-01 00:00:02', 30);
-- ROWS cumulative
SELECT *, SUM(amount) OVER (PARTITION BY customer_id ORDER BY purchase_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_rows
FROM sample
ORDER BY purchase_ts, id;
-- RANGE cumulative (24h)
SELECT *, SUM(amount) OVER (PARTITION BY customer_id ORDER BY purchase_ts RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW) AS cum_range
FROM sample
ORDER BY purchase_ts, id;
