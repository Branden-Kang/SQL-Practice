# Linear Regression

CREATE VIEW daily_orders AS (
  SELECT
    `Order Date` AS order_date,
    DATEDIFF(`Order Date`, MIN(`Order Date`) OVER (ORDER BY `Order Date`)) AS order_date_number,
    COUNT(`Order ID`) AS orders,
    SUM(COUNT(`Order ID`)) OVER (
      ORDER BY `Order Date`
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS `cumulative_orders`
  FROM
    super_store
  GROUP BY
    `Order Date`
);

SELECT
  order_date_number AS x,
  AVG(order_date_number) OVER() AS x_mean,
  cumulative_orders AS y,
  AVG(cumulative_orders) OVER() AS y_mean
FROM
  daily_orders
  
WITH
  means AS (
    SELECT
      order_date_number AS x,
      AVG(order_date_number) OVER() AS x_mean,
 
      cumulative_orders AS y,
      AVG(cumulative_orders) OVER() AS y_mean
    FROM
      daily_orders
  ),

  slope_calculation AS (
    SELECT
      *,
      SUM((x - x_mean) * (y - y_mean)) OVER () / SUM(POWER((x - x_mean), 2)) OVER () AS slope
    FROM
      means
  )
SELECT
  *
from
  slope_calculation
  
WITH
  means AS (
    SELECT
      order_date_number AS x,
      AVG(order_date_number) OVER() as x_mean,
 
      cumulative_orders AS y,
      AVG(cumulative_orders) OVER() as y_mean
    FROM
      daily_orders
  ),
  slope_calculation AS (
    SELECT
      *,
      SUM((x - x_mean) * (y - y_mean)) OVER () / SUM(POWER((x - x_mean), 2)) OVER () AS slope
    FROM
      means
  ),
  intercept_calculation AS (
    SELECT
      *,
      y_mean - (x_mean * slope) AS intercept
    FROM
      slope_calculation
  )
SELECT
  *
FROM
  intercept_calculation
  
# You must chain all the CTE! Content is reduced to ease understanding.
linear_regression AS (
    SELECT
      x,
      x_mean,
      y,
      y_mean,
      slope,
      intercept,
      (x * slope + intercept) AS y_predicted,
      ((x + 1) * slope + intercept) AS next_y_predicted
    FROM
      intercept_calculation
  )
SELECT
  *
FROM
  linear_regression
  
# What if you are using PostgreSQL ?

WITH linear_regression AS (
  SELECT 
    regr_slope(y, x) AS slope,
    regr_intercept(y, x) AS intercept
  FROM
    super_store
  WHERE
    y IS NOT NULL
)

SELECT
   x,
   (x * slope) + intercept AS prediction
FROM
   super_store
CROSS JOIN
   linear_regression
WHERE
   y IS NULL
;
