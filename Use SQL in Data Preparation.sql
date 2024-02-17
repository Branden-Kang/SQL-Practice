-- 1. Eliminating Duplicates
SELECT DISTINCT order_idf FROM orders;
SELECT DISTINCT ON (customer_id) * FROM orders;

-- 2. Dealing with missing values
SELECT COALESCE(order_date, CURRENT_DATE) FROM orders;
SELECT NULLIF(order_amount, 0) FROM orders;
SELECT CASE
  WHEN order_amount > 1000 THEN 'High'
  WHEN order_amount BETWEEN 500 AND 1000 THEN 'Medium'
  WHEN order_amount < 500 THEN 'Low'
  ELSE 'Unknown'
END AS order_level FROM orders;

-- 3. Standardizing mismatched data types
SELECT CAST(order_amount AS INTEGER) FROM orders;
SELECT CONVERT(order_date, CURRENT_DATE) FROM orders;
SELECT FORMAT(order_date, 'YYYY-MM-DD') FROM orders;

-- 4. Grouping and Filtering data
SELECT customer_id, SUM(order_amount) AS total_amount,
        AVG(order_amount) AS average_amount
FROM orders
GROUP BY customer_id;

SELECT customer_id, SUM(order_amount) AS total_amount
FROM orders
GROUP BY customer_id
HAVING SUM(order_amount) > 5000;

SELECT *
FROM orders
WHERE order_date BETWEEN '2024-01-01'AND '2024-01-31';

-- 5. Merging data with SQL joins and unions
SELECT * 
FROM orders 
JOIN customers ON orders.customer_id = customers.customer_id;

-- 6. Union vs. Union All:
SELECT * FROM orders 
UNION 
SELECT * FROM returns;

-- 7. Creating new variables:
SELECT order_amount, CASE
  WHEN order_amount > 1000 THEN 'High'
  WHEN order_amount BETWEEN 500 AND 1000 THEN 'Medium'
  WHEN order_amount < 500 THEN 'Low'
  ELSE 'Unknown'
END AS order_level FROM orders;

-- 8. Aggregating data:
SELECT customer_id, 
  SUM(order_amount) AS total_amount, 
  AVG(order_amount) AS average_amount 
FROM orders 
GROUP BY customer_id;

-- 9. Applying statistical and mathematical functions:
SELECT ROUND(order_amount, 2) FROM orders;

-- 10.  Sorting and ranking data
SELECT * FROM orders ORDER BY order_date DESC LIMIT 10;
