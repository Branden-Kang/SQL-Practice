CREATE TABLE store_sales (
  store_id INT,
  store_name VARCHAR(50),
  state VARCHAR(50),
  month DATE,
  sales DECIMAL(10, 2)
);

INSERT INTO store_sales VALUES
(1, 'ABC Mart', 'California', '2022-01-01', 10000.00),
(2, 'XYZ Store', 'Texas', '2022-01-01', 7500.00),
(3, 'Corner Shop', 'New York', '2022-01-01', 12000.00),
(4, 'MegaMart', 'California', '2022-01-01', 25000.00),
(5, 'My Store', 'Texas', '2022-01-01', 5000.00),
(6, 'SuperMart', 'New York', '2022-01-01', 18000.00),
(7, 'ABC Mart', 'California', '2022-02-01', 15000.00),
(8, 'XYZ Store', 'Texas', '2022-02-01', 9000.00),
(9, 'Corner Shop', 'New York', '2022-02-01', 10000.00),
(10, 'MegaMart', 'California', '2022-02-01', 30000.00),
(11, 'My Store', 'Texas', '2022-02-01', 6000.00),
(12, 'SuperMart', 'New York', '2022-02-01', 20000.00),
(13, 'ABC Mart', 'California', '2022-03-01', 12000.00),
(14, 'XYZ Store', 'Texas', '2022-03-01', 10500.00),
(15, 'Corner Shop', 'New York', '2022-03-01', 15000.00),
(16, 'MegaMart', 'California', '2022-03-01', 27500.00),
(17, 'My Store', 'Texas', '2022-03-01', 5500.00),
(18, 'SuperMart', 'New York', '2022-03-01', 19000.00),
(19, 'ABC Mart', 'California', '2022-04-01', 14000.00),
(20, 'XYZ Store', 'Texas', '2022-04-01', 8000.00);

SELECT 
    store_name,
    state,
    SUM(sales) AS total_sales,
    AVG(SUM(sales)) OVER (PARTITION BY state) AS state_average
FROM 
    store_sales
GROUP BY 
    store_name, state;
    
SELECT
 store_name,
 state,
 sales,
 DENSE_RANK() OVER (PARTITION BY state
ORDER BY
 sales DESC) AS store_sales_rank
FROM
 store_sales;
 
-- 1. Calculating running totals
SELECT
 store_name,
 MONTH,
 sales,
 SUM(sales) OVER (PARTITION BY store_name
ORDER BY
 "month") AS running_total
FROM
 store_sales;
 
-- 2. Comparing to a group statistic.
SELECT
 store_name,
 state ,
 MONTH,
 sales,
 AVG(sales) OVER (PARTITION BY state, "month") AS running_total
FROM
 store_sales;
 
-- 3. Calculating moving averages
SELECT
 store_name ,
 MONTH,
 sales,
 AVG(sales) OVER (PARTITION BY store_name
ORDER BY
 MONTH ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_sales
FROM
 store_sales;

SELECT
 store_name ,
 MONTH,
 sales,
 AVG(sales) OVER (PARTITION BY store_name 
ORDER BY
 MONTH ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS moving_avg_sales
FROM
 store_sales;
