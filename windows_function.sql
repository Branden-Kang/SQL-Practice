-- LAG()
SELECT 
MONTH_ID, MONTH_NAME, 
LAG(REVENUE, 1) OVER (ORDER BY MONTH_ID) AS PREVIOUS_MONTH_REVENUE,
REVENUE AS CURRENT_MONTH_REVENUE
FROM MONTHLY_REVENUE_DETAIL

SELECT 
MONTH_ID, MONTH_NAME, 
LAG(REVENUE, 1) OVER (ORDER BY MONTH_ID) AS PREVIOUS_MONTH_REVENUE,
REVENUE,
   FORMAT((REVENUE - LAG(REVENUE, 1) OVER (ORDER BY MONTH_ID))/
LAG(REVENUE, 1) OVER (ORDER BY MONTH_ID), 'P')
FROM MONTHLY_REVENUE_DETAILS

 CREATE DATABASE WINDOWS_FUNCTION_PRACTICE_MEDIUM_ARTICLE;

   USE WINDOWS_FUNCTION_PRACTICE_MEDIUM_ARTICLE;

   CREATE TABLE MONTHLY_REVENUE_DETAILS
    (MONTH_ID INT,
    MONTH_NAME TEXT,
    REVENUE NUMERIC(10,2));


    INSERT INTO MONTHLY_REVENUE_DETAILS
    VALUES (1, 'JANUARY', 50500),
           (2, 'FEBRUARY', 42500),
           (3, 'MARCH', 65000),
           (4, 'APRIL', 71000),
           (5, 'MAY', 68000),
           (6, 'JUNE', 59000),
           (7, 'JULY', 81000),
           (8, 'AUGUST', 71500),
           (9, 'SPETEMBER', 64000),
           (10, 'OCTOBER', 87000),
           (11, 'NOVEMBER', 89000),
           (12, 'DECEMBER', 125000);
                
    CREATE TABLE TESLA_YEARLY_STOCK_PRICE_HISTORY(
    YEAR INT,
    REVENUE FLOAT);


    INSERT INTO TESLA_YEARLY_STOCK_PRICE_HISTORY
    VALUES (2009, 112000000),
           (2010, 117000000),
           (2011, 204000000),
           (2012, 413000000),
           (2013, 2013000000),
           (2014, 3198000000),
           (2015, 4046000000),
           (2016, 7000000000),
           (2017, 11759000000),
           (2018, 21461000000),
           (2019, 24578000000),
           (2020, 31536000000),
           (2021, 53823000000),
           (2022, 81462000000),
           (2023, 96773000000);

-- LEAD()
SELECT 
MONTH_ID, MONTH_NAME, REVENUE,
LEAD(REVENUE, 1) OVER (ORDER BY MONTH_ID) AS NEXT_MONTH_REVENUE
FROM MONTHLY_REVENUE_DETAILS
  
SELECT 
MONTH_ID, MONTH_NAME,
REVENUE,
LEAD(REVENUE, 1) OVER (ORDER BY MONTH_ID) AS NEXT_MONTH_REVENUE,
FORMAT((LEAD(REVENUE, 1) OVER (ORDER BY MONTH_ID) - REVENUE)
/REVENUE, 'P')
FROM MONTHLY_REVENUE_DETAILS

-- ROW_NUMBER()
-- RANK()
-- DENSE_RANK()
SELECT *,
ROW_NUMBER() OVER (ORDER BY NUMBER) AS ROW_NUMBER,
RANK() OVER (ORDER BY NUMBER) AS RANK,
DENSE_RANK() OVER (ORDER BY NUMBER) AS DENSE_RANK
FROM LIST_OF_NUMBERS

SELECT Category,
Module as App_Name,
FORMAT(ROUND(SUM(Daily_Revenue), -3), 'C', 'US-US') 
AS REVENUE_PER_MODULE,
ROW_NUMBER() OVER (PARTITION BY Category
                   ORDER BY SUM(Daily_Revenue) DESC) AS RN
FROM [dbo].[DW_DEFI_REVENUE_TABLE]
GROUP BY Category, Module

SELECT *
FROM (
SELECT Category,
       Module as App_Name,
    FORMAT(ROUND(SUM(Daily_Revenue), -3), 'C', 'US-US') AS REVENUE_PER_MODULE,
    ROW_NUMBER() OVER (PARTITION BY Category
                             ORDER BY SUM(Daily_Revenue) DESC) AS RN
FROM [dbo].[DW_DEFI_REVENUE_TABLE]
GROUP BY Category, Module
) SUB_SUB    
WHERE RN <= 2

-- FIRST_VALUE()
-- LAST_VALUE()
SELECT TOP 1
       YEAR, 
       REVENUE,
       FIRST_VALUE(REVENUE) OVER
       (ORDER BY YEAR) AS FIRST_YEAR_REVENUE,
       LAST_VALUE(REVENUE) OVER
       (ORDER BY YEAR DESC) AS MOST_RECENT_YEAR_REVENUE
FROM TESLA_YEARLY_STOCK_PRICE_HISTORY
