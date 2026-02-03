-- Window Functions
-- AVG(column_name) OVER (
--     PARTITION BY [optional_grouping]
--     ORDER BY [date_column]
--     ROWS BETWEEN [N] PRECEDING AND CURRENT ROW
-- )

-- The 7-Day Rolling Revenue
SELECT 
    sale_date,
    daily_revenue,
    -- The magic happens here
    AVG(daily_revenue) OVER (
        ORDER BY sale_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_7d
FROM ecommerce_sales
;

-- 5-Day Moving Average for User Signups
SELECT 
    region,
    signup_date,
    signup_count,
    AVG(signup_count) OVER (
        PARTITION BY region 
        ORDER BY signup_date 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS moving_avg_signups_5d
FROM user_growth
;

-- Missing Dates
WITH date_series AS (
    -- Generates every date from Jan 1 to Jan 8
    SELECT generate_series('2024-01-01'::date, '2024-12-31'::date, '1 day')::date AS sale_date
),
complete_data AS (
    -- Joins the calendar to actual sales, filling gaps with 0
    SELECT 
        ds.sale_date,
    -- Fill the gaps
        COALESCE(s.daily_revenue, 0) AS daily_revenue
    FROM date_series ds
    LEFT JOIN ecommerce_sales s ON ds.sale_date = s.sale_date
)
SELECT 
    sale_date,
    daily_revenue,
    AVG(daily_revenue) OVER (
        ORDER BY sale_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_7d
FROM complete_data
;

-- The “Cold Start” Problem
WITH moving_avg_calc AS (
    SELECT 
        sale_date,
        daily_revenue,
        AVG(daily_revenue) OVER (ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg,
        ROW_NUMBER() OVER (ORDER BY sale_date) AS day_count
    FROM ecommerce_sales
)
SELECT 
    sale_date, 
    daily_revenue, 
    rolling_avg
FROM moving_avg_calc
WHERE day_count >= 7
; -- Only show results once we have a full 7-day window
