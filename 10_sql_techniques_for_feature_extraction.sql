-- 1. Modularize with CTEs (Common Table Expressions)
WITH cleaned AS (
    SELECT
        user_id,
        COALESCE(spend, 0) AS spend,
        COALESCE(login_at, DATE '2000-01-01') AS login_at
    FROM raw_users
),

filtered AS (
    SELECT *
    FROM cleaned
    WHERE spend >= 0
),
aggregated AS (
    SELECT
        user_id,
        SUM(spend) AS total_spend,
        MAX(login_at) AS last_login
    FROM filtered
    GROUP BY user_id
)
SELECT
    user_id,
    total_spend,
    last_login
FROM aggregated;

-- 2. Interaction Features (Mathematical Combinations)
SELECT
    user_id,
    total_orders,
    total_returns,
    CAST(total_returns AS FLOAT) 
        / NULLIF(total_orders, 0) AS return_rate
FROM user_orders;

-- 3. Logarithmic Transformations on the Fly
SELECT
    user_id,
    view_count,
    LOG(view_count + 1) AS view_count_log,
    LOG(followers + 1) AS followers_log
FROM user_stats;

-- 4. Lag and Lead Features for Sequence Modeling
SELECT
    user_id,
    event_time,
    page_visited,
    LAG(page_visited, 1) OVER (
        PARTITION BY user_id
        ORDER BY event_time
    ) AS prev_page,
    LAG(page_visited, 2) OVER (
        PARTITION BY user_id
        ORDER BY event_time
    ) AS prev_page_2
FROM page_events
ORDER BY user_id, event_time;

-- 5. Complex String Parsing for Categorical Features
SELECT
    user_id,
    email,
    SPLIT_PART(email, '@', 2) AS domain_provider
FROM users;

-- 6. Handling Sparse Data with COALESCE
SELECT
    user_id,
    COALESCE(page_views, 0) AS page_views_filled,
    COALESCE(ad_clicks, 0) AS ad_clicks_filled,
    COALESCE(device_type, 'Unknown') AS device_type_filled
FROM user_engagement;

-- 7. NTILE Binning for Outlier Management
SELECT
    user_id,
    spend,
    NTILE(10) OVER (
        ORDER BY spend
    ) AS spending_decile
FROM user_spend;

-- 8. Time-Since and Time-Until Recency Features
SELECT
    user_id,
    MAX(purchase_date) AS last_purchase_date,
    DATEDIFF(day, MAX(purchase_date), CURRENT_DATE) AS days_since_last_purchase,
    MIN(purchase_date) AS first_purchase_date,
    DATEDIFF(day, CURRENT_DATE, MIN(purchase_date)) AS days_until_first_purchase -- if forecasting future events
FROM purchases
GROUP BY user_id;

-- 9. Sliding Window Aggregates
SELECT
    user_id,
    date,
    spend,
    AVG(spend) OVER (
        PARTITION BY user_id
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7d
FROM transactions
ORDER BY user_id, date;

-- 10. One-Hot Encoding via Conditional Aggregation
SELECT
    user_id,
    SUM(CASE WHEN category = 'electronics' THEN 1 ELSE 0 END) AS electronics_count,
    SUM(CASE WHEN category = 'fashion' THEN 1 ELSE 0 END) AS fashion_count,
    SUM(CASE WHEN category = 'grocery' THEN 1 ELSE 0 END) AS grocery_count,
    COUNT(*) AS total_transactions
FROM transactions
GROUP BY user_id
ORDER BY user_id;
