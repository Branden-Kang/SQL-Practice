-- Use the pattern to calculate the Lower Bound (LB) and Upper Bound (UB) dynamically
WITH Stats_Aggregates AS (
    SELECT
        campaign_id,
        -- Calculate the core statistical components
        AVG(daily_revenue) AS mean_revenue,
        STDDEV(daily_revenue) AS std_dev,
        COUNT(daily_revenue) AS n_samples,
        -- 1.96 is the Z-score for a 95% Confidence Level
        1.96 AS z_score
    FROM
        marketing_campaign_performance
    WHERE
        event_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY
        campaign_id
    HAVING
        COUNT(daily_revenue) > 1 -- Need at least 2 data points for variance
),
Confidence_Calculations AS (
    SELECT
        campaign_id,
        mean_revenue,
        -- Calculate Standard Error: sigma / sqrt(n)
        (std_dev / SQRT(n_samples)) AS standard_error,
        z_score
    FROM
        Stats_Aggregates
)
SELECT
    campaign_id,
    ROUND(mean_revenue, 2) AS avg_revenue,
    -- Margin of Error = Z * Standard Error
    ROUND(z_score * standard_error, 2) AS margin_of_error,
    -- Construct the Interval
    ROUND(mean_revenue - (z_score * standard_error), 2) AS ci_lower_bound,
    ROUND(mean_revenue + (z_score * standard_error), 2) AS ci_upper_bound,
    CASE
        WHEN (mean_revenue - (z_score * standard_error)) > 1000 THEN 'Significant Success ✅'
        ELSE 'Noise / Inconclusive ⚠️'
    END AS statistical_verdict
FROM
    Confidence_Calculations
ORDER BY
    mean_revenue DESC;

-- Rolling CIs
WITH Rolling_Stats AS (
    SELECT
        transaction_date,
        daily_retention_rate,
        -- Calculate moving average over 7 days
        AVG(daily_retention_rate) OVER (
            ORDER BY transaction_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS moving_avg,
        -- Calculate moving standard deviation over 7 days
        STDDEV(daily_retention_rate) OVER (
            ORDER BY transaction_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS moving_std_dev,
        -- Count rows to ensure we have enough data for the window
        COUNT(daily_retention_rate) OVER (
            ORDER BY transaction_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS window_count
    FROM
        subscription_metrics
),
Rolling_CI AS (
    SELECT
        transaction_date,
        daily_retention_rate,
        moving_avg,
        -- Calculate 95% CI Bounds (Z = 1.96)
        -- Formula: Mean ± (Z * (StdDev / Sqrt(N)))
        (moving_avg - (1.96 * (moving_std_dev / SQRT(window_count)))) AS lower_bound,
        (moving_avg + (1.96 * (moving_std_dev / SQRT(window_count)))) AS upper_bound
    FROM
        Rolling_Stats
    WHERE
        window_count >= 7 -- Only analyze after full window is established
)
SELECT
    transaction_date,
    ROUND(daily_retention_rate, 4) as actual_rate,
    ROUND(lower_bound, 4) as ci_min,
    ROUND(upper_bound, 4) as ci_max,
    -- The "Panic Button" Metric
    ROUND(upper_bound - lower_bound, 4) as ci_width,
    CASE
        WHEN daily_retention_rate < lower_bound THEN 'Drop Alert 🔻'
        WHEN (upper_bound - lower_bound) > 0.15 THEN 'Volatility Spike (Bug?) 🐛'
        ELSE 'Stable ✅'
    END AS status
FROM
    Rolling_CI
ORDER BY
    transaction_date DESC;

-- FPC
WITH Population_Stats AS (
    -- Define your known total population (N)
    -- In a real scenario, this might be a sub-query counting total active VIPs
    SELECT 200.0 AS population_N
),
Sample_Aggregates AS (
    SELECT
        feature_id,
        AVG(satisfaction_score) AS sample_mean,
        STDDEV(satisfaction_score) AS sample_std_dev,
        COUNT(satisfaction_score) AS sample_n
    FROM
        vip_survey_responses
    GROUP BY
        feature_id
    HAVING
        COUNT(satisfaction_score) > 1
)
SELECT
    s.feature_id,
    ROUND(s.sample_mean, 2) AS avg_score,
    s.sample_n,
    p.population_N,
    
    -- 1. Standard Error (The Naive Approach)
    ROUND(
        s.sample_std_dev / SQRT(s.sample_n), 
    3) AS standard_se,
    
    -- 2. Finite Population Correction Factor
    -- Formula: SQRT((N - n) / (N - 1))
    ROUND(
        SQRT((p.population_N - s.sample_n) / (p.population_N - 1)), 
    3) AS fpc_factor,
    
    -- 3. Adjusted Margin of Error (95% CI)
    -- As sample_n gets closer to population_N, this error shrinks to Zero.
    ROUND(
        1.96 * (s.sample_std_dev / SQRT(s.sample_n)) * SQRT((p.population_N - s.sample_n) / (p.population_N - 1)), 
    2) AS adj_margin_of_error,
    
    CASE 
        WHEN (s.sample_n / p.population_N) > 0.05 THEN 'FPC Applied ✅'
        ELSE 'Standard Approx OK ℹ️'
    END AS calculation_method
FROM
    Sample_Aggregates s
CROSS JOIN
    Population_Stats p
ORDER BY
    s.sample_mean DESC;

-- The Sanity Check
SELECT
    -- The number everyone looks at
    AVG(your_column) AS mean,
    -- The number YOU need to look at
    AVG(your_column) - 1.96 * (STDDEV(your_column) / SQRT(COUNT(*))) AS lower_bound,
    AVG(your_column) + 1.96 * (STDDEV(your_column) / SQRT(COUNT(*))) AS upper_bound
FROM
    your_table;

-- Handling Proportions
SELECT
    variant_id,
    SUM(converted) / COUNT(*) AS conversion_rate,
    -- Wilson Score Interval Lower Bound
    (
      (SUM(converted) + 1.92) / (COUNT(*) + 3.84) -
      1.96 * SQRT(
        (SUM(converted) * (COUNT(*) - SUM(converted)) / COUNT(*) + 0.96) /
        (COUNT(*) + 3.84)
      ) / COUNT(*)
    ) AS lower_bound,
    -- Wilson Score Interval Upper Bound
    (
      (SUM(converted) + 1.92) / (COUNT(*) + 3.84) +
      1.96 * SQRT(
        (SUM(converted) * (COUNT(*) - SUM(converted)) / COUNT(*) + 0.96) /
        (COUNT(*) + 3.84)
      ) / COUNT(*)
    ) AS upper_bound
FROM
    ab_test_results
GROUP BY
    variant_id;
