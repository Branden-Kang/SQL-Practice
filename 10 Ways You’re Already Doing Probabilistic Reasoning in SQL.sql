-- 1. Confidence Intervals via STDDEV + AVG
WITH channel_stats AS (
    SELECT
        channel_name,
        COUNT(*) AS sample_size,
        AVG(revenue_amount) AS mean_revenue,
        -- Calculate Sample Standard Deviation
        STDDEV_SAMP(revenue_amount) AS std_dev_revenue
    FROM conversions
    WHERE conversion_date >= CURRENT_DATE - 30
    GROUP BY channel_name
    HAVING COUNT(*) > 30 -- CLT requires decent sample size (>30 is rule of thumb)
)
SELECT
    channel_name,
    sample_size,
    
    -- 1. Point Estimate
    ROUND(mean_revenue::numeric, 2) AS avg_revenue,
    
    -- 2. Margin of Error (95% Confidence)
    -- Formula: 1.96 * (StDev / Sqrt(N))
    ROUND(
        (1.96 * (std_dev_revenue / SQRT(sample_size)))::numeric, 
        2
    ) AS margin_of_error,
    
    -- 3. Confidence Interval Bounds
    ROUND((mean_revenue - (1.96 * (std_dev_revenue / SQRT(sample_size))))::numeric, 2) AS lower_bound_95,
    ROUND((mean_revenue + (1.96 * (std_dev_revenue / SQRT(sample_size))))::numeric, 2) AS upper_bound_95

FROM channel_stats
ORDER BY mean_revenue DESC;

-- 2. Exponential Smoothing with Self-Joins
SELECT
    user_id,
    category_id,
    
    -- 1. Simple Count (Static)
    -- "How many times did they interact total?"
    COUNT(*) AS raw_interaction_count,
    
    -- 2. Time-Decayed Score (Dynamic)
    -- "How relevant is this interest right now?"
    -- We use a decay rate (lambda) of 0.1. 
    -- Interpretation: Interest drops by ~10% per day passed.
    ROUND(
        SUM(
            CASE 
                -- Weighted Action: A 'Purchase' (5 pts) is worth more than a 'View' (1 pt)
                WHEN action_type = 'purchase' THEN 5.0 
                ELSE 1.0 
            END 
            * -- The Decay Function: EXP( -Rate * Days_Ago )
            EXP(-0.1 * (EXTRACT(EPOCH FROM (NOW() - event_timestamp)) / 86400))
        )::numeric, 
    2) AS recency_weighted_score

FROM user_actions
WHERE event_timestamp >= NOW() - INTERVAL '60 days' -- Optimization: ignore very old dust
GROUP BY user_id, category_id
ORDER BY recency_weighted_score DESC;

-- 3. Bayesian Updating with Incremental Filters
WITH today_data AS (
    -- 1. The Likelihood: Evidence from today's activity
    SELECT
        vendor_id,
        COUNT(*) AS n_trials_today,
        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS k_successes_today
    FROM daily_log
    WHERE date = CURRENT_DATE
    GROUP BY vendor_id
),
priors AS (
    -- 2. The Prior: Historical stats (or Global Defaults for new vendors)
    -- We assume a "Weak Prior" of 19 successes and 1 failure (95% rate, weight of 20)
    -- This prevents a single failure from tanking a score immediately
    SELECT
        v.vendor_id,
        COALESCE(h.total_successes, 19) AS alpha_prior,
        COALESCE(h.total_failures, 1)   AS beta_prior
    FROM vendors v
    LEFT JOIN historical_stats h ON v.vendor_id = h.vendor_id
)
SELECT
    p.vendor_id,
    
    -- The "Naive" Frequentist Score
    -- Problem: If 1 order total and it failed, this says 0% reliability.
    COALESCE(t.k_successes_today::FLOAT / NULLIF(t.n_trials_today, 0), 0) AS raw_daily_score,

-- The Bayesian Posterior Score (Updated Belief)
    -- Formula: (Prior Success + Today Success) / (Prior Total + Today Total)
    (p.alpha_prior + COALESCE(t.k_successes_today, 0))::FLOAT 
    / ((p.alpha_prior + p.beta_prior) + COALESCE(t.n_trials_today, 0)) AS bayesian_reliability_score
FROM priors p
LEFT JOIN today_data t ON p.vendor_id = t.vendor_id
ORDER BY bayesian_reliability_score ASC;

-- 4. Monte Carlo Simulations via Recursive CTEs
WITH RECURSIVE simulations AS (
    -- 1. Anchor Member: Initialize the starting state for 5 paths
    SELECT 
        1 AS day_num,
        path_id,
        100.0 AS price -- Starting Price ($100)
    FROM (SELECT generate_series(1, 5) AS path_id) AS seeds

UNION ALL
    -- 2. Recursive Member: Calculate the next day based on the previous day
    SELECT 
        s.day_num + 1,
        s.path_id,
        -- Apply random shock: Previous Price * (1 + Random % change)
        -- Simulates volatility between -2% and +2%
        s.price * (1 + (RAND() * 0.04 - 0.02)) 
    FROM simulations s
    WHERE s.day_num < 30 -- Stop after 30 days
)
SELECT 
    day_num,
    path_id,
    ROUND(price::numeric, 2) as projected_price
FROM simulations
ORDER BY path_id, day_num;

-- 5. Markov Chain Logic with LAG()
WITH transition_pairs AS (
    SELECT
        user_id,
        session_id,
        created_at,
        -- Define the "From" state (Previous Step)
        LAG(page_name) OVER (PARTITION BY session_id ORDER BY created_at) as from_state,
        -- Define the "To" state (Current Step)
        page_name as to_state
    FROM page_views
),
state_counts AS (
    -- Count how many times each specific transition occurred
    SELECT 
        from_state,
        to_state,
        COUNT(*) as transition_count
    FROM transition_pairs
    WHERE from_state IS NOT NULL -- Remove the first page view (no previous state)
    GROUP BY from_state, to_state
)
SELECT
    from_state,
    to_state,
    transition_count,
    
    -- Calculate Transition Probability: P(Next=To | Current=From)
    -- Formula: Count(Specific Transition) / Count(All Transitions starting from 'From')
    transition_count::FLOAT / SUM(transition_count) OVER (PARTITION BY from_state) as transition_probability,
    
    -- Visualizing the weight
    ROUND((transition_count::FLOAT / SUM(transition_count) OVER (PARTITION BY from_state)) * 100, 1) as prob_pct
    
FROM state_counts
ORDER BY from_state, transition_probability DESC;

-- 6. Binomial Approximations with SUM() + GROUP BY
WITH daily_stats AS (
    SELECT
        transaction_date,
        COUNT(*) as n_trials,         -- Total transactions (n)
        SUM(is_failed) as x_successes -- Total failures (x)
    FROM payments
    GROUP BY transaction_date
),
historical_baseline AS (
    -- Calculate the global failure rate (p) from past data
    SELECT
        SUM(x_successes)::FLOAT / SUM(n_trials) as p_rate
    FROM daily_stats
    WHERE transaction_date < CURRENT_DATE
)
SELECT
    ds.transaction_date,
    ds.n_trials,
    ds.x_successes as actual_failures,
    
    -- 1. Expected Value (Mean) = n * p
    (ds.n_trials * hb.p_rate) as expected_failures,

-- 2. Standard Deviation = SQRT( n * p * (1-p) )
    -- This measures the expected "spread" or variance in the data
    SQRT(ds.n_trials * hb.p_rate * (1.0 - hb.p_rate)) as std_dev,
    -- 3. Z-Score (How many standard deviations away is today?)
    -- If Z-Score > 3, it is a < 0.3% probability event (An Anomaly)
    (ds.x_successes - (ds.n_trials * hb.p_rate)) 
    / SQRT(ds.n_trials * hb.p_rate * (1.0 - hb.p_rate)) as z_score
FROM daily_stats ds
CROSS JOIN historical_baseline hb
ORDER BY ds.transaction_date DESC;

-- 7. Conditional Aggregation for Naive Bayes
SELECT
    -- Feature 1: Country Analysis
    t.country,
    -- Calculate P(Fraud | Country)
    -- "If a transaction is from this country, what is the chance it is fraud?"
    SUM(CASE WHEN t.is_fraud = 1 THEN 1 ELSE 0 END)::FLOAT 
    / COUNT(*) AS country_fraud_probability,
    
    -- Feature 2: Device Analysis (Aggregated relative to the country group for this view)
    -- Note: In a full Naive Bayes, these would often be separate lookup tables. 
    -- Here we show how to get multiple conditional probabilities in one pass.
    t.device_type,
    SUM(CASE WHEN t.is_fraud = 1 THEN 1 ELSE 0 END)::FLOAT 
    / COUNT(*) AS device_fraud_probability,

-- 8. Leverage CUME_DIST for Percentile-Based Probability
SELECT
    deal_id,
    deal_amount,
    
    -- 1. Cumulative Probability (CDF)
    -- "What is the probability a deal is LESS THAN or EQUAL to this amount?"
    CUME_DIST() OVER (ORDER BY deal_amount) AS prob_le_amount,

-- 2. Survival Function (Complementary CDF)
    -- "What is the probability a deal EXCEEDS this amount?"
    -- Useful for: "How likely are we to close a deal bigger than this?"
    1.0 - CUME_DIST() OVER (ORDER BY deal_amount) AS prob_exceeding_amount,
    -- 3. Percentile Rank (Contextualizing)
    -- Rounds to readable percentages for business stakeholders
    ROUND(
        (1.0 - CUME_DIST() OVER (ORDER BY deal_amount)) * 100, 
        2
    ) AS chance_to_beat_pct
FROM closed_deals
WHERE close_date >= NOW() - INTERVAL '3 months'
ORDER BY deal_amount DESC;

-- 9. Simulate Bernoulli Trials with RAND()
SELECT
    user_region,
    assigned_group,
    COUNT(*) as user_count,
    -- Calculate actual percentage to verify the simulation matches target probabilities
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY user_region) as actual_pct
FROM (
    SELECT
        user_id,
        user_region,
        -- Generate a random value once per row
        RAND() as probability_score, 
        CASE 
            -- First 5% probability (0.00 to 0.05)
            WHEN RAND() < 0.05 THEN 'Group A: Beta (High Risk)'
            -- Next 15% probability (0.05 to 0.20)
            WHEN RAND() >= 0.05 AND RAND() < 0.20 THEN 'Group B: Canary (Med Risk)'
            -- Remaining 80% probability
            ELSE 'Group C: Stable (Low Risk)'
        END as assigned_group
    FROM users
    WHERE is_active = 1
) as simulation_data
GROUP BY user_region, assigned_group
ORDER BY user_region, assigned_group;

-- 10. Use COUNT + Window Functions to Estimate Probabilities
SELECT
    subscription_tier,
    user_id,
    is_churned,
    
    -- 1. Conditional Probability: P(Churn | Tier)
    -- "How likely is a user in THIS tier to churn?"
    SUM(is_churned) OVER (PARTITION BY subscription_tier)::FLOAT
    / COUNT(*) OVER (PARTITION BY subscription_tier) AS tier_churn_prob,

-- 2. Marginal Probability: P(Churn)
    -- "How likely is ANY user to churn (Global Baseline)?"
    SUM(is_churned) OVER ()::FLOAT
    / COUNT(*) OVER () AS global_churn_prob,
    -- 3. Risk Ratio (Lift)
    -- "Is this tier riskier than the average user?"
    -- Formula: P(Churn | Tier) / P(Churn)
    (SUM(is_churned) OVER (PARTITION BY subscription_tier)::FLOAT / COUNT(*) OVER (PARTITION BY subscription_tier))
    / (SUM(is_churned) OVER ()::FLOAT / COUNT(*) OVER ()) AS risk_lift
FROM subscriptions
-- Filter to active analysis window to ensure relevance
WHERE created_at >= NOW() - INTERVAL '1 year';
