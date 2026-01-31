-- 1. Obsess Over Output Formatting
SELECT
    -- Formatting Name: Concatenating for readability
    CONCAT(UPPER(LEFT(last_name, 1)), LOWER(SUBSTRING(last_name, 2, LEN(last_name))), ', ', first_name) AS full_name_display,

    -- Formatting Numbers: Adding currency symbols and commas directly
    FORMAT(sales_target, 'C', 'en-US') AS formatted_target,

    -- Handling NULLs: Never returning blank cells for critical metrics
    COALESCE(FORMAT(sales_achieved, 'C', 'en-US'), 'No Sales Recorded') AS formatted_actuals,

    -- Adding Visual Logic: Creating a text-based progress bar for quick scanning
    CASE
        WHEN sales_achieved >= sales_target THEN 'Goal Met'
        WHEN sales_achieved >= (sales_target * 0.8) THEN 'Near Goal'
        ELSE 'Needs Attention'
    END AS status_indicator,

    -- Advanced: Generating a simple text-based bar chart
    REPLICATE('|', CAST((sales_achieved * 10.0 / sales_target) AS INT)) AS visual_progress
FROM sales_team_performance
ORDER BY sales_achieved DESC;

-- 2. Network Outside Data Teams
SELECT 
    sales_rep_name,
    
    -- Junior Metric: Raw Revenue (Misleading)
    SUM(deal_amount) as raw_revenue,
    
    -- Senior Metric: The "Real" Score (Tribal Knowledge applied)
    SUM(
        CASE 
            -- Rule learned from Sales Ops: "House" accounts are unearned wins.
            WHEN account_type = 'House Account' THEN 0 
            
            -- Rule learned from Finance: Renewals only count for 10% credit.
            WHEN deal_type = 'Renewal' THEN deal_amount * 0.10
            
            -- Standard New Business counts 100%
            ELSE deal_amount 
        END
    ) as commissionable_performance
    
FROM sales_opportunities
WHERE status = 'Closed Won'
GROUP BY sales_rep_name
ORDER BY commissionable_performance DESC;

-- 3. Build “Decision Batteries”
/* Senior Move: Embedding 'Decision Logic' into a View.
  Instead of just reporting numbers, this View makes the decision 
  (Green/Red/Warning) for the stakeholder.
*/

CREATE OR REPLACE VIEW v_DailyServerHealth AS
WITH DailyStats AS (
    SELECT
        server_id,
        CAST(log_timestamp AS DATE) as log_date,
        AVG(cpu_usage) as avg_cpu,
        MAX(cpu_usage) as max_cpu
    FROM server_logs
    WHERE log_timestamp >= DATEADD(day, -30, GETDATE()) -- Rolling 30 day window
    GROUP BY server_id, CAST(log_timestamp AS DATE)
)
SELECT
    log_date,
    server_id,
    avg_cpu,
    max_cpu,
    -- The "Battery": Pre-calculating the alert status
    CASE 
        WHEN max_cpu >= 95 THEN 'CRITICAL ALERT'
        WHEN avg_cpu >= 80 THEN 'WARNING'
        ELSE 'HEALTHY'
    END as status_flag
FROM DailyStats;

-- 4. Code for Humans, Not Machines
/* Objective: Find VIPs (>$10k spend) with zero activity in 90 days */

WITH CustomerLifetimeValue AS (
    -- Step 1: Who are our best customers?
    SELECT 
        customer_id, 
        SUM(total_amount) as lifetime_spend
    FROM orders
    WHERE status = 'completed'
    GROUP BY customer_id
    HAVING SUM(total_amount) > 10000
),

RecentActivity AS (
    -- Step 2: Who has bought something recently?
    SELECT DISTINCT customer_id
    FROM orders
    WHERE created_at >= DATEADD(day, -90, GETDATE())
)

-- Step 3: Combine logic. (The "Human" part)
-- It reads like an English sentence: Take VIPs, keep those NOT in RecentActivity.
SELECT 
    vip.customer_id,
    vip.lifetime_spend
FROM CustomerLifetimeValue vip
LEFT JOIN RecentActivity recent ON vip.customer_id = recent.customer_id
WHERE recent.customer_id IS NULL;

-- 5. Proactively Kill Low-Value Requests
/* Objective: Validate if 'job_title' is a usable field for segmentation.
   Tactic: Check Fill Rate (completeness) and Cardinality (uniqueness)
*/

SELECT
    COUNT(*) as total_rows,
    
    -- 1. Fill Rate: Is there enough data to matter?
    SUM(CASE WHEN job_title IS NOT NULL AND job_title <> '' THEN 1 ELSE 0 END) 
        * 100.0 / COUNT(*) as fill_rate_pct,
        
    -- 2. Cardinality: Is the data too fragmented? (High ratio = bad for grouping)
    COUNT(DISTINCT job_title) as unique_values,
    
    -- 3. Top Concentration: Do the top 3 titles cover a meaningful portion?
    (SELECT SUM(cnt) FROM (
        SELECT TOP 3 COUNT(*) as cnt 
        FROM leads 
        GROUP BY job_title 
        ORDER BY cnt DESC
     ) as top_3) * 100.0 / COUNT(*) as top_3_dominance_pct

FROM leads;

-- 6. Master One Advanced Tool Deeply
WITH Step1_FlagNewSessions AS (
    SELECT
        user_id,
        timestamp,
        -- Check if the gap between this event and the last one is > 30 mins
        -- If yes, it's the start of a new session (1), else it's part of the current one (0)
        CASE 
            WHEN DATEDIFF(minute, LAG(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp), timestamp) > 30 
            THEN 1 
            ELSE 0 
        END as is_new_session
    FROM website_clicks
),
Step2_CreateSessionID AS (
    SELECT
        user_id,
        timestamp,
        -- A running total of the 'is_new_session' flags effectively generates a unique ID 
        -- for every session per user.
        SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY timestamp) as session_id
    FROM Step1_FlagNewSessions
)
-- Now we can aggregate by this custom-made Session ID
SELECT
    user_id,
    session_id,
    MIN(timestamp) as session_start,
    MAX(timestamp) as session_end,
    COUNT(*) as events_in_session
FROM Step2_CreateSessionID
GROUP BY user_id, session_id;

-- 7. Document Like They’ll Be Forget Tomorrow
/* Query: Monthly Net Revenue (GAAP Adjusted)
  Owner: Data Team
  Ticket Reference: JIRA-DAT-405
  Logic Update: As of 2024-01, we exclude 'Beta' users from revenue metrics.
*/

WITH ExcludedUsers AS (
    -- These are internal QA accounts used for testing payments
    -- List maintained by Engineering (last sync: 2025-01-15)
    SELECT user_id 
    FROM internal_users 
    WHERE role IN ('QA', 'Dev', 'Beta')
),

RevenueBase AS (
    SELECT 
        t.transaction_id,
        t.amount_cents,
        t.currency,
        -- LOGIC: Stripe fees are 2.9% + 30 cents. 
        -- We deduct this to find Net Revenue.
        (t.amount_cents * 0.029) + 30 AS estimated_fee_cents
    FROM transactions t
    LEFT JOIN ExcludedUsers eu ON t.user_id = eu.user_id
    WHERE eu.user_id IS NULL -- Excluding internal accounts
      AND t.status = 'succeeded'
)

SELECT 
    DATE_TRUNC('month', created_at) as month,
    SUM(amount_cents - estimated_fee_cents) / 100.0 as net_revenue_usd
FROM RevenueBase
GROUP BY 1;

-- 8. Speak “Business,” Not Just Data
WITH FailedCheckouts AS (
    SELECT
        t.transaction_id,
        t.amount,
        c.customer_tier -- e.g., 'Enterprise' vs 'Standard'
    FROM transactions t
    JOIN customers c ON t.customer_id = c.id
    WHERE t.status = 'failed'
      AND t.created_at >= DATEADD(day, -7, GETDATE())
)
SELECT
    customer_tier,
    COUNT(transaction_id) as failure_count,
    -- The "Senior" Layer: Translating technical errors into financial loss
    FORMAT(SUM(amount), 'C', 'en-US') as revenue_at_risk,
    
    -- Contextualizing severity: Is this a big deal?
    CAST(SUM(amount) * 100.0 / NULLIF((SELECT SUM(amount) FROM transactions), 0) 
    AS DECIMAL(5,2)) as pct_of_total_revenue
FROM FailedCheckouts
GROUP BY customer_tier
-- Order by Money, not Count. Enterprise failures matter more even if volume is low.
ORDER BY SUM(amount) DESC;

-- 9. Automate Before Perfecting
-- The "Senior" approach: Wrapping logic in a reusable shell immediately
CREATE OR ALTER PROCEDURE GetRegionalChurn
    @RegionName NVARCHAR(50),
    @ReportMonth DATE
AS
BEGIN
    -- 'Ugly' but functional: Using Temp Tables for speed rather than complex CTEs
    -- This allows for easy step-by-step debugging later if it breaks
    SELECT
        CustomerID,
        SubscriptionStatus
    INTO #MonthlyData
    FROM Subscriptions
    WHERE Region = @RegionName
      AND EOMONTH(StartDate) = EOMONTH(@ReportMonth);

    -- Calculate Churn
    SELECT
        @RegionName AS Region,
        COUNT(CASE WHEN SubscriptionStatus = 'Cancelled' THEN 1 END) * 1.0 / COUNT(*) AS ChurnRate,
        GETDATE() AS ReportGeneratedAt
    FROM #MonthlyData;
    
    -- Clean up (Good habit, even if 'ugly' code)
    DROP TABLE #MonthlyData;
END;

-- AUTOMATION IN ACTION:
-- Now, the analyst can generate 5 reports in 5 seconds:
EXEC GetRegionalChurn 'North America', '2023-10-01';
EXEC GetRegionalChurn 'Europe', '2023-10-01';

-- 10. Frame Problems Before Solving Them
WITH DailySales AS (
    SELECT
        sale_date,
        SUM(revenue) as daily_rev
    FROM sales_data
    WHERE sale_date >= DATEADD(year, -1, GETDATE())
    GROUP BY sale_date
),
TrendAnalysis AS (
    SELECT
        sale_date,
        daily_rev,
        -- Calculate 7-day moving average to smooth noise
        AVG(daily_rev) OVER (
            ORDER BY sale_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as moving_avg,
        -- Compare current day to previous week (WoW)
        LAG(daily_rev, 7) OVER (ORDER BY sale_date) as prev_week_rev
    FROM DailySales
)
SELECT
    sale_date,
    daily_rev,
    moving_avg,
    prev_week_rev,
    -- Calculate % drop
    (daily_rev - prev_week_rev) * 100.0 / NULLIF(prev_week_rev, 0) as pct_change
FROM TrendAnalysis
-- The "Frame": Only alert if revenue drops by >15% vs last week
WHERE (daily_rev - prev_week_rev) * 100.0 / NULLIF(prev_week_rev, 0) < -15
ORDER BY sale_date DESC;
