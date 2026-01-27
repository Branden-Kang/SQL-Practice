-- 1. Rollback Strategies: Hope for the Best, Plan for the Worst
/* Scenario: We are updating the 'dim_customers' table.
   We do not want users to see partial data while we load.
*/

-- 1. Create/Truncate the Staging Table (The 'Green' environment)
CREATE TABLE IF NOT EXISTS dim_customers_staging LIKE dim_customers;
TRUNCATE TABLE dim_customers_staging;
-- 2. Load data into Staging (Heavy lifting happens here)
INSERT INTO dim_customers_staging 
SELECT * FROM raw_customer_data
WHERE is_valid = 1;
-- 3. Run Quality Checks on Staging (The Safety Net)
-- If this fails, we stop here. Production is untouched.
IF (SELECT COUNT(*) FROM dim_customers_staging) < 1000
    THROW 51000, 'Rollback: Too few customers in staging.', 1;
-- 4. The Atomic Swap (The Switch)
BEGIN TRANSACTION;
    -- Rename current Prod to Backup (for easy rollback)
    ALTER TABLE dim_customers RENAME TO dim_customers_backup_20231027;
    
    -- Rename Staging to Prod (Live instantly)
    ALTER TABLE dim_customers_staging RENAME TO dim_customers;
COMMIT TRANSACTION;
-- Emergency Rollback Script (Save this!):
-- ALTER TABLE dim_customers RENAME TO dim_customers_failed;
-- ALTER TABLE dim_customers_backup_20231027 RENAME TO dim_customers;


-- 2. Deterministic Logging: Because “It Worked on My Machine” Isn’t Enough
/* Scenario: We are running the nightly 'Sales_Load' job.
   We need to link every inserted row back to this specific execution.
*/

-- 1. Start the Job and Generate a Run ID
INSERT INTO job_history (job_name, status, start_time)
VALUES ('Sales_Load', 'RUNNING', CURRENT_TIMESTAMP);
-- Capture the generated ID (Syntax varies by DB, e.g., SCOPE_IDENTITY() or RETURNING)
DECLARE @RunID INT = SCOPE_IDENTITY(); 
-- 2. Perform the Transformation with the Watermark
INSERT INTO fact_sales (
    sale_id, 
    amount, 
    customer_id, 
    etl_run_id,      -- The Golden Key
    etl_loaded_at
)
SELECT 
    s.id, 
    s.amount, 
    s.cust_id, 
    @RunID,          -- Stamp every row
    CURRENT_TIMESTAMP
FROM raw_staging_sales s;
-- 3. Close the Job Log
UPDATE job_history
SET 
    status = 'SUCCESS', 
    end_time = CURRENT_TIMESTAMP,
    rows_affected = @@ROWCOUNT
WHERE run_id = @RunID;


-- 3. Backpressure Handling: When Pipeline Can’t Keep Up
/* Scenario: We need to archive 10 million old records from 'logs'.
   Running a single DELETE for 10M rows will lock the table.
   We use a WHILE loop to delete 5,000 rows at a time.
*/

DECLARE @BatchSize INT = 5000;
DECLARE @RowsAffected INT = 1;
-- Loop until no more rows need processing
WHILE @RowsAffected > 0
BEGIN
    BEGIN TRANSACTION;
    -- Delete a small chunk
    DELETE TOP (@BatchSize) 
    FROM logs
    WHERE event_date < DATEADD(year, -1, GETDATE());
    -- Capture how many rows we just touched
    SET @RowsAffected = @@ROWCOUNT;
    COMMIT TRANSACTION;
    -- Optional: Artificial delay to let other queries run (Backpressure relief)
    -- WAITFOR DELAY '00:00:01'; 
    
    -- Checkpoint/Log progress
    PRINT CONCAT('Archived batch of ', @BatchSize, ' rows.');
END

  
-- 4. Data Quality Gates: Fail Fast, Fix Faster
/* Scenario: Before loading the 'fact_revenue' table, we assert that:
   1. We are not loading more than 1% NULL customer_ids.
   2. Total revenue is not zero (unless it is a known holiday).
*/

DECLARE @TotalRows INT;
DECLARE @NullCustomerCount INT;
DECLARE @TotalRevenue DECIMAL(18,2);
-- 1. Gather Metrics from Staging
SELECT 
    @TotalRows = COUNT(*),
    @NullCustomerCount = SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END),
    @TotalRevenue = SUM(amount)
FROM staging_revenue;
-- 2. The Quality Gate Logic
IF (@TotalRows = 0)
BEGIN
    THROW 51000, 'CRITICAL: Staging table is empty. Aborting load.', 1;
END
IF ((CAST(@NullCustomerCount AS FLOAT) / @TotalRows) > 0.01)
BEGIN
    -- Fail if NULL rate exceeds 1%
    THROW 51000, 'QUALITY FAIL: Null Customer ID rate exceeds 1%. Aborting.', 1;
END
IF (@TotalRevenue = 0)
BEGIN
    THROW 51000, 'LOGIC FAIL: Total Revenue is 0. Suspicious data.', 1;
END
-- 3. If we survive the gates, proceed to Load
INSERT INTO fact_revenue SELECT * FROM staging_revenue;
PRINT 'SUCCESS: Data passed all quality gates and was loaded.';


-- 5. Dependency Management: The Domino Effect You Can’t Ignore
/* Scenario: We calculate 'daily_revenue' but ONLY if 'raw_orders' 
   has data for the current date. If not, we abort/skip to prevent 
   reporting a partial or zero revenue day.
*/

BEGIN TRANSACTION;
-- 1. Dependency Check (The Guardrail)
-- If the count is 0, this variable forces the subsequent logic to skip.
DECLARE @SourceHasData INT;
SET @SourceHasData = (
    SELECT COUNT(*) 
    FROM raw_orders 
    WHERE order_date = CURRENT_DATE
);
-- 2. Conditional Execution
IF @SourceHasData > 0
BEGIN
    -- Safe to run transformation
    DELETE FROM daily_revenue WHERE report_date = CURRENT_DATE;
    INSERT INTO daily_revenue (report_date, total_revenue)
    SELECT 
        order_date, 
        SUM(order_amount)
    FROM raw_orders
    WHERE order_date = CURRENT_DATE
    GROUP BY order_date;
    
    PRINT 'SUCCESS: Daily revenue calculated.';
END
ELSE
BEGIN
    -- Fail loudly or log the delay
    PRINT 'WARNING: Upstream dependency (raw_orders) is empty for today. Job skipped.';
    -- Optional: THROW 50000, 'Upstream Data Missing', 1;
END
COMMIT TRANSACTION;


-- 6. Immutable Data: If It’s Logged, Don’t Modify It
/* Scenario: Raw logs have inconsistent state codes ('CA', 'cali', 'Calif'). 
   We DO NOT update the 'raw_logs' table. 
   We create a standardized view for downstream users.
*/

CREATE OR REPLACE VIEW v_clean_user_events AS
SELECT 
    event_id,
    user_id,
    raw_timestamp,
    -- Transformation logic lives here, not in the storage layer
    CASE 
        WHEN state_code IN ('CA', 'cali', 'Calif', 'Calliforna') THEN 'CA'
        WHEN state_code = 'NY' THEN 'NY'
        ELSE COALESCE(state_code, 'UNKNOWN')
    END AS clean_state_code,
    
    -- Preserve the original data for audit
    state_code AS original_raw_state_code
FROM raw_logs; -- This table is NEVER updated, only appended to.


-- 7. Partitioning: The Secret to Scaling Without Pain
/* Scenario: We store billions of website hits.
Most analysts only query the last 7 days.
We partition the table by 'event_date'.
*/

-- 1. DDL: Create the table with a Partition Strategy
CREATE TABLE web_logs (
    log_id STRING,
    user_id INT,
    url_path STRING,
    status_code INT,
    event_timestamp TIMESTAMP,
    event_date DATE  -- This is our Partition Key
)
PARTITION BY (event_date); 
-- In BigQuery/Spark, this physically separates files by date.
-- 2. The Optimized Query (Partition Pruning in Action)
SELECT 
    url_path, 
    COUNT(*) as hits
FROM web_logs
WHERE event_date >= '2023-10-01' AND event_date <= '2023-10-07'
-- The DB engine reads ONLY the folders for these 7 days.
-- It strictly IGNORES the years of historical data also in the table.
GROUP BY 1;


-- 8. Incremental Processing: Don’t Reinvent the Wheel
/* Scenario: We update our Analytics table every 15 minutes.
   We store the last run time in a metadata table called 'etl_watermarks'.
*/

-- 1. Get the High-Water Mark (the last time this job ran successfully)
DECLARE @LastRunTime TIMESTAMP;
SET @LastRunTime = (
    SELECT last_loaded_at 
    FROM etl_watermarks 
    WHERE table_name = 'sales_analytics'
);
-- 2. Perform the Incremental Load (Upsert logic)
MERGE INTO sales_analytics AS target
USING (
    SELECT * FROM raw_sales_source
    -- CRITICAL: Only pull data newer than the watermark
    WHERE updated_at > @LastRunTime 
) AS source
ON target.sale_id = source.sale_id
WHEN MATCHED THEN
    UPDATE SET 
        target.amount = source.amount,
        target.status = source.status,
        target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
    INSERT (sale_id, amount, status, created_at, updated_at)
    VALUES (source.sale_id, source.amount, source.status, source.created_at, source.updated_at);
-- 3. Update the Watermark for the next run
UPDATE etl_watermarks
SET last_loaded_at = CURRENT_TIMESTAMP
WHERE table_name = 'sales_analytics';


-- 9. Idempotent Transformations: Write Once, Run Forever
/* Scenario: We are loading daily sales data. 
   If a transaction ID already exists, update the status (e.g., Pending -> Completed).
   If it is new, insert it.
*/

MERGE INTO fact_sales AS target
USING staging_sales AS source
    ON target.transaction_id = source.transaction_id
    AND target.sale_date = source.sale_date -- Optimization for partitioned tables
-- 1. When the record exists, update it (Idempotent: Result is always the latest state)
WHEN MATCHED THEN
    UPDATE SET 
        target.amount = source.amount,
        target.status = source.status,
        target.updated_at = CURRENT_TIMESTAMP
-- 2. When the record is new, insert it
WHEN NOT MATCHED THEN
    INSERT (transaction_id, sale_date, customer_id, amount, status, created_at)
    VALUES (source.transaction_id, source.sale_date, source.customer_id, source.amount, source.status, CURRENT_TIMESTAMP);


-- 10. Schema Validation Before Run: Silent Killer of Pipelines
/* Scenario: We are moving data from 'raw_staging' to 'prod_transactions'.
 We need to ensure dates are valid dates and amounts are valid numbers.
*/

-- 1. Identify and Insert Bad Records into Quarantine
INSERT INTO quarantine_transactions (user_id, raw_date, raw_amount, error_reason, ingestion_time)
SELECT 
    user_id, 
    event_date, 
    amount,
    CASE 
        WHEN user_id IS NULL THEN 'CRITICAL: Missing Primary Key'
        WHEN TRY_CAST(event_date AS DATE) IS NULL THEN 'ERROR: Invalid Date Format'
        WHEN TRY_CAST(amount AS DECIMAL(18,2)) IS NULL THEN 'ERROR: Non-Numeric Amount'
        WHEN TRY_CAST(amount AS DECIMAL(18,2)) < 0 THEN 'LOGIC: Negative Amount'
    END AS error_reason,
    CURRENT_TIMESTAMP
FROM raw_staging
WHERE 
    user_id IS NULL 
    OR TRY_CAST(event_date AS DATE) IS NULL 
    OR TRY_CAST(amount AS DECIMAL(18,2)) IS NULL
    OR TRY_CAST(amount AS DECIMAL(18,2)) < 0;
-- 2. Insert Only Clean Data into Production
INSERT INTO prod_transactions (user_id, event_date, amount)
SELECT 
    user_id, 
    CAST(event_date AS DATE), 
    CAST(amount AS DECIMAL(18,2))
FROM raw_staging
WHERE 
    user_id IS NOT NULL 
    AND TRY_CAST(event_date AS DATE) IS NOT NULL 
    AND TRY_CAST(amount AS DECIMAL(18,2)) IS NOT NULL
    AND TRY_CAST(amount AS DECIMAL(18,2)) >= 0;
