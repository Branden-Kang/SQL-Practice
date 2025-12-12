-- 1. Cross-Correlate Multiple Time-Series in One Query
WITH t1 AS (
    SELECT * FROM transactions
),
t2 AS (
    SELECT * FROM support_events
),
-- For each support event, find the nearest earlier transaction
aligned AS (
    SELECT
        t2.event_id,
        t2.timestamp AS support_ts,
        t1.trans_id,
        t1.timestamp AS trans_ts,
        -- Only consider transactions before the support event
        ROW_NUMBER() OVER (
            PARTITION BY t2.event_id
            ORDER BY t1.timestamp DESC
        ) AS rn
    FROM t2
    LEFT JOIN t1
        ON t1.timestamp <= t2.timestamp
)
SELECT
    event_id,
    support_ts,
    trans_id,
    trans_ts,
    
    -- Time delta for cross-correlation features
    EXTRACT(EPOCH FROM (support_ts - trans_ts)) AS seconds_lag
FROM aligned
WHERE rn = 1
ORDER BY support_ts;

WITH paired AS (
    SELECT
        a.id AS id_a,
        b.id AS id_b,
        a.timestamp AS ts_a,
        b.timestamp AS ts_b,
ABS(EXTRACT(EPOCH FROM (b.timestamp - a.timestamp))) AS time_diff,
        ROW_NUMBER() OVER (
            PARTITION BY a.id
            ORDER BY ABS(EXTRACT(EPOCH FROM (b.timestamp - a.timestamp)))
        ) AS rn
    FROM stream_a a
    LEFT JOIN stream_b b
        ON b.timestamp BETWEEN a.timestamp - INTERVAL '30 seconds'
                         AND a.timestamp + INTERVAL '30 seconds'
)
SELECT *
FROM paired
WHERE rn = 1
ORDER BY ts_a;

-- 2. Pre-Aggregate High-Frequency Data for ML

WITH buckets AS (
    SELECT
        device_id,
-- Create fixed time windows (10 seconds)
        DATE_TRUNC('second', timestamp) 
           + INTERVAL '10 seconds' * FLOOR(EXTRACT(EPOCH FROM timestamp) / 10) 
           AS window_ts,
        value
    FROM gpu_logs
),
agg AS (
    SELECT
        device_id,
        window_ts,
        -- Preserve max spike
        MAX(value) AS max_value,
        -- Preserve extreme but robust metric
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY value) AS p99_value,
        -- Capture distribution bucket (e.g., temp ranges)
        WIDTH_BUCKET(value, 30, 110, 8) AS temp_bucket
    FROM buckets
    GROUP BY device_id, window_ts, temp_bucket
)
SELECT
    device_id,
    window_ts,
    max_value,
    p99_value,
    temp_bucket
FROM agg
ORDER BY device_id, window_ts;

-- 3. Encode Cyclical Time Features Correctly
SELECT
    timestamp,
-- Hour of day (0-23)
    EXTRACT(HOUR FROM timestamp) AS hour,
    SIN(2 * PI() * EXTRACT(HOUR FROM timestamp) / 24.0) AS hour_sin,
    COS(2 * PI() * EXTRACT(HOUR FROM timestamp) / 24.0) AS hour_cos,
    -- Day of week (0-6)
    EXTRACT(DOW FROM timestamp) AS dow,
    SIN(2 * PI() * EXTRACT(DOW FROM timestamp) / 7.0) AS dow_sin,
    COS(2 * PI() * EXTRACT(DOW FROM timestamp) / 7.0) AS dow_cos,
    -- Month of year (1-12)
    EXTRACT(MONTH FROM timestamp) AS month,
    SIN(2 * PI() * EXTRACT(MONTH FROM timestamp) / 12.0) AS month_sin,
    COS(2 * PI() * EXTRACT(MONTH FROM timestamp) / 12.0) AS month_cos
FROM events;

-- 4. Track State Persistence with Sessionization
WITH annotated AS (
    SELECT
        machine_id,
        timestamp,
        status,

-- Identify start of a new "error" session
        CASE 
            WHEN status = 'error' 
                 AND LAG(status) OVER (
                    PARTITION BY machine_id ORDER BY timestamp
                 ) <> 'error'
            THEN 1
            ELSE 0
        END AS session_start_flag
),
session_ids AS (
    SELECT
        machine_id,
        timestamp,
        status,
        -- Cumulative sum creates a unique session_id per contiguous error block
        SUM(session_start_flag) OVER (
            PARTITION BY machine_id
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_id
    FROM annotated
),
error_sessions AS (
    SELECT
        machine_id,
        timestamp,
        status,
        session_id,
        -- Compute session-level duration
        MIN(timestamp) OVER (
            PARTITION BY machine_id, session_id
        ) AS session_start,
        MAX(timestamp) OVER (
            PARTITION BY machine_id, session_id
        ) AS session_end
    FROM session_ids
    WHERE status = 'error'
)
SELECT
    machine_id,
    timestamp,
    status,
    session_start,
    session_end,
    EXTRACT(EPOCH FROM (session_end - session_start)) AS seconds_in_error
FROM error_sessions
ORDER BY machine_id, timestamp;


-- 5. Create Seasonality-Adjusted Trends
WITH baseline AS (
    SELECT
        timestamp,
        value,
        EXTRACT(DOW FROM timestamp) AS dow,

-- compute the average for each day-of-week
        AVG(value) OVER (
            PARTITION BY EXTRACT(DOW FROM timestamp)
        ) AS dow_mean,
        -- compute the median (optional but more robust)
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) OVER (
            PARTITION BY EXTRACT(DOW FROM timestamp)
        ) AS dow_median
)
SELECT
    timestamp,
    value,
    dow_mean,
    dow_median,
    -- seasonality-adjusted trend (mean-based)
    value - dow_mean AS detrended_value,
    -- robust version (median-based)
    value - dow_median AS robust_detrended_value
FROM baseline
ORDER BY timestamp;

WITH enriched AS (
    SELECT
        timestamp,
        value,
        EXTRACT(DOW FROM timestamp) AS dow,
        EXTRACT(MONTH FROM timestamp) AS mon,

AVG(value) OVER (
            PARTITION BY EXTRACT(DOW FROM timestamp)
        ) AS dow_avg,
        AVG(value) OVER (
            PARTITION BY EXTRACT(MONTH FROM timestamp)
        ) AS mon_avg
)
SELECT
    timestamp,
    value,
    -- Remove both weekly + monthly cycles
    value - (dow_avg + mon_avg) / 2 AS seasonality_adjusted
FROM enriched
ORDER BY timestamp;

-- 6. Smooth Noisy Data with Exponential Weighting
WITH RECURSIVE ema_calc AS (
    -- Seed with the earliest timestamp
    SELECT
        device_id,
        timestamp,
        value,
        value::float AS ema  -- initial EMA
    FROM sensor_data
    WHERE timestamp = (SELECT MIN(timestamp) FROM sensor_data)

UNION ALL
    -- Apply exponential weighting forward in time
    SELECT
        sd.device_id,
        sd.timestamp,
        sd.value,
        (alpha * sd.value + (1 - alpha) * ec.ema) AS ema
    FROM sensor_data sd
    JOIN ema_calc ec
        ON sd.device_id = ec.device_id
       AND sd.timestamp = (
             SELECT MIN(timestamp)
             FROM sensor_data
             WHERE timestamp > ec.timestamp
         )
    CROSS JOIN (SELECT 0.1 AS alpha) p  -- decay factor
)
SELECT *
FROM ema_calc
ORDER BY timestamp;


-- 7. Build Event-Lagged Features Without Self-Joins
WITH enriched AS (
    SELECT
        device_id,
        timestamp,
        event_flag,  -- 1 when an event occurs, else 0

-- Last event timestamp (efficient reverse scan)
        LAST_VALUE(
            CASE WHEN event_flag = 1 THEN timestamp END
        ) IGNORE NULLS OVER (
            PARTITION BY device_id
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS last_event_ts,
        -- Next event timestamp (forward scan)
        LEAD(
            CASE WHEN event_flag = 1 THEN timestamp END
        ) IGNORE NULLS OVER (
            PARTITION BY device_id
            ORDER BY timestamp
        ) AS next_event_ts
    FROM event_stream
)
SELECT
    device_id,
    timestamp,
    event_flag,
    EXTRACT(EPOCH FROM (timestamp - last_event_ts)) AS seconds_since_last_event,
    EXTRACT(EPOCH FROM (next_event_ts - timestamp)) AS seconds_until_next_event
FROM enriched
ORDER BY device_id, timestamp;


-- 8. Detect Anomalies with Z-Scores in Pure SQL
WITH stats AS (
    SELECT
        device_id,
        AVG(value) AS mean_val,
        STDDEV(value) AS std_val
    FROM sensor_data
    GROUP BY device_id
),
tagged AS (
    SELECT
        sd.device_id,
        sd.timestamp,
        sd.value,
        s.mean_val,
        s.std_val,

-- Global Z-score
        (sd.value - s.mean_val) / NULLIF(s.std_val, 0) AS zscore,
        -- Rolling Z-score (to avoid flagging stale anomalies)
        (sd.value - AVG(sd.value) OVER (
            PARTITION BY sd.device_id
            ORDER BY sd.timestamp
            ROWS BETWEEN 60 PRECEDING AND CURRENT ROW
        ))
        /
        NULLIF(
            STDDEV(sd.value) OVER (
                PARTITION BY sd.device_id
                ORDER BY sd.timestamp
                ROWS BETWEEN 60 PRECEDING AND CURRENT ROW
            ),
            0
        ) AS rolling_zscore
    FROM sensor_data sd
    JOIN stats s
        ON sd.device_id = s.device_id
)
SELECT
    device_id,
    timestamp,
    value,
    CASE WHEN ABS(zscore) > 3 THEN 1 ELSE 0 END AS global_outlier,
    CASE WHEN ABS(rolling_zscore) > 3 THEN 1 ELSE 0 END AS rolling_outlier
FROM tagged
ORDER BY device_id, timestamp;

-- 9. Impute Missing Timestamps with Time Bucketing
WITH grid AS (
    -- Create 5-minute buckets over required range
    SELECT 
        generate_series(
            (SELECT MIN(timestamp) FROM sensor_data),
            (SELECT MAX(timestamp) FROM sensor_data),
            INTERVAL '5 minutes'
        ) AS bucket_ts
),
aligned AS (
    -- Snap each event into its bucket
    SELECT
        g.bucket_ts,
        sd.device_id,
        sd.value,
        ROW_NUMBER() OVER (
            PARTITION BY sd.device_id, g.bucket_ts
            ORDER BY sd.timestamp DESC
        ) AS rn
    FROM grid g
    LEFT JOIN sensor_data sd
        ON sd.timestamp >= g.bucket_ts
       AND sd.timestamp <  g.bucket_ts + INTERVAL '5 minutes'
)
SELECT
    device_id,
    bucket_ts AS timestamp,
    -- Forward-fill last known value
    LAST_VALUE(value IGNORE NULLS) OVER (
        PARTITION BY device_id ORDER BY bucket_ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS imputed_value
FROM aligned
WHERE rn = 1
ORDER BY device_id, bucket_ts;

10. Use Window Functions to Calculate Rolling Volatility
SELECT
  device_id,
  timestamp,
  value,
  
  -- 30-window rolling volatility
  STDDEV(value) OVER (
      PARTITION BY device_id 
      ORDER BY timestamp 
      ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
  ) AS rolling_volatility,

-- Compare current volatility to 1-hour-old baseline
  LAG(
      STDDEV(value) OVER (
          PARTITION BY device_id 
          ORDER BY timestamp 
          ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
      )
  , 60) OVER (
      PARTITION BY device_id ORDER BY timestamp
  ) AS baseline_volatility,
  CASE 
    WHEN STDDEV(value) OVER (
            PARTITION BY device_id 
            ORDER BY timestamp 
            ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
         )
         >
         LAG(
             STDDEV(value) OVER (
                 PARTITION BY device_id 
                 ORDER BY timestamp 
                 ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
             )
         , 60) OVER (PARTITION BY device_id ORDER BY timestamp)
    THEN 'volatility_up'
    ELSE 'stable'
  END AS regime_shift_flag
FROM sensor_stream;
