WITH RECURSIVE gift_combos AS (
    -- Anchor: start with each base item
    SELECT item_id, item_name, 1 AS depth, item_name AS combo
    FROM products

UNION ALL
    -- Recursive member: build combinations
    SELECT g.item_id, p.item_name, g.depth + 1,
           g.combo || ', ' || p.item_name
    FROM gift_combos g
    JOIN products p ON g.depth < 3
)
SELECT * FROM gift_combos;

-- generate a million test rows
WITH RECURSIVE numbers AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM numbers WHERE n < 1000000
)
SELECT * FROM numbers;

-- build a ten-year calendar dimension
WITH RECURSIVE calendar AS (
    SELECT DATE '2020-01-01' AS dt
    UNION ALL
    SELECT dt + INTERVAL '1 day' FROM calendar WHERE dt < DATE '2029-12-31'
)
SELECT dt,
       EXTRACT(YEAR FROM dt) AS year,
       EXTRACT(QUARTER FROM dt) AS quarter,
       CASE WHEN EXTRACT(DOW FROM dt) IN (6,0) THEN 'Weekend' ELSE 'Weekday' END AS day_type
FROM calendar;

-- Finding the Shortest Delivery Route

WITH RECURSIVE route_paths AS (
    -- Anchor: start from the source hub
    SELECT 
        start_hub AS current_hub,
        ARRAY[start_hub] AS path,
        0 AS total_cost
    FROM routes
    WHERE start_hub = 'A'

UNION ALL
    -- Recursive member: explore connected hubs
    SELECT 
        r.end_hub AS current_hub,
        path || r.end_hub,
        total_cost + r.cost
    FROM route_paths p
    JOIN routes r 
        ON p.current_hub = r.start_hub
    WHERE NOT r.end_hub = ANY(p.path) -- prevent cycles
)
SELECT *
FROM route_paths
WHERE current_hub = 'Z'
ORDER BY total_cost
LIMIT 1;

-- Sessionizing Clickstream Events in Pure SQL
WITH RECURSIVE sessions AS (
    -- Anchor: first event per user
    SELECT 
        user_id,
        event_time,
        event_time AS session_start,
        1 AS session_id
    FROM clickstream c
    WHERE NOT EXISTS (
        SELECT 1 FROM clickstream 
        WHERE user_id = c.user_id 
          AND event_time < c.event_time
    )

UNION ALL
    -- Recursive step: next event for the same user
    SELECT 
        c.user_id,
        c.event_time,
        CASE 
            WHEN c.event_time - s.event_time > INTERVAL '30 minutes' 
            THEN c.event_time     -- new session
            ELSE s.session_start  -- same session
        END AS session_start,
        CASE 
            WHEN c.event_time - s.event_time > INTERVAL '30 minutes' 
            THEN s.session_id + 1
            ELSE s.session_id
        END AS session_id
    FROM sessions s
    JOIN clickstream c 
      ON c.user_id = s.user_id 
     AND c.event_time > s.event_time
     AND c.event_time < s.event_time + INTERVAL '1 hour'
)
SELECT 
    user_id,
    session_id,
    MIN(session_start) AS session_start,
    MAX(event_time) AS session_end,
    COUNT(*) AS events_in_session,
    EXTRACT(EPOCH FROM MAX(event_time) - MIN(session_start))/60 AS session_duration_min
FROM sessions
GROUP BY user_id, session_id
ORDER BY user_id, session_start;

WITH RECURSIVE numbers AS (
    SELECT 1 AS num           -- Anchor member
    UNION ALL
    SELECT num + 1            -- Recursive member
    FROM numbers
    WHERE num < 100           -- Termination condition
)
SELECT * FROM numbers;
