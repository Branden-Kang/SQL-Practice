-- 1. Reading and Maintaining the Query

-- CTE
WITH active_goals AS (
    SELECT 
        contact_id, 
        COUNT(*) AS total_goals
    FROM goals
    WHERE status = 1
    GROUP BY contact_id
)
SELECT 
    c.name, 
    c.surname, 
    ag.total_goals
FROM contacts c
JOIN active_goals ag ON c.id = ag.contact_id
WHERE ag.total_goals > 2;


-- Subquery
SELECT 
    c.name, 
    c.surname, 
    (
        SELECT COUNT(*) 
        FROM goals g 
        WHERE g.contact_id = c.id AND g.status = 1
    ) AS total_goals
FROM contacts c
WHERE (
        SELECT COUNT(*) 
        FROM goals g 
        WHERE g.contact_id = c.id AND g.status = 1
    ) > 2;

-- 2. Query Reuse
-- CTE
WITH active_habits AS (
    SELECT 
        contact_id, 
        COUNT(*) AS habit_count
    FROM habits
    WHERE status = 1
    GROUP BY contact_id
)
SELECT 
    c.name, 
    c.surname
FROM contacts c
JOIN active_habits ah ON c.id = ah.contact_id
WHERE ah.habit_count > 2

UNION

SELECT 
    c.name, 
    c.surname
FROM contacts c
JOIN active_habits ah ON c.id = ah.contact_id
WHERE ah.habit_count < 0;


-- Subquery
SELECT 
    c.name, 
    c.surname
FROM contacts c
WHERE (
        SELECT COUNT(*) 
        FROM habits h 
        WHERE h.contact_id = c.id AND h.status = 1
    ) > 2

UNION

SELECT 
    c.name, 
    c.surname
FROM contacts c
WHERE (
        SELECT COUNT(*) 
        FROM habits h 
        WHERE h.contact_id = c.id AND h.status = 1
    ) < 0;

-- 3. Recursive Query
WITH goal_hierarchy AS (
    -- Select the top level goals
    SELECT id, name, description, contact_id, parent_goal_id, 1 AS level
    FROM goals
    WHERE contact_id = 1 AND parent_goal_id IS NULL

    UNION ALL

    -- Select subgoals and increment the level
    SELECT g.id, g.name, g.description, g.contact_id, g.parent_goal_id, cte.level + 1
    FROM goals g
    JOIN goal_hierarchy cte ON g.parent_goal_id = cte.id
)
-- Get all goals and subgoals
SELECT 
 id, 
 name,
 description, 
 level
FROM goal_hierarchy
ORDER BY level, id;
