-- Thanks to https://towardsdatascience.com/5-sql-tips-to-make-your-queries-prettier-and-easier-to-read-d9e3a543514f

1. case sensitive
SELECT firstName, count(*) from Users WHERE last_name = ‘smith’ Group By firstName

-- ->>

SELECT first_name, COUNT(*) FROM users WHERE last_name = ‘smith’ GROUP BY first_name

-- 2. indentation
SELECT g.id, COUNT(u.id) FROM users u JOIN groups g on u.group_id = g.id WHERE u.name = 'John' GROUP BY g.id ORDER BY COUNT(u.id) desc

-- -->
SELECT g.id, COUNT(u.id)
FROM users u JOIN groups g on u.group_id = g.id
WHERE u.name = 'John'
GROUP BY g.id
ORDER BY COUNT(u.id) desc

-- -->

SELECT
    g.id
  , COUNT(u.id)
FROM users u
    JOIN groups g on u.group_id = g.id
WHERE u.name = ‘John’
GROUP BY
    g.id
ORDER BY
    COUNT(u.id) desc
    
-- 3. Number
SELECT
    first_name
    , last_name
    , COUNT(*)
FROM users
GROUP BY
    first_name
    , last_name
ORDER BY
    COUNT(*) desc
    
-- -->    

SELECT
    first_name
    , last_name
    , COUNT(*)
FROM users
GROUP BY 1, 2
ORDER BY 3 desc

-- 4. Common Table Expressions
WITH employee_by_title_count AS (
    SELECT
        t.name as job_title
        , COUNT(e.id) as amount_of_employees
    FROM employees e
        JOIN job_titles t on e.job_title_id = t.id
    GROUP BY 1
),
salaries_by_title AS (
     SELECT
         name as job_title
         , salary
     FROM job_titles
)
SELECT *
FROM employee_by_title_count e
    JOIN salaries_by_title s ON s.job_title = e.job_title
    
-- 5. aliases
SELECT
    u.id
    , u.name
    , t.id
    , t.name
    , (SELECT COUNT(*) FROM job_titles where name = t.name)
FROM users u
    JOIN job_title t on u.job_title_id = t.id
    
-- -->

SELECT
    u.id as user_id
    , u.name as user_name
    , t.id as job_title_id
    , t.name as job_title_name
    , (SELECT COUNT(*) FROM job_titles where name = t.name) as count_users_with_job
FROM users u
    JOIN job_title t on u.job_title_id = t.id
