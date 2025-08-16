-- Regular CTEs
WITH SimpleNumbers AS (
  SELECT 1 AS Number
  UNION ALL
  SELECT 2
  UNION ALL
  SELECT 3
)
SELECT * FROM SimpleNumbers
;

-- Recursive CTEs
WITH RECURSIVE cte_name AS (
  -- Anchor (start)
  SELECT base_value

  UNION ALL

  -- Recursive part (builds on previous output)
  SELECT next_value FROM cte_name WHERE condition
)
SELECT * FROM cte_name
;

WITH RECURSIVE CountNumbers AS (
  SELECT 1 AS Number
  UNION ALL
  SELECT Number + 1 FROM CountNumbers WHERE Number < 5
)
SELECT * FROM CountNumbers
;

-- Building a Safe Org Chart
WITH RECURSIVE Descendants AS (
  SELECT
    name,
    manager_name,
    1 AS generation,
    name AS path
  FROM employees
  WHERE name = 'Alice'

  UNION ALL

  SELECT
    e.name,
    e.manager_name,
    d.generation + 1,
    d.path || '>' || e.name
  FROM employees e
  JOIN Descendants d ON e.manager_name = d.name
  WHERE POSITION('>' || e.name || '>' IN '>' || d.path || '>') = 0
)
SELECT name, manager_name, generation, path FROM Descendants
;
