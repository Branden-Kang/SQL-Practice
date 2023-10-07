CREATE TABLE exp_window_function (
  name varchar,
  age integer,
  department varchar,
  salary integer);

INSERT INTO exp_window_function VALUES
('ramesh', 20, 'finance', 50000),
('deep', 24, 'sales', 30000),
('suresh', 22, 'finance', 50000),
('ram', 28, 'finance', 20000),
('pradeep', 22, 'sales', 20000);

-- Query 1: Showing rows with department average salary
SELECT department, AVG(salary) FROM exp_window_function GROUP BY department;

SELECT
  *,
  AVG(salary) OVER (PARTITION BY department) as avg_salary
FROM exp_window_function;

-- Query 2: From Query1’s output, let’s sort by department and salary
SELECT
  *,
  AVG(salary) OVER (PARTITION BY department) as avg_salary
FROM exp_window_function
ORDER BY department, salary;

-- Query 3: Showing rows only finance department + average salary
SELECT
  *,
  AVG(salary) OVER (PARTITION BY department) as avg_salary
FROM exp_window_function
WHERE department='finance';

-- Query 4: Showing rows with ALL employees’ average salary
SELECT
  *,
  AVG(salary) OVER () as avg_salary
FROM exp_window_function;
