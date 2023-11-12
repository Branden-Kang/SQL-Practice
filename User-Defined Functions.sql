-- Create a simple function that returns the sum of two integers
CREATE OR REPLACE FUNCTION add_two_numbers(a INT, b INT) RETURNS INT AS $$
BEGIN
  RETURN a + b;
END;
$$ LANGUAGE plpgsql;

-- Call the function
SELECT add_two_numbers(5, 7); -- Returns 12

-- Create a table-valued function that returns all employees in a department
CREATE OR REPLACE FUNCTION get_employees_in_department(dept_id INT) RETURNS TABLE(id INT, name TEXT) AS $$
BEGIN
  RETURN QUERY SELECT employee_id, employee_name FROM employees WHERE department_id = dept_id;
END;
$$ LANGUAGE plpgsql;

-- Call the function
SELECT * FROM get_employees_in_department(101); -- Returns all employees in department 101

-- Create a trigger function to update the last_modified timestamp on table updates
CREATE OR REPLACE FUNCTION update_last_modified() RETURNS TRIGGER AS $$
BEGIN
  NEW.last_modified := NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create a trigger that uses the function
CREATE TRIGGER update_last_modified_trigger
BEFORE UPDATE ON your_table
FOR EACH ROW
EXECUTE FUNCTION update_last_modified();

-- Create an aggregation function that calculates the median
CREATE OR REPLACE FUNCTION median_accumulator(sfunc INTERNAL, stype INTERNAL, finalfunc INTERNAL)
RETURNS internal STRICT
LANGUAGE C;

-- Define the final function
CREATE OR REPLACE FUNCTION median_finalfunc(internal) RETURNS double precision LANGUAGE sql IMMUTABLE STRICT AS $$
  SELECT CASE
    WHEN $1 IS NULL THEN NULL
    ELSE (percentile_cont(0.5) WITHIN GROUP (ORDER BY $1))
  END;
$$;

-- Create an aggregate that uses the function
CREATE AGGREGATE median(double precision) (
  SFUNC = median_accumulator,
  STYPE = internal,
  FINALFUNC = median_finalfunc
);
