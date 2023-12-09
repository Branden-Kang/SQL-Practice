-- Sample table with 1 million rows
CREATE TABLE large_table (
  id INT,
  name VARCHAR(50), 
  address VARCHAR(100)
);

-- Insert 1 million rows 
INSERT INTO large_table 
SELECT id, 
       concat('Name ', id),
       concat('Address ', id)
FROM generate_series(1, 1000000) as id;

-- Query without index
EXPLAIN ANALYZE 
SELECT * 
FROM large_table
WHERE id = 500000;

-- Add index on id column
CREATE INDEX idx_id ON large_table (id);

-- Query with index
EXPLAIN ANALYZE  
SELECT *
FROM large_table
WHERE id = 500000;
