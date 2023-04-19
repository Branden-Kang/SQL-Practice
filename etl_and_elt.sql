-- Reference: https://medium.com/@abdelilah.moulida/sql-and-data-integration-etl-and-elt-e693ca0544ce
-- Extracting Data from Multiple Sources
SELECT * FROM customers WHERE country = 'USA';

LOAD DATA INFILE '/branden/file.csv'
INTO TABLE customers
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n';

-- import requests
-- response = requests.get('https://api.example.com/endpoint')
-- data=response.json()
-- print(data

-- Transforming Data using SQL Queries
SELECT LOWER(name) AS lower_name FROM customers;
SELECT name AS full_name FROM customers;
SELECT * FROM customers
UNION ALL
SELECT * FROM orders;

INSERT INTO customers (name, email, country)
VALUES ('John Doe', 'johndoe@example.com', 'USA');

-- Loading Data into a Destination Database or Data Warehouse
UPDATE customers
SET email = 'john.smith@example.com'
WHERE name = 'John Smith';
