-- Creating a Time-Series Table
CREATE TABLE temperature_data (
    sensor_id INT,
    timestamp TIMESTAMP,
    temperature NUMERIC(5, 2)
);
-- Basic Time-Series Queries
SELECT * FROM temperature_data
WHERE sensor_id = 1
AND timestamp >= '2023-01-01' AND timestamp < '2023-01-02';
-- Aggregating Time-Series Data
SELECT
    DATE(timestamp) AS date,
    AVG(temperature) AS avg_temperature
FROM temperature_data
WHERE sensor_id = 1
GROUP BY DATE(timestamp)
ORDER BY date;
-- Time-Series Gap Filling 
SELECT
    generate_series(MIN(timestamp), MAX(timestamp), '1 hour') AS timestamp,
    sensor_id,
    COALESCE(temperature, LAG(temperature) OVER (PARTITION BY sensor_id ORDER BY timestamp)) AS temperature
FROM temperature_data
WHERE sensor_id = 1
-- Calculating Moving Averages
SELECT
    timestamp,
    sensor_id,
    temperature,
    AVG(temperature) OVER (PARTITION BY sensor_id ORDER BY timestamp ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS moving_average
FROM temperature_data
WHERE sensor_id = 1
