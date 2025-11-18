SELECT c.customer_id
FROM customers c
LEFT ANTI JOIN orders o
    ON c.customer_id = o.customer_id;

-- OR

SELECT c.customer_id
FROM customers c
LEFT JOIN orders o 
    ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;

SELECT t1.id
FROM events t1
LEFT ANTI JOIN events t2
    ON t1.id = t2.id
   AND t2.timestamp > t1.timestamp;

-- Or

SELECT t1.id
FROM events t1
LEFT JOIN events t2
    ON t1.id = t2.id
   AND t2.timestamp > t1.timestamp
WHERE t2.id IS NULL;

SELECT s.source_key
FROM stage_table s
LEFT ANTI JOIN final_table f
    ON s.source_key = f.source_key;

-- Or

SELECT s.source_key
FROM stage_table s
LEFT JOIN final_table f
    ON s.source_key = f.source_key
WHERE f.source_key IS NULL;
