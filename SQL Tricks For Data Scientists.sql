--Thanks to https://towardsdatascience.com/sql-tricks-for-data-scientists-53298467dd5

-- Find a Repeating Event
SELECT a.first_name, a.last_name, f.title 
FROM actor a 
JOIN film_actor fa ON fa.actor_id = a.actor_id 
JOIN film f ON f.film_id = fa.film_id;

SELECT a.first_name, fa.film_id, f.title 
FROM actor a 
LEFT JOIN (
  SELECT actor_id, MAX(film_id) as film_id 
  FROM film_actor group by actor_id
) fa ON fa.actor_id = a.actor_id 
LEFT JOIN film f ON f.film_id = fa.film_id;

SELECT a.first_name, fa.film_id, f.title 
FROM actor a 
LEFT JOIN (
  SELECT actor_id, MAX(film_id) as film_id 
  FROM film_actor group by actor_id
) fa ON fa.actor_id = a.actor_id 
LEFT JOIN film f ON f.film_id = fa.film_id;

-- Pivoting
SELECT MONTHNAME(r.rental_date), c.name, count(r.rental_id) 
FROM rental r 
LEFT JOIN film f ON f.film_id = r.inventory_id 
LEFT JOIN film_category fc ON fc.film_id = f.film_id 
LEFT JOIN category c ON c.category_id = fc.category_id 
GROUP BY MONTHNAME(r.rental_date),c.name;

SELECT MONTHNAME(r.rental_date), 
COUNT(CASE WHEN c.name = ‘Horror’ THEN r.rental_id ELSE NULL END) AS HorrorCount, 
COUNT(CASE WHEN c.name = ‘Action’ THEN r.rental_id ELSE NULL END) AS ActionCount, 
COUNT(CASE WHEN c.name = ‘Comedy’ THEN r.rental_id ELSE NULL END) AS ComedyCount, 
COUNT(CASE WHEN c.name = ‘Sci-Fi’ THEN r.rental_id ELSE NULL END) AS ScifiCount 
FROM rental r 
LEFT JOIN film f ON f.film_id = r.inventory_id 
LEFT JOIN film_category fc ON fc.film_id = f.film_id 
LEFT JOIN category c ON c.category_id = fc.category_id 
GROUP BY MONTHNAME(r.rental_date);

SET @sql = NULL;
SELECT
 GROUP_CONCAT(DISTINCT
 CONCAT(
 ‘COUNT(CASE WHEN c.name = ‘’’,
 name,
 ‘’’ THEN r.rental_id ELSE NULL END) AS `’,
 name,
 ‘`’
 )
 ) INTO @sql
FROM category;
SET @sql = CONCAT(‘SELECT MONTHNAME(r.rental_date), ‘, @sql ,’ FROM rental r LEFT JOIN film f ON f.film_id = r.inventory_id LEFT JOIN film_category fc ON fc.film_id = f.film_id LEFT JOIN category c ON c.category_id = fc.category_id GROUP BY MONTHNAME(r.rental_date)’);PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET SESSION group_concat_max_len = 1000000;

-- Rolling windows
SELECT MONTHNAME(r.rental_date), c.name, count(r.rental_id),     
SUM(count(r.rental_id)) over(PARTITION BY MONTHNAME(r.rental_date)) as rental_month_total, count(rental_id) / SUM(count(r.rental_id)) over(PARTITION BY MONTHNAME(r.rental_date)) * 100 as percentage_of_rentals 
FROM rental r 
LEFT JOIN film f ON f.film_id = r.inventory_id 
LEFT JOIN film_category fc ON fc.film_id = f.film_id 
LEFT JOIN category c ON c.category_id = fc.category_id 
GROUP BY MONTHNAME(r.rental_date),c.name;

-- Generating Data
SELECT DATE_FORMAT(r.rental_date,"%Y-%M") as rental_date, count(r.rental_id) as rental_count 
FROM rental r  
LEFT JOIN film f ON f.film_id = r.inventory_id  
LEFT JOIN film_category fc ON fc.film_id = f.film_id  
LEFT JOIN category c ON c.category_id = fc.category_id  
GROUP BY DATE_FORMAT(r.rental_date,"%Y-%M");

WITH RECURSIVE t(v) as (   
  SELECT  DATE('2005-03-01')   
  UNION ALL   
  SELECT v + INTERVAL 1 MONTH   
  FROM t    
  LIMIT 12 
) 
SELECT DATE_FORMAT(t.v,"%Y-%M") as rental_date, count(r.rental_id) as rental_count FROM rental r  
LEFT JOIN film f ON f.film_id = r.inventory_id   
LEFT JOIN film_category fc ON fc.film_id = f.film_id   
LEFT JOIN category c ON c.category_id = fc.category_id   
RIGHT JOIN t on DATE_FORMAT(t.v,"%Y-%M")  = DATE_FORMAT(r.rental_date,"%Y-%M") 
GROUP BY DATE_FORMAT(t.v,"%Y-%M");
