--Thanks to https://medium.com/towards-artificial-intelligence/5-tricky-sql-queries-solved-919266e2d524

--Query 1
SELECT CONCAT(Name, '(', SUBSTR(Profession,1,1),')')
FROM table;

--Query2
SELECT AVG(Salary) - AVG(Replace(Salary, 0, ''))
FROM table;

--Query3: binary search tree
--There are three types: Root, Leaf, and Inner
SELECT CASE WHEN P IS NULL THEN CONCAT(N, 'Root')
            WHEN N IN (SELECT DISTINCT P from BST) THEN CONCAT(N, 'Inner')
            ELSE CONCAT(N, 'Leaf')
            END
FROM BST
ORDER BY N asc;

--Query4
SELECT COUNT(user_id)
FROM
(SELECT user_id
 FROM orders
 GROUP BY user_id
 HAVING COUNT(DISTINCT DATE(date)) > 1) t1
 
 --QUERY5
SELECT
    s1.user_id, (CASE WHEN s2.user_id IS NOT NULL THEN 1 ELSE 0 END) AS overlap
FROM subscriptions AS s1
LEFT JOIN subscriptions AS s2
    ON s1.user_id != s2.user_id
        AND s1.start_date <= s2.end_date
        AND s1.end_date >= s2.start_date
GROUP BY s1.user_id
