SELECT *
FROM tblSouthPark

-- 1) IS NULL and IS NOT NULL Operators
SELECT 
    ID, 
    Student,
    Email1,
    Email2
FROM tblSouthPark
WHERE Email1 IS NULL AND Email2 IS NULL
ORDER BY ID

SELECT 
    ID, 
    Student,
    Email1,
    Email2
FROM tblSouthPark
WHERE Email1 IS NOT NULL AND Email2 IS NOT NULL
ORDER BY ID

-- 2) ISNULL() Function
SELECT 
    ID, 
    Student,
    ISNULL(Father,‘Missing’) AS Father
FROM tblSouthPark
ORDER BY ID

SELECT 
    ID, 
    Student,
    ISNULL(Father,‘Missing’) AS Father
FROM tblSouthPark
ORDER BY ID

-- 3) COALESCE() Function
SELECT 
    ID, 
    Student,
    COALESCE(Email1, Email2, 'N/A') AS Primary_Email
FROM tblSouthPark
ORDER BY ID

-- 4) CASE Expression
SELECT 
      ID,
      Student,
      CASE
          WHEN Email1 IS NOT NULL THEN Email1
          WHEN Email2 IS NOT NULL THEN Email2
          ELSE 'N/A'
      END AS Primary_Email
FROM tblSouthPark
ORDER BY ID

-- 5) NULLIF() Function
-- NULLIF('Red','Orange') -- Returns Red
-- NULLIF(0,NULL) -- Returns 0
-- NULLIF(0,0) -- Returns NULL

SELECT 
    ID, 
    Student,
    NULLIF(Phone,'') AS Phone
FROM tblSouthPark
ORDER BY ID

var1 = 1
var2 = 0
var1/var2 --This will generate a "division by zero" error

var1/NULLIF(var2,0)--This doesn't trigger a "division by zero" error

SELECT COALESCE(NULLIF(Work,''),Cell) AS Primary FROM Sample
