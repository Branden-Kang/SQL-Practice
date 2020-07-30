# Reference: https://towardsdatascience.com/using-sql-to-detect-outliers-aff676bb2c1a
CREATE TABLE KidWts (Name nvarchar(20),Age int ,Weight float);
INSERT INTO KidWts VALUES
(‘Lily’,3,15), 
(‘Muhammad’,30,98), 
(‘Daphne’, 3, 16), 
(‘Max’, 2, 12),
(‘Chloe’,1,11),
(‘Jackie’,2,14),
(‘Albert’,3,17);

WITH MedianTab (MedianWt)
AS
(SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Weight)
OVER () as MedianWeight
FROM KidWts),

DispersionTab (AbsDispersion)
AS
(SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (Abs(Weight-MedianWt)))
OVER () as AbsDispersion
FROM MedianTab JOIN KidWts on 1=1)

Select DISTINCT *,ABS((Weight-MedianWt)/AbsDispersion) 
FROM KidWts Join DispersionTab on 1=1
JOIN MedianTab on 1=1
