-- https://medium.com/swlh/linear-regression-in-sql-is-it-possible-b9cc787d622f
SELECT x, AVG(x) OVER() as x_bar,
       y, AVG(y) OVER() as y_bar
FROM sample

SElECT slope, y_bar_max - x_bar_max * slope as intercept
FROM 
(
SELECT SUM((x-x_bar)*(y-y_bar))/SUM((x-x_bar)*(x-x_bar)) as slope,
       MAX(x_bar) as x_bar_max,
       MAX(y_bar) as y_bar_max
FROM
( 
SELECT x, AVG(x) OVER() as x_bar,
       y, AVG(y) OVER() as y_bar
FROM sample
)
)

with trend_line AS
(
SELECT slope, y_bar_max - x_bar_max * slope as intercept
FROM 
(
SELECT SUM((x-x_bar)*(y-y_bar)) / SUM((x-x_bar)*(x-x_bar)) as slope,
       MAX(x_bar) as x_bar_max,
       MAX(y_bar) as y_bar_max
FROM
(
SELECT x, AVG(x) OVER() as x_bar,
       y, AVG(y) OVER() as y_bar
FROM sample
) data1
) data2    
)

SELECT sample.*, (sample.x * (SELECT slope FROM trend_line) + (SELECT intercept FROM trend_line) AS trend_line
FROM sample
