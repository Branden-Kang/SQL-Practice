--select *
--from coinname;
--
--select *
--from coinprice;
--
--select *
--from coinname, coinprice
--where coinname.coin = coinprice.coin;
--Q1
select coinname.coin, coinname.name, rank() over (order by max(coinprice.marketcap) desc) as RANKING
from coinname, coinprice
where coinname.coin = coinprice.coin
group by coinname.coin, coinname.name;
--Q1_different version
select coin, name, rank() over (order by cap desc) as ranking from coinname natural join (
select coin, max(marketcap) as cap from coinprice
group by coin);
--Q2
select year,decode(month,'Jan',1,'Feb',2,'Mar',3,'Apr',4,'May',5,'Jun',6,'Jul',7,'Aug',8,'Sep',9,'Oct',10,'Nov',11,'Dec',12) as NUMMONTH,day, 
        to_char(avg(close) over (partition by year, month order by year, month, day asc rows 30 preceding), '9999.9999') as avg30
from coinname, coinprice
where coinname.coin = coinprice.coin and name = 'Iota' and year = 2017 and month = 'Jun';
--Q2_differnet version
select year, nummonth, day,
to_char(avg(close) over (order by year, nummonth, day rows 30 preceding),'9.999') as avg30
from (
select year, decode(month,'Jan',1,'Feb',2,'Mar',3,'Apr',4,'May',5,'Jun',6,'Jul',7,'Aug',8,'Sep',9,'Oct',10,'Nov',11,'Dec',12) as nummonth,
day, close from coinprice natural join coinname
where name = 'Iota' );
--Q3
select *
from (select  coin, year, month as MON, day, volume, 
        rank() over (partition by coin order by volume desc) as RANK
from (select coin, year, month, day, volume from coinprice where coin IS NOT NULL and volume is not null)
order by coin, RANK asc)
where rank <= 5;
--Q3_different version
select * from (
select coin, year, month, day, to_char(volume,'999999999999') as volume, rank() over (partition by coin order by volume desc) as rank
from coinprice
where volume is not null) where rank <= 5;
--Q4
select coinname.coin, year, month, to_char(avg(close), 99999.99) as close
from coinname, coinprice
where coinname.coin = coinprice.coin
group by rollup(coinname.coin, year, month);
--Q4_different version
select coin, year, month, to_char(avg(close),'99999.99') as close 
from coinprice 
group by rollup (coin, year, month);
--select year, month, day, sum(total) as total
--from (select year, month, day, amount ⇤ price as total
--from sale)
--group by rollup(year, month, day);
--Q5
-- It is better to use rollup instead of acube because Rollup generates n+1 output but cube generates 2^n output and cube relations are often large.
-- When I use a cube in question4, the combination of output is so huge and it is not easy to interpret.
-- Here is my "cube" code below:
--select coinname.coin, year, month, to_char(avg(close), 99999.99) as close
--from coinname, coinprice
--where coinname.coin = coinprice.coin
--group by rollup(coinname.coin), rollup(year), rollup(month);
--Question 6
--Answer: group by rollup(a), rollup(b), rollup(c), rollup(d)
--For example, groupby by rollup(a), rollup(b) is {emptyset,a} * {emptyset,b} = {emptyset, {a}, {b}, {a,b}} = cube {a,b}
--So, the cartesian product of a, b, c, d, emptyset gives all the groupings created by the cube function.