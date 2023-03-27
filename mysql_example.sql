-- 1. Connect to the server
-- sudo mysql -u root

-- 2. Create a Database
create database retail;
use retail;

-- 3. Create tables
create table customer (
  cust_id int primary key,
  age int,
  location varchar (20),
  gender varchar (20));
  
-- 4. Create another table
create table orders (
    order_id int primary key,
    date date,
    amount decimal(5,2),
    cust_id int,
    foreign key (cust_id) references customer(cust_id)
    on delete cascade);
    
-- 5. View Tables
show tables;

-- 6. Table Description — info( )
desc orders;

-- 7. Modify Tables
alter table orders add is_sale varchar(20);

-- 8. Delete Feature
alter table orders drop is_sale;

-- 9. Enter data
insert into customer values (
1000, 42, 'Austin', 'female);

-- 10. Insert Multiple Lines
insert into customer values 
    (1001, 34, 'Austin', 'male'),
    (1002, 37, 'Houston', 'male'),
    (1003, 25, 'Austin', 'female'),
    (1004, 28, 'Houston', 'female'),
    (1005, 22, 'Dallas', 'male');
    
-- 11. Deleting Lines
delete from orders
where order_id = 17;

-- 12. Update a Line
update orders
    set amount = 27.40  # alter this column
    where order_id = 1;
select * from orders limit 1;

-- 13. Replicate a Table Structure
create table orders_copy like orders;
show tables;

-- 14. Replicate a whole Table
create table new_orders
select * from orders;

-- 15. Drop Tables
drop table orders_copy, new_orders;
show tables;

-- 16. View Table Features
select * from orders
limit 3;

-- 17. Select Specific Columns
select order_id, amount 
from orders
limit 3;

-- 18. Condition Where
select * from orders
where date = '2020-10-01';

-- 19. Multiple Where Conditions
select * from orders
    where date = '2020-10-01' and amount > 50;

-- 20. Sort
select * from orders
    where date = '2020-10-02'
    order by amount;
    
-- 21. Ascending Sort
select * from orders
    where date = '2020-10-02'
    order by amount desc;
    
-- 22. Count (fundamental)
select count(distinct(date)) as day_count
from orders;

-- 23. Group By
select date, count(order_id) as order_count
    from orders
    group by date;
    
-- 24. Average daily value
select date, count(order_id) as order_count
    from orders
    group by date;
    
-- 25. Group and Filter
select date, avg(amount)
    from orders
    group by date
    having avg(amount) > 30
    order by avg(amount) desc;
    
-- 26. Maximum Daily Value
select date, max(amount)
    from orders
    group by date;
    
-- 27. Combine Various Functions
select cust_id, max(amount) - min(amount) as dif
    from orders
    group by cust_id
    order by dif desc
    limit 3;

-- 28. Grouping Count
select location, gender, count(cust_id)
    from customer
    group by location, gender;

-- 29. Relationship
select customer.location, avg(orders.amount) as avg
    from customer
    join orders
    on customer.cust_id = orders.cust_id
    group by customer.location;
    
-- 30. Aggregation and Filter
select avg(c.age) as avg_age
    from customer c
    join orders o
    on c.cust_id = o.cust_id
    where o.date = '2020-10-03';
    
-- 31. Nested Condition
select c.location, o.amount
   from customer c
   join orders o
   on c.cust_id = o.cust_id
   where o.amount = (select max(amount) from orders);
